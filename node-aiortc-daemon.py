"""
WebRTC Daemon Process

Communicates with a parent process using stdin/stdout and JSON marshalling.
"""


import asyncio
import json
import os
import sys
import threading

from aiortc import RTCPeerConnection, RTCSessionDescription
from collections import namedtuple

# Special thanks to DS. on stackoverflow
# https://stackoverflow.com/questions/6578986/how-to-convert-json-data-into-a-python-object


def _json_object_hook(d): return namedtuple('X', d.keys())(*d.values())


def json2obj(data): return json.loads(data, object_hook=_json_object_hook)


# From the example TextSignaling package in aiortc
def description_from_string(descr_str):
    descr_dict = json.loads(descr_str)
    return RTCSessionDescription(
        sdp=descr_dict['sdp'],
        type=descr_dict['type']
    )


def description_to_string(descr):
    return json.dumps({
        'sdp': descr.sdp,
        'type': descr.type
    })


class RPCDispatcher(object):
    """
    Wraps aiortc's functionality, returning values to the caller when necessary.

    We assume that the caller will handle all signaling.
    """

    def __init__(self, loop, write):
        self.peers = {}
        self.loop = loop
        self.output = write

    async def _raiseEvent(self, eventName, eventData):
        """
        Use the RPC channel to raise an event in the caller.
        """
        event = {
            "type": "event",
            "value": {
                "name": eventName,
                "data": eventData,
            }
        }
        await self.loop.run_in_executor(None, self.output.write, json.dumps(event) + '\n')

    async def create_peer_connection(self, peerid):
        if peerid in self.peers:
            raise Exception("Peer already exists")
        else:
            self.peers[peerid] = {
                'pc': RTCPeerConnection(),
                'event': asyncio.Event()
            }
            return True

    async def offer(self, peerid):
        """
        Generate an offer to open a channel.
        """
        pc = self.peers[peerid]['pc']
        event = self.peers[peerid]['event']
        channel = pc.createDataChannel('chan')

        self.peers[peerid]['channel'] = channel

        @channel.on('open')
        async def on_open():
            event.set()
            await self._raiseEvent("channel_open", {"peer": peerid})

        @channel.on('message')
        async def on_message(message):
            await self._raiseEvent("channel_message", {
                "peer": peerid, "message": message})

        await pc.setLocalDescription(await pc.createOffer())
        return description_to_string(pc.localDescription)

    async def complete_offer(self, answer, peerid):
        """
        Complete an offer begun with get_local_offer_sdp once an answer has
        been received, and open the connection.
        """
        answer = json2obj(answer)

        pc = self.peers[peerid]['pc']
        event = self.peers[peerid]['event']

        await pc.setRemoteDescription(answer)
        await event.wait()
        return True

    async def answer(self, offer, peerid):
        """
        Answer an SDP-encoded offer
        """
        offer = json2obj(offer)

        pc = self.peers[peerid]['pc']
        event = self.peers[peerid]['event']

        @pc.on('datachannel')
        async def on_channel(channel):

            @channel.on('message')
            async def on_message(message):
                await self._raiseEvent("channel_message", {"peer": peerid, "message": message})

            self.peers[peerid]['channel'] = channel
            await self._raiseEvent("channel_open", {"peer": peerid})

        await pc.setRemoteDescription(offer)

        await pc.setLocalDescription(await pc.createAnswer())
        return description_to_string(pc.localDescription)

    async def send(self, message, peerid):
        self.peers[peerid]['event'].wait()
        chan = self.peers[peerid]['channel']

        chan.send(message)


# This class simply requires exclusive access to write to the underlying
# output.
class LockedOutput(object):
    """
    Lock calls to write on an object, ensuring that writes do not, ever, overlap
    """

    def __init__(self, output):
        self.output = output
        self.lock = threading.Lock()

    def write(self, msgbuf):
        with self.lock:
            self.output.write(msgbuf)


async def respond(dispatcher, out, rpcLine):
    """
    Handle responding to an RPC invocation, which has the following structure:
    {
        id : number (a unique identifier for this call)
        rpcEndpoint : string (the method to invoke within the dispatcher)
        args : array (passed as *args)
        kwArgs : dict (passed as **kwArgs)
    }
    """
    data = json.loads(rpcLine)
    try:
        args = data["args"]
    except KeyError:
        args = []
    try:
        kwArgs = data["kwArgs"]
    except KeyError:
        kwArgs = {}

    try:
        resp = {
            "type": "response",
            "value": {
                "id": data["id"],
                "rpcEndpoint": data["rpcEndpoint"],
                "value": await (getattr(dispatcher, data["rpcEndpoint"])(*args, **kwArgs)),
            }
        }
    except Exception as ex:
        resp = {
            "type": "throw",
            "value": {
                "id": data["id"],
                "rpcEndpoint": data["rpcEndpoint"],
                "value": str(ex)
            }
        }
    await loop.run_in_executor(None, out.write, json.dumps(resp) + '\n')


async def main(loop):
    """
    Create an RPC Dispatcher and listen for input on stdin asynchronously
    """
    read = sys.stdin
    write = LockedOutput(sys.stdout)
    dispatcher = RPCDispatcher(loop, write)
    while True:
        line = await loop.run_in_executor(None, read.readline)
        asyncio.ensure_future(respond(dispatcher, write, line))


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    # This call diverges from the main body, since there's a `while True` in it
    loop.run_until_complete(main(loop))
