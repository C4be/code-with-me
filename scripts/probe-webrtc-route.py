#!/usr/bin/env python3
"""Connect as a temporary WebRTC guest and print the selected ICE route.

Requires aiortc and websockets. Pass the invite URL on stdin; it is never logged.
"""

import asyncio
import json
import os
import sys
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

import websockets
from aiortc import RTCConfiguration, RTCIceServer, RTCPeerConnection, RTCSessionDescription
from aiortc.sdp import candidate_from_sdp


async def main():
    invite = urlparse(sys.stdin.readline().strip())
    params = parse_qs(invite.query)
    room = params.get("room", [""])[0]
    code = params.get("code", [""])[0]
    if invite.scheme != "https" or not room or not code:
        raise ValueError("Expected an HTTPS invitation with room and code")
    signal_host = "127.0.0.1:8787" if os.environ.get("CWM_LOCAL_SIGNAL") == "1" else invite.netloc
    signal_scheme = "ws" if os.environ.get("CWM_LOCAL_SIGNAL") == "1" else "wss"
    signal = urlunparse((signal_scheme, signal_host, f"/signal/{room}", "", urlencode({"code": code}), ""))

    async with websockets.connect(signal, open_timeout=12) as socket:
        hello = json.loads(await asyncio.wait_for(socket.recv(), 12))
        if hello.get("type") != "hello":
            raise RuntimeError("Signaling server did not send ICE configuration")
        servers = [RTCIceServer(**item) for item in hello.get("iceServers", [])]
        print(f"ICE servers: {len(servers)}", flush=True)
        pc = RTCPeerConnection(RTCConfiguration(iceServers=servers))
        ready = asyncio.Event()
        ice_ready = asyncio.Event()
        pending = []

        @pc.on("datachannel")
        def on_datachannel(channel):
            print("Data channel received", flush=True)
            @channel.on("open")
            def on_open():
                print("Data channel opened", flush=True)
                ready.set()

        @pc.on("iceconnectionstatechange")
        def on_ice_change():
            print(f"ICE state: {pc.iceConnectionState}", flush=True)
            if pc.iceConnectionState in ("connected", "completed"):
                ice_ready.set()

        async def add_candidate(message):
            raw = message.get("candidate") or {}
            line = raw.get("candidate", "")
            if not line:
                return
            candidate = candidate_from_sdp(line.removeprefix("candidate:"))
            candidate.sdpMid = raw.get("sdpMid")
            candidate.sdpMLineIndex = raw.get("sdpMLineIndex")
            await pc.addIceCandidate(candidate)

        async def receive_signals():
            async for payload in socket:
                message = json.loads(payload)
                if message.get("type") == "ice":
                    if pc.remoteDescription:
                        await add_candidate(message)
                    else:
                        pending.append(message)
                elif message.get("type") == "offer":
                    print("Offer received", flush=True)
                    description = message["description"]
                    await pc.setRemoteDescription(RTCSessionDescription(sdp=description["sdp"], type=description["type"]))
                    for item in pending:
                        await add_candidate(item)
                    pending.clear()
                    await pc.setLocalDescription(await pc.createAnswer())
                    await socket.send(json.dumps({
                        "type": "answer",
                        "to": message["from"],
                        "description": {"type": pc.localDescription.type, "sdp": pc.localDescription.sdp},
                    }))
                    print("Answer sent", flush=True)

        signal_task = asyncio.create_task(receive_signals())
        try:
            await asyncio.wait_for(ice_ready.wait(), 30)
            try:
                await asyncio.wait_for(ready.wait(), 3)
            except asyncio.TimeoutError:
                pass
            selected = pc.sctp.transport.transport._connection._nominated
            pair = next(iter(selected.values()), None)
            if pair is None:
                raise RuntimeError("Data channel opened, but no ICE candidate pair is available")
            local = pair.protocol.local_candidate
            remote = pair.remote_candidate
            route = "relay" if "relay" in (local.type, remote.type) else "direct"
            print(json.dumps({"connection": pc.connectionState, "dataChannelOpen": ready.is_set(), "route": route, "localType": local.type, "remoteType": remote.type}))
        finally:
            signal_task.cancel()
            await pc.close()


if __name__ == "__main__":
    asyncio.run(main())
