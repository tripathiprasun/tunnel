"""
Tunnel - signaling server.

This server's ONLY job is to help two browsers find each other and exchange
the SDP offer/answer and ICE candidates needed to establish a direct WebRTC
connection. It never sees chat messages or file contents once that
connection is up - those travel peer-to-peer over the RTCDataChannel.

State is kept entirely in memory (a plain dict). There is no database and
nothing is written to disk. Restarting this process drops all rooms, which
is expected and fine - rooms are meant to be short-lived.

Protocol (JSON messages over a single WebSocket connection per client):

  Client -> Server
    {"type": "create"}
    {"type": "join", "room": "583921"}
    {"type": "offer", "data": <RTCSessionDescriptionInit>}
    {"type": "answer", "data": <RTCSessionDescriptionInit>}
    {"type": "ice-candidate", "data": <RTCIceCandidateInit>}
    {"type": "leave"}

  Server -> Client
    {"type": "created", "room": "583921"}
    {"type": "joined", "room": "583921"}
    {"type": "peer-joined"}
    {"type": "peer-left"}
    {"type": "room-expired"}
    {"type": "offer", "data": ...}          (relayed)
    {"type": "answer", "data": ...}         (relayed)
    {"type": "ice-candidate", "data": ...}  (relayed)
    {"type": "error", "message": "..."}
"""

import asyncio
import json
import logging
import os
import secrets
import time

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("tunnel-signaling")

# --------------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------------

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", "8765"))  # Render injects PORT; 8765 for local dev

# Set this to your frontend's origin (scheme + host only, no path).
# Defaults to the GitHub Pages origin this project is served from; override
# with the FRONTEND_ORIGIN env var if you ever move or add a custom domain.
# Comma-separate multiple origins if needed, e.g. "https://a.com,https://b.com".
#
# IMPORTANT: this must match EXACTLY (scheme + host + port) the origin the
# browser sends when it opens the WebSocket - if it doesn't match, the
# library rejects the connection before your code ever runs, which looks
# from the browser like the server is simply unreachable. If you are ever
# unsure what origin your frontend is actually running from (custom domain,
# a preview deployment, testing from a different host, etc.), it's safer to
# temporarily set ALLOW_ANY_ORIGIN=1 to confirm that's not what's blocking
# you, then lock it back down to the real origin(s) once confirmed.
DEFAULT_FRONTEND_ORIGIN = "https://tripathiprasun.github.io"

ALLOW_ANY_ORIGIN = os.environ.get("ALLOW_ANY_ORIGIN", "").strip() == "1"

_frontend_origin_env = os.environ.get("FRONTEND_ORIGIN", "").strip()
if ALLOW_ANY_ORIGIN:
    ALLOWED_ORIGINS = None  # disables the origin check entirely
    logger.warning(
        "ALLOW_ANY_ORIGIN=1 - origin checking is DISABLED. "
        "Use this only to diagnose connectivity issues, not in production."
    )
elif _frontend_origin_env:
    ALLOWED_ORIGINS = [o.strip() for o in _frontend_origin_env.split(",") if o.strip()]
    logger.info("Restricting WebSocket connections to origins: %s", ALLOWED_ORIGINS)
else:
    ALLOWED_ORIGINS = [DEFAULT_FRONTEND_ORIGIN]
    logger.info(
        "FRONTEND_ORIGIN not set - defaulting to %s. "
        "Set FRONTEND_ORIGIN to override.",
        DEFAULT_FRONTEND_ORIGIN,
    )

ROOM_PIN_LENGTH = 6
ROOM_MAX_PEERS = 2
ROOM_EMPTY_TIMEOUT_SECONDS = 10 * 60   # room created but never joined -> expire
ROOM_HARD_TIMEOUT_SECONDS = 2 * 60 * 60  # absolute max lifetime of any room
CLEANUP_INTERVAL_SECONDS = 30

# --------------------------------------------------------------------------
# In-memory room state (no persistence, by design)
# --------------------------------------------------------------------------
# pin -> {"peers": [websocket, ...], "initiator": websocket, "created_at": float}
rooms = {}


def generate_pin():
    for _ in range(100):
        pin = "".join(secrets.choice("0123456789") for _ in range(ROOM_PIN_LENGTH))
        if pin not in rooms:
            return pin
    raise RuntimeError("Could not allocate a free PIN - too many active rooms.")


async def safe_send(ws, payload):
    try:
        await ws.send(json.dumps(payload))
    except Exception:
        # Peer likely disconnected already; the disconnect handler will clean up.
        pass


def other_peer(room, ws):
    for peer in room["peers"]:
        if peer is not ws:
            return peer
    return None


# --------------------------------------------------------------------------
# Background cleanup of abandoned / stale rooms
# --------------------------------------------------------------------------

async def cleanup_loop():
    while True:
        await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)
        now = time.time()
        expired_pins = []
        for pin, room in rooms.items():
            age = now - room["created_at"]
            if len(room["peers"]) < ROOM_MAX_PEERS and age > ROOM_EMPTY_TIMEOUT_SECONDS:
                expired_pins.append(pin)
            elif age > ROOM_HARD_TIMEOUT_SECONDS:
                expired_pins.append(pin)

        for pin in expired_pins:
            room = rooms.pop(pin, None)
            if not room:
                continue
            logger.info("Expiring stale room %s", pin)
            for peer in list(room["peers"]):
                await safe_send(peer, {"type": "room-expired"})
                try:
                    await peer.close()
                except Exception:
                    pass


# --------------------------------------------------------------------------
# Connection handler
# --------------------------------------------------------------------------

async def handler(websocket):
    joined_pin = None

    try:
        async for raw_message in websocket:
            try:
                msg = json.loads(raw_message)
            except (json.JSONDecodeError, TypeError, UnicodeDecodeError):
                await safe_send(websocket, {"type": "error", "message": "Invalid message: not valid JSON."})
                continue

            if not isinstance(msg, dict) or "type" not in msg:
                await safe_send(websocket, {"type": "error", "message": "Invalid message: missing 'type'."})
                continue

            msg_type = msg.get("type")

            # ---------------- create ----------------
            if msg_type == "create":
                if joined_pin:
                    await safe_send(websocket, {"type": "error", "message": "Already in a tunnel."})
                    continue
                try:
                    pin = generate_pin()
                except RuntimeError as err:
                    await safe_send(websocket, {"type": "error", "message": str(err)})
                    continue
                rooms[pin] = {"peers": [websocket], "initiator": websocket, "created_at": time.time()}
                joined_pin = pin
                await safe_send(websocket, {"type": "created", "room": pin})
                logger.info("Room %s created", pin)

            # ---------------- join ----------------
            elif msg_type == "join":
                pin = str(msg.get("room", "")).strip()
                if not pin.isdigit() or len(pin) != ROOM_PIN_LENGTH:
                    await safe_send(websocket, {"type": "error", "message": "Invalid PIN format."})
                    continue

                room = rooms.get(pin)
                if not room:
                    await safe_send(websocket, {"type": "error", "message": "This tunnel does not exist or has expired."})
                    continue
                if len(room["peers"]) >= ROOM_MAX_PEERS:
                    await safe_send(websocket, {"type": "error", "message": "This tunnel is already occupied."})
                    continue

                room["peers"].append(websocket)
                joined_pin = pin
                await safe_send(websocket, {"type": "joined", "room": pin})
                await safe_send(room["initiator"], {"type": "peer-joined"})
                logger.info("Peer joined room %s", pin)

            # ---------------- SDP / ICE relay ----------------
            elif msg_type in ("offer", "answer", "ice-candidate"):
                if not joined_pin or joined_pin not in rooms:
                    await safe_send(websocket, {"type": "error", "message": "You are not in a tunnel."})
                    continue
                room = rooms[joined_pin]
                peer = other_peer(room, websocket)
                if not peer:
                    await safe_send(websocket, {"type": "error", "message": "No peer connected yet."})
                    continue
                await safe_send(peer, {"type": msg_type, "data": msg.get("data")})

            # ---------------- leave ----------------
            elif msg_type == "leave":
                break

            else:
                await safe_send(websocket, {"type": "error", "message": f"Unknown message type: {msg_type}"})

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception:
        logger.exception("Unexpected error in connection handler")
    finally:
        if joined_pin and joined_pin in rooms:
            room = rooms[joined_pin]
            if websocket in room["peers"]:
                room["peers"].remove(websocket)
            remaining = list(room["peers"])
            # A tunnel is only ever meant to hold two peers; if either one
            # leaves, the tunnel is considered over rather than left open
            # for a third party to join.
            rooms.pop(joined_pin, None)
            for peer in remaining:
                await safe_send(peer, {"type": "peer-left"})
            logger.info("Room %s closed", joined_pin)


# --------------------------------------------------------------------------
# Plain HTTP responses for platform health checks (not part of the WS protocol)
# --------------------------------------------------------------------------
#
# NOTE ON WHY THIS IS THE MOST LIKELY REASON NOTHING WAS CONNECTING AT ALL:
#
# The `websockets` library changed the signature of `process_request` in a
# backwards-incompatible way starting with version 13 (the new default
# asyncio server implementation). The OLD signature was:
#
#     def process_request(path, request_headers): ...
#
# The NEW signature is:
#
#     async def process_request(connection, request): ...
#
# Critically, BOTH signatures take exactly two positional arguments, so if
# you `pip install websockets` today (which installs the latest version)
# while this function is still written for the old signature, Python does
# NOT raise a "wrong number of arguments" error - it happily calls the
# function, but `request_headers` (the second argument) is now actually a
# `Request` object, not a headers mapping. That object has no `.get()`
# method, so the very first line (`request_headers.get("Upgrade", "")`)
# raises an AttributeError - for every single request, including the
# WebSocket handshake itself. The handshake then fails silently from the
# browser's point of view: it just looks like the server refused the
# connection, on every network, every time. This is almost certainly what
# you were hitting.
#
# The function below is written to work correctly with EITHER API, by
# inspecting what it was actually handed, so it keeps working regardless of
# which `websockets` version ends up installed.

async def process_request(arg1, arg2):
    # New API (websockets >= 13): process_request(connection, request)
    # `arg2` is a Request object exposing `.headers` (a Headers mapping).
    if hasattr(arg2, "headers"):
        connection = arg1
        headers = arg2.headers
        is_new_api = True
    # Old API (websockets < 13): process_request(path, request_headers)
    # `arg2` IS the headers mapping directly.
    else:
        headers = arg2
        is_new_api = False

    if headers.get("Upgrade", "").lower() == "websocket":
        return None  # let the normal WebSocket handshake proceed

    body_text = "Tunnel signaling server is running.\n"
    if is_new_api:
        return connection.respond(200, body_text)
    else:
        body = body_text.encode()
        return (200, [("Content-Type", "text/plain"), ("Content-Length", str(len(body)))], body)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

async def main():
    logger.info("Starting Tunnel signaling server on %s:%s", HOST, PORT)
    cleanup_task = asyncio.create_task(cleanup_loop())
    try:
        async with websockets.serve(
            handler,
            HOST,
            PORT,
            process_request=process_request,
            origins=ALLOWED_ORIGINS,
            max_size=2 * 1024 * 1024,   # signaling messages are tiny; this is a generous cap
            ping_interval=20,
            ping_timeout=20,
        ):
            await asyncio.Future()  # run forever
    finally:
        cleanup_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())