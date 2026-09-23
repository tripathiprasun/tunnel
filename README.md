# Tunnel

A minimal, peer-to-peer chat and file-transfer tool. Two browsers connect
directly over WebRTC using a 6-digit PIN — no accounts, no server storage,
no database. A small signaling server only helps the two browsers find each
other; once connected, everything (messages and files) travels directly
between them over an encrypted `RTCDataChannel`.

## How it works

1. One person clicks **Create Tunnel** and gets a 6-digit PIN.
2. They share that PIN with the other person (any channel — text, call, etc).
3. The other person enters the PIN and clicks **Join**.
4. The signaling server relays the WebRTC handshake (SDP offer/answer + ICE
   candidates) between the two browsers.
5. Once the handshake completes, a direct `RTCDataChannel` connection opens
   and the signaling server is no longer involved in the conversation.
6. Either side can send chat messages or files. Files are chunked and
   streamed directly peer-to-peer.
7. If either side leaves, the tunnel closes. Rooms are single-use and
   expire automatically if abandoned.

## Project structure

```
.
├── signaling_server.py   # Python signaling server (WebSocket relay only)
├── index.html             # Frontend page
├── frontend.js             # Frontend logic (WebRTC, chat, file transfer)
└── style.css                # Frontend styling
```

## Signaling server

The server (`signaling_server.py`) does exactly one job: pair up two
browsers by PIN and relay their WebRTC handshake messages. It:

- Keeps all state in memory (a plain dict) — nothing is persisted to disk.
- Never sees chat messages or file contents; those only flow once the
  direct peer-to-peer connection is up.
- Automatically expires unused or abandoned rooms.

### Running locally

```bash
pip install websockets
python signaling_server.py
```

By default it listens on `0.0.0.0:8765` (configurable via the `PORT`
environment variable).

### Deploying

The server is deployed on [Render](https://render.com) and is publicly
reachable at:

```
wss://server-1-n1sw.onrender.com
```

Allowed WebSocket origins are restricted via `FRONTEND_ORIGIN` (defaults to
`https://tripathiprasun.github.io` in the code, but can be overridden with
an environment variable on Render if needed).

## Frontend

The frontend is a static site (plain HTML/CSS/JS, no build step) intended
for GitHub Pages, currently published at:

```
https://tripathiprasun.github.io/tunnel
```

`frontend.js` hardcodes the signaling server URL (`SIGNALING_SERVER_URL`)
and the STUN server list (`ICE_SERVERS`). To point this frontend at a
different signaling server, edit that constant directly.

### Deploying to GitHub Pages

1. Push `index.html`, `frontend.js`, and `style.css` to the repo.
2. In the repo settings, enable GitHub Pages for the branch/folder these
   files live in.
3. Visit `https://tripathiprasun.github.io/tunnel`.

## Notes and limitations

- **No TURN server.** Connectivity relies on STUN (Google's public STUN
  server by default) for NAT traversal. On restrictive networks or
  symmetric NATs, the WebRTC connection may fail to establish. Add a TURN
  server to `ICE_SERVERS` in `frontend.js` if you need more reliable
  connectivity.
- **No persistence, no history.** Once a tunnel closes, nothing about that
  session is recoverable — by design.
- **Two peers per room, max.** A third person cannot join an active tunnel.
- **No reconnection support.** If the signaling connection drops before
  the WebRTC handshake completes, the session must be restarted.
