"use strict";

/* =========================================================================
   CONFIGURATION
   ========================================================================= */

const SIGNALING_SERVER_URL = "wss://server-1-n1sw.onrender.com";

// STUN helps peers discover their public address, but STUN alone only
// works when at least one side is on a permissive NAT/firewall. On real
// mobile networks, corporate networks, and many home routers (symmetric
// NAT / carrier-grade NAT), a direct connection cannot be established at
// all without a TURN server relaying the traffic. This is the classic
// cause of "works between two tabs on my machine or same Wi-Fi, but fails
// between two different networks" - which is expected with STUN-only
// config. There is no way around this without a TURN server; add one below
// to make Tunnel work reliably across arbitrary networks.
//
// To get free TURN credentials:
//   1. Go to metered.ca/stun-turn (or similar - "Xirsys" and "Twilio NTS"
//      also have free tiers) and create a free account.
//   2. Generate a credential; it gives you a ready-made iceServers array
//      with your own username/credential pair.
//   3. Paste that array's TURN entries below, alongside the STUN entry.
// Free tiers are usage-capped (e.g. 20 GB/month) but are plenty for
// personal use. Publicly-shared demo credentials (like the old
// "openrelayproject/openrelayproject" pair some tutorials use) are not
// reliable anymore, providers have locked those down, so use your own.
const ICE_SERVERS = [
  { urls: "stun:stun.l.google.com:19302" },
  // { urls: "turn:YOUR_TURN_HOST:80", username: "YOUR_USERNAME", credential: "YOUR_CREDENTIAL" },
  // { urls: "turn:YOUR_TURN_HOST:443?transport=tcp", username: "YOUR_USERNAME", credential: "YOUR_CREDENTIAL" },
];

const CHUNK_SIZE = 16 * 1024;             // 16 KiB per data-channel message
const SEND_BUFFER_HIGH_WATER = 1 * 1024 * 1024; // pause sending above 1 MB buffered
const SEND_BUFFER_LOW_WATER = 256 * 1024;       // resume once buffer drains below this

/* =========================================================================
   DOM REFERENCES
   ========================================================================= */

const $ = (id) => document.getElementById(id);

const el = {
  statusIndicator: $("status-indicator"),
  statusText: document.querySelector("#status-indicator .status-text"),
  unsupportedBanner: $("unsupported-banner"),

  viewHome: $("view-home"),
  viewWaiting: $("view-waiting"),
  viewRoom: $("view-room"),

  homeError: $("home-error"),
  btnCreate: $("btn-create"),
  formJoin: $("form-join"),
  inputPin: $("input-pin"),
  btnJoin: $("btn-join"),

  pinValue: $("pin-value"),
  btnCopyPin: $("btn-copy-pin"),
  waitingMessage: $("waiting-message"),
  btnCancelWaiting: $("btn-cancel-waiting"),

  roomPinLabel: $("room-pin-label"),
  btnEnd: $("btn-end"),
  messages: $("messages"),
  transfers: $("transfers"),
  formSend: $("form-send"),
  fileInput: $("file-input"),
  btnAttach: $("btn-attach"),
  inputMessage: $("input-message"),
  btnSendText: $("btn-send-text"),

  tplMessage: $("tpl-message"),
  tplTransfer: $("tpl-transfer"),
};

/* =========================================================================
   STATE
   ========================================================================= */

const state = {
  ws: null,
  pc: null,
  dc: null,
  role: null,          // "initiator" | "joiner"
  pin: null,
  pendingCandidates: [],
  outgoingQueue: [],    // File objects waiting to be sent
  sending: false,
  incoming: null,       // { id, name, size, mime, received, chunks[] }
  transferEls: new Map(), // transfer id -> { root, bar, meta, downloadBtn, cancelBtn, img }
  outgoingCancelled: new Set(),
};

/* =========================================================================
   BROWSER SUPPORT CHECK
   ========================================================================= */

function browserSupportsRequiredFeatures() {
  return !!(window.RTCPeerConnection && window.WebSocket && window.Blob && window.File);
}

if (!browserSupportsRequiredFeatures()) {
  el.unsupportedBanner.hidden = false;
  el.btnCreate.disabled = true;
  el.btnJoin.disabled = true;
}

/* =========================================================================
   VIEW / STATUS HELPERS
   ========================================================================= */

function showView(view) {
  el.viewHome.hidden = view !== "home";
  el.viewWaiting.hidden = view !== "waiting";
  el.viewRoom.hidden = view !== "room";
}

// Are we currently in the middle of (or already inside) a tunnel session?
// Used to stop the home screen's Create/Join actions from firing again
// once a session has already started.
function inActiveSession() {
  return !!state.role || !el.viewWaiting.hidden || !el.viewRoom.hidden;
}

function setStatus(status, label) {
  el.statusIndicator.hidden = status === "idle";
  el.statusIndicator.className = "status status-" + status;
  el.statusText.textContent = label;
}

function showHomeError(message) {
  el.homeError.textContent = message;
  el.homeError.hidden = !message;
}

function resetHomeErrorSoon() {
  setTimeout(() => showHomeError(""), 6000);
}

/* =========================================================================
   SIGNALING (WebSocket)
   ========================================================================= */

function connectSignaling() {
  return new Promise((resolve, reject) => {
    let settled = false;
    let socket;
    try {
      socket = new WebSocket(SIGNALING_SERVER_URL);
    } catch (err) {
      reject(new Error("Could not open a connection to the signaling server."));
      return;
    }

    const onOpen = () => {
      settled = true;
      state.ws = socket;
      socket.removeEventListener("error", onError);
      resolve(socket);
    };
    const onError = () => {
      if (!settled) {
        settled = true;
        reject(new Error("Could not reach the signaling server. It may be offline or waking up."));
      }
    };

    socket.addEventListener("open", onOpen, { once: true });
    socket.addEventListener("error", onError, { once: true });

    socket.addEventListener("message", onSignalingMessage);
    socket.addEventListener("close", onSignalingClose);
  });
}

function sendSignal(obj) {
  if (state.ws && state.ws.readyState === WebSocket.OPEN) {
    state.ws.send(JSON.stringify(obj));
  }
}

function onSignalingClose() {
  if (state.role && !el.viewRoom.hidden) {
    // We were mid-session and lost the signaling link before or during
    // negotiation. The WebRTC connection itself (if already up) is
    // unaffected, but reconnection support isn't implemented, so warn.
    if (!state.dc || state.dc.readyState !== "open") {
      setStatus("failed", "Connection failed");
      addSystemMessage("Lost connection to the signaling server before the tunnel finished connecting.");
    }
  }
}

async function onSignalingMessage(event) {
  let msg;
  try {
    msg = JSON.parse(event.data);
  } catch {
    return;
  }
  if (!msg || typeof msg !== "object" || !msg.type) return;

  switch (msg.type) {
    case "created":
      state.pin = msg.room;
      state.role = "initiator";
      el.pinValue.textContent = formatPin(state.pin);
      el.roomPinLabel.textContent = formatPin(state.pin);
      showView("waiting");
      setStatus("waiting", "Waiting for peer\u2026");
      break;

    case "joined":
      state.pin = msg.room;
      state.role = "joiner";
      el.roomPinLabel.textContent = formatPin(state.pin);
      enterRoom();
      setStatus("negotiating", "Negotiating\u2026");
      addSystemMessage("Joined tunnel. Waiting for the connection to establish\u2026");
      break;

    case "peer-joined":
      // We are the initiator; the other side just joined our room.
      el.waitingMessage.textContent = "Peer found, connecting\u2026";
      enterRoom();
      setStatus("negotiating", "Negotiating\u2026");
      await startWebRTC(true);
      break;

    case "peer-left":
      handlePeerLeft();
      break;

    case "room-expired":
      handleRoomExpired();
      break;

    case "offer":
      await handleRemoteOffer(msg.data);
      break;

    case "answer":
      await handleRemoteAnswer(msg.data);
      break;

    case "ice-candidate":
      await handleRemoteIceCandidate(msg.data);
      break;

    case "error":
      handleSignalingError(msg.message || "Unknown error.");
      break;

    default:
      break;
  }
}

function handleSignalingError(message) {
  if (el.viewHome.hidden === false || (!el.viewWaiting.hidden && !state.dc)) {
    showHomeError(message);
    resetHomeErrorSoon();
  }
  if (!el.viewWaiting.hidden) {
    // e.g. tried to create while already in a room, so return home
    showView("home");
    setStatus("idle", "Idle");
  }
}

function formatPin(pin) {
  return pin || "------";
}

/* =========================================================================
   HOME ACTIONS
   ========================================================================= */

el.btnCreate.addEventListener("click", async () => {
  // Guard: ignore if a session is already starting/active (e.g. the user
  // is already in the waiting room or in a live chat).
  if (inActiveSession()) return;

  showHomeError("");
  el.btnCreate.disabled = true;
  el.btnJoin.disabled = true;
  try {
    await connectSignaling();
    sendSignal({ type: "create" });
  } catch (err) {
    showHomeError(err.message);
    resetHomeErrorSoon();
  } finally {
    el.btnCreate.disabled = false;
    el.btnJoin.disabled = false;
  }
});

el.formJoin.addEventListener("submit", async (e) => {
  e.preventDefault();
  if (inActiveSession()) return;

  const pin = el.inputPin.value.trim();
  if (!/^[0-9]{6}$/.test(pin)) {
    showHomeError("Enter the 6-digit PIN exactly as it was given to you.");
    resetHomeErrorSoon();
    return;
  }
  showHomeError("");
  el.btnJoin.disabled = true;
  el.btnCreate.disabled = true;
  try {
    await connectSignaling();
    sendSignal({ type: "join", room: pin });
  } catch (err) {
    showHomeError(err.message);
    resetHomeErrorSoon();
  } finally {
    el.btnJoin.disabled = false;
    el.btnCreate.disabled = false;
  }
});

el.btnCopyPin.addEventListener("click", async () => {
  const text = el.pinValue.textContent;
  try {
    await navigator.clipboard.writeText(text);
    el.btnCopyPin.textContent = "Copied";
    setTimeout(() => (el.btnCopyPin.textContent = "Copy"), 1500);
  } catch {
    // Clipboard API unavailable, fall back silently (PIN is visible anyway).
  }
});

el.btnCancelWaiting.addEventListener("click", () => {
  teardown();
  showView("home");
  setStatus("idle", "Idle");
});

/* =========================================================================
   ROOM ENTRY
   ========================================================================= */

function enterRoom() {
  showView("room");
  el.messages.innerHTML = "";
  el.transfers.innerHTML = "";
  el.transfers.hidden = true;

  // The room view is its own scroll container (see CSS), but make sure the
  // outer page itself is scrolled to the top so the composer at the bottom
  // of the room view is on-screen immediately, with no hunting required.
  window.scrollTo(0, 0);

  // Focus the message box so the person can start typing the moment the
  // data channel opens (it's enabled in setupDataChannel's onopen handler).
  el.inputMessage.focus();
}

/* =========================================================================
   WEBRTC SETUP
   ========================================================================= */

async function startWebRTC(isInitiator) {
  const pc = new RTCPeerConnection({ iceServers: ICE_SERVERS });
  state.pc = pc;
  state.pendingCandidates = [];

  pc.onicecandidate = (event) => {
    if (event.candidate) {
      sendSignal({ type: "ice-candidate", data: event.candidate.toJSON() });
    }
  };

  pc.onconnectionstatechange = () => {
    switch (pc.connectionState) {
      case "connecting":
        setStatus("negotiating", "Negotiating\u2026");
        break;
      case "connected":
        // Actual "connected" UI state is driven by the data channel opening.
        break;
      case "disconnected":
        setStatus("disconnected", "Disconnected");
        addSystemMessage("Peer connection interrupted.");
        break;
      case "failed":
        setStatus("failed", "Connection failed");
        addSystemMessage("The connection failed. This can happen on restrictive networks that require a TURN server.");
        failAllActiveTransfers();
        break;
      case "closed":
        setStatus("disconnected", "Disconnected");
        break;
    }
  };

  if (isInitiator) {
    const channel = pc.createDataChannel("tunnel", { ordered: true });
    setupDataChannel(channel);

    const offer = await pc.createOffer();
    await pc.setLocalDescription(offer);
    sendSignal({ type: "offer", data: offer });
  } else {
    pc.ondatachannel = (event) => setupDataChannel(event.channel);
  }
}

async function handleRemoteOffer(data) {
  if (!state.pc) {
    await startWebRTC(false);
  }
  const pc = state.pc;
  await pc.setRemoteDescription(new RTCSessionDescription(data));
  await flushPendingCandidates();
  const answer = await pc.createAnswer();
  await pc.setLocalDescription(answer);
  sendSignal({ type: "answer", data: answer });
}

async function handleRemoteAnswer(data) {
  if (!state.pc) return;
  await state.pc.setRemoteDescription(new RTCSessionDescription(data));
  await flushPendingCandidates();
}

async function handleRemoteIceCandidate(data) {
  if (!data) return;
  const candidate = new RTCIceCandidate(data);
  if (state.pc && state.pc.remoteDescription) {
    try {
      await state.pc.addIceCandidate(candidate);
    } catch (err) {
      console.warn("Failed to add ICE candidate", err);
    }
  } else {
    state.pendingCandidates.push(candidate);
  }
}

async function flushPendingCandidates() {
  const queued = state.pendingCandidates;
  state.pendingCandidates = [];
  for (const candidate of queued) {
    try {
      await state.pc.addIceCandidate(candidate);
    } catch (err) {
      console.warn("Failed to add queued ICE candidate", err);
    }
  }
}

function setupDataChannel(channel) {
  state.dc = channel;
  channel.binaryType = "arraybuffer";
  channel.bufferedAmountLowThreshold = SEND_BUFFER_LOW_WATER;

  channel.onopen = () => {
    setStatus("connected", "Connected");
    el.inputMessage.disabled = false;
    el.btnSendText.disabled = false;
    addSystemMessage("Connected. Messages and files now travel directly between your browsers.");
    el.inputMessage.focus();
  };

  channel.onclose = () => {
    setStatus("disconnected", "Disconnected");
    el.inputMessage.disabled = true;
    el.btnSendText.disabled = true;
    failAllActiveTransfers();
  };

  channel.onerror = (event) => {
    console.warn("Data channel error", event);
  };

  channel.onmessage = (event) => {
    if (typeof event.data === "string") {
      handleControlMessage(event.data);
    } else {
      handleIncomingChunk(event.data);
    }
  };
}

/* =========================================================================
   PEER LEFT / ROOM EXPIRED / TEARDOWN
   ========================================================================= */

function handlePeerLeft() {
  setStatus("disconnected", "Disconnected");
  addSystemMessage("The other person disconnected. This tunnel is now closed.");
  el.inputMessage.disabled = true;
  el.btnSendText.disabled = true;
  failAllActiveTransfers();
  closeConnectionsOnly();
}

function handleRoomExpired() {
  addSystemMessage("This tunnel expired due to inactivity.");
  setStatus("disconnected", "Disconnected");
  closeConnectionsOnly();
}

function closeConnectionsOnly() {
  if (state.dc) {
    try { state.dc.close(); } catch {}
  }
  if (state.pc) {
    try { state.pc.close(); } catch {}
  }
  state.dc = null;
  state.pc = null;
}

function teardown() {
  sendSignal({ type: "leave" });
  closeConnectionsOnly();
  if (state.ws) {
    try { state.ws.close(); } catch {}
  }
  state.ws = null;
  state.role = null;
  state.pin = null;
  state.pendingCandidates = [];
  state.outgoingQueue = [];
  state.sending = false;
  state.incoming = null;
  state.outgoingCancelled.clear();
  for (const t of state.transferEls.values()) {
    if (t.objectUrl) URL.revokeObjectURL(t.objectUrl);
  }
  state.transferEls.clear();
  el.inputMessage.value = "";
  el.inputMessage.disabled = true;
  el.btnSendText.disabled = true;
}

el.btnEnd.addEventListener("click", () => {
  teardown();
  showView("home");
  setStatus("idle", "Idle");
});

window.addEventListener("beforeunload", () => {
  if (state.ws || state.pc) {
    try { sendSignal({ type: "leave" }); } catch {}
  }
});

/* =========================================================================
   CHAT
   ========================================================================= */

el.formSend.addEventListener("submit", (e) => {
  e.preventDefault();
  const text = el.inputMessage.value.trim();
  if (!text || !state.dc || state.dc.readyState !== "open") return;
  const ts = Date.now();
  state.dc.send(JSON.stringify({ type: "chat", text, ts }));
  addChatMessage("You", text, ts, true);
  el.inputMessage.value = "";
});

function handleControlMessage(raw) {
  let msg;
  try {
    msg = JSON.parse(raw);
  } catch {
    return;
  }
  if (!msg || typeof msg !== "object") return;

  switch (msg.type) {
    case "chat":
      addChatMessage("Peer", msg.text, msg.ts, false);
      break;
    case "file-meta":
      beginIncomingTransfer(msg);
      break;
    case "file-end":
      completeIncomingTransfer(msg.id);
      break;
    case "file-cancel":
      cancelIncomingTransfer(msg.id, msg.reason);
      break;
    default:
      break;
  }
}

function addChatMessage(author, text, ts, fromMe) {
  const node = el.tplMessage.content.firstElementChild.cloneNode(true);
  node.classList.add(fromMe ? "from-me" : "from-peer");
  node.querySelector(".message-author").textContent = author;
  node.querySelector(".message-time").textContent = formatTime(ts);
  node.querySelector(".message-text").textContent = text;
  el.messages.appendChild(node);
  el.messages.scrollTop = el.messages.scrollHeight;
}

function addSystemMessage(text) {
  const node = el.tplMessage.content.firstElementChild.cloneNode(true);
  node.classList.add("system");
  node.querySelector(".message-author").textContent = "";
  node.querySelector(".message-time").textContent = formatTime(Date.now());
  node.querySelector(".message-text").textContent = text;
  el.messages.appendChild(node);
  el.messages.scrollTop = el.messages.scrollHeight;
}

function formatTime(ts) {
  const d = new Date(ts || Date.now());
  return d.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

/* =========================================================================
   FILE TRANSFER: SENDING
   ========================================================================= */

el.btnAttach.addEventListener("click", () => el.fileInput.click());

el.fileInput.addEventListener("change", () => {
  const files = Array.from(el.fileInput.files || []);
  el.fileInput.value = "";
  if (!files.length) return;
  if (!state.dc || state.dc.readyState !== "open") {
    addSystemMessage("Can't send files: not connected.");
    return;
  }
  for (const file of files) {
    state.outgoingQueue.push(file);
  }
  processOutgoingQueue();
});

function generateId() {
  if (window.crypto && crypto.randomUUID) return crypto.randomUUID();
  return "id-" + Math.random().toString(36).slice(2) + Date.now().toString(36);
}

async function processOutgoingQueue() {
  if (state.sending) return;
  state.sending = true;
  while (state.outgoingQueue.length) {
    const file = state.outgoingQueue.shift();
    try {
      await sendFile(file);
    } catch (err) {
      console.warn("File send failed", err);
    }
  }
  state.sending = false;
}

async function sendFile(file) {
  const id = generateId();
  const mime = file.type || "application/octet-stream";
  const row = createTransferRow(id, file.name, file.size, "out");

  state.dc.send(JSON.stringify({
    type: "file-meta", id, name: file.name, size: file.size, mime,
  }));

  let offset = 0;
  try {
    while (offset < file.size) {
      if (state.outgoingCancelled.has(id)) {
        throw new Error("cancelled");
      }
      if (!state.dc || state.dc.readyState !== "open") {
        throw new Error("channel closed");
      }
      await waitForBufferSpace();
      const slice = file.slice(offset, offset + CHUNK_SIZE);
      const buffer = await slice.arrayBuffer();
      state.dc.send(buffer);
      offset += buffer.byteLength;
      updateTransferProgress(id, offset, file.size);
    }
    state.dc.send(JSON.stringify({ type: "file-end", id }));
    markTransferComplete(id, file);
  } catch (err) {
    if (err.message === "cancelled") {
      markTransferFailed(id, "Cancelled");
      if (state.dc && state.dc.readyState === "open") {
        state.dc.send(JSON.stringify({ type: "file-cancel", id, reason: "cancelled by sender" }));
      }
    } else {
      markTransferFailed(id, "Transfer failed");
    }
  } finally {
    state.outgoingCancelled.delete(id);
  }
}

function waitForBufferSpace() {
  const dc = state.dc;
  if (!dc || dc.bufferedAmount <= SEND_BUFFER_HIGH_WATER) {
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    const onLow = () => {
      dc.removeEventListener("bufferedamountlow", onLow);
      resolve();
    };
    dc.addEventListener("bufferedamountlow", onLow);
  });
}

/* =========================================================================
   FILE TRANSFER: RECEIVING
   ========================================================================= */

function beginIncomingTransfer(meta) {
  state.incoming = {
    id: meta.id,
    name: meta.name,
    size: meta.size,
    mime: meta.mime || "application/octet-stream",
    received: 0,
    chunks: [],
  };
  createTransferRow(meta.id, meta.name, meta.size, "in");
}

function handleIncomingChunk(data) {
  const t = state.incoming;
  if (!t) return; // stray chunk with no active metadata, ignore defensively
  t.chunks.push(data);
  t.received += data.byteLength;
  updateTransferProgress(t.id, t.received, t.size);
}

function completeIncomingTransfer(id) {
  const t = state.incoming;
  if (!t || t.id !== id) return;
  let blob;
  try {
    blob = new Blob(t.chunks, { type: t.mime });
  } catch (err) {
    markTransferFailed(id, "Could not reconstruct file");
    state.incoming = null;
    return;
  }
  if (blob.size !== t.size) {
    // Not fatal, surface as a soft warning but still offer the download.
    console.warn(`Received size (${blob.size}) does not match announced size (${t.size}) for ${t.name}`);
  }
  const url = URL.createObjectURL(blob);
  markTransferComplete(id, { name: t.name, size: blob.size, type: t.mime }, url);
  state.incoming = null;
}

function cancelIncomingTransfer(id, reason) {
  if (state.incoming && state.incoming.id === id) {
    state.incoming = null;
  }
  markTransferFailed(id, reason ? "Cancelled by peer" : "Cancelled");
}

/* =========================================================================
   TRANSFER UI
   ========================================================================= */

function createTransferRow(id, name, size, direction) {
  el.transfers.hidden = false;
  const node = el.tplTransfer.content.firstElementChild.cloneNode(true);
  node.dataset.id = id;
  node.querySelector(".transfer-name").textContent =
    (direction === "out" ? "\u2191 " : "\u2193 ") + name;
  node.querySelector(".transfer-meta").textContent = `0 B / ${formatBytes(size)}`;

  const cancelBtn = node.querySelector(".transfer-cancel");
  const downloadBtn = node.querySelector(".transfer-download");
  const bar = node.querySelector(".progress-bar");
  const img = node.querySelector(".transfer-preview");

  cancelBtn.addEventListener("click", () => {
    if (direction === "out") {
      state.outgoingCancelled.add(id);
    } else {
      if (state.incoming && state.incoming.id === id) {
        state.dc && state.dc.readyState === "open" &&
          state.dc.send(JSON.stringify({ type: "file-cancel", id, reason: "cancelled by receiver" }));
        state.incoming = null;
        markTransferFailed(id, "Cancelled");
      }
    }
  });

  el.transfers.appendChild(node);
  state.transferEls.set(id, { root: node, bar, meta: node.querySelector(".transfer-meta"), cancelBtn, downloadBtn, img, size, direction });
  return node;
}

function updateTransferProgress(id, sent, total) {
  const t = state.transferEls.get(id);
  if (!t) return;
  const pct = total > 0 ? Math.min(100, (sent / total) * 100) : 100;
  t.bar.style.width = pct.toFixed(1) + "%";
  t.meta.textContent = `${formatBytes(sent)} / ${formatBytes(total)}`;
}

function markTransferComplete(id, fileInfo, objectUrl) {
  const t = state.transferEls.get(id);
  if (!t) return;
  t.bar.style.width = "100%";
  t.meta.textContent = `${formatBytes(fileInfo.size)} - complete`;
  t.cancelBtn.hidden = true;

  if (objectUrl) {
    t.objectUrl = objectUrl;
    t.downloadBtn.href = objectUrl;
    t.downloadBtn.download = fileInfo.name;
    t.downloadBtn.hidden = false;

    if (fileInfo.type && fileInfo.type.startsWith("image/")) {
      t.img.src = objectUrl;
      t.img.hidden = false;
    }
  }
}

function markTransferFailed(id, reason) {
  const t = state.transferEls.get(id);
  if (!t) return;
  t.root.classList.add("failed");
  t.meta.textContent = reason || "Failed";
  t.cancelBtn.hidden = true;
}

function failAllActiveTransfers() {
  for (const [id, t] of state.transferEls.entries()) {
    if (!t.root.classList.contains("failed") && t.downloadBtn.hidden) {
      markTransferFailed(id, "Connection lost");
    }
  }
  state.incoming = null;
  state.outgoingQueue = [];
}

function formatBytes(bytes) {
  if (bytes === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(bytes) / Math.log(1024)));
  const value = bytes / Math.pow(1024, i);
  return `${value.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

/* =========================================================================
   MISC
   ========================================================================= */

el.inputPin.addEventListener("input", () => {
  el.inputPin.value = el.inputPin.value.replace(/[^0-9]/g, "").slice(0, 6);
});

setStatus("idle", "Idle");
showView("home");