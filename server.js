/**
 * peer.chat — Signalling Server
 *
 * Minimal WebSocket server that relays WebRTC offers, answers,
 * and ICE candidates between peers in the same room.
 *
 * The server NEVER sees video/audio data — it only handles
 * the initial handshake. Once peers connect, traffic is P2P.
 *
 * Deploy to: Railway, Fly.io, Render, or any Node host.
 *
 * Usage:
 *   npm install
 *   node server.js
 *   # or: PORT=3001 node server.js
 */

const WebSocket = require('ws');

const PORT = process.env.PORT || 3001;
const wss  = new WebSocket.Server({ port: PORT });

// rooms: Map<roomId, Map<socketId, { ws, username }>>
const rooms = new Map();
let nextId = 1;

wss.on('listening', () => {
  console.log(`[signal] Server listening on ws://localhost:${PORT}`);
});

wss.on('connection', (ws) => {
  const socketId = nextId++;
  ws._id = socketId;
  ws._room = null;

  console.log(`[signal] Client ${socketId} connected`);

  ws.on('message', (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return sendTo(ws, { type: 'error', message: 'Invalid JSON' });
    }

    switch (msg.type) {
      case 'join':
        handleJoin(ws, msg);
        break;
      case 'offer':
      case 'answer':
      case 'ice-candidate':
        relay(ws, msg);
        break;
      default:
        sendTo(ws, { type: 'error', message: `Unknown type: ${msg.type}` });
    }
  });

  ws.on('close', () => {
    handleLeave(ws);
    console.log(`[signal] Client ${socketId} disconnected`);
  });

  ws.on('error', (err) => {
    console.error(`[signal] Client ${socketId} error:`, err.message);
  });
});

/* ── Room management ────────────────────────────────────────────── */

function handleJoin(ws, msg) {
  const { room, username } = msg;

  if (!room || typeof room !== 'string' || room.length > 64) {
    return sendTo(ws, { type: 'error', message: 'Invalid room name' });
  }

  ws._room = room;
  ws._username = username || 'anon';

  if (!rooms.has(room)) {
    rooms.set(room, new Map());
  }

  const peers = rooms.get(room);

  if (peers.size >= 2) {
    return sendTo(ws, { type: 'error', message: 'Room is full (max 2 peers)' });
  }

  if (peers.size === 0) {
    // First peer — wait for someone to join
    peers.set(ws._id, { ws, username: ws._username });
    sendTo(ws, { type: 'room-joined', room, peerCount: 0 });
    console.log(`[signal] ${ws._username} created room "${room}"`);
  } else {
    // Second peer — notify both sides
    peers.set(ws._id, { ws, username: ws._username });

    // Tell the new peer the room is ready
    sendTo(ws, { type: 'room-joined', room, peerCount: 1 });

    // Tell the existing peer to initiate the call
    broadcastToOthers(ws, {
      type: 'peer-joined',
      username: ws._username
    });

    console.log(`[signal] ${ws._username} joined room "${room}" — starting call`);
  }
}

function handleLeave(ws) {
  if (!ws._room) return;

  const peers = rooms.get(ws._room);
  if (!peers) return;

  peers.delete(ws._id);

  // Notify remaining peer
  broadcastToOthers(ws, {
    type: 'peer-left',
    username: ws._username
  });

  // Clean up empty rooms
  if (peers.size === 0) {
    rooms.delete(ws._room);
    console.log(`[signal] Room "${ws._room}" closed`);
  }
}

/* ── Message relay ──────────────────────────────────────────────── */

function relay(ws, msg) {
  // Attach room and sender username, then forward to the other peer
  broadcastToOthers(ws, {
    ...msg,
    username: ws._username
  });
}

/* ── Helpers ────────────────────────────────────────────────────── */

function sendTo(ws, data) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(data));
  }
}

function broadcastToOthers(senderWs, data) {
  const peers = rooms.get(senderWs._room);
  if (!peers) return;
  peers.forEach(({ ws }) => {
    if (ws !== senderWs) sendTo(ws, data);
  });
}

/* ── Health check (HTTP ping for Railway / Fly.io keep-alive) ─── */
const http = require('http');
const healthServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    const roomCount = rooms.size;
    const peerCount = [...rooms.values()].reduce((n, r) => n + r.size, 0);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', rooms: roomCount, peers: peerCount }));
  } else {
    res.writeHead(404);
    res.end();
  }
});

healthServer.listen(process.env.HEALTH_PORT || 3000, () => {
  console.log(`[signal] Health check on http://localhost:${process.env.HEALTH_PORT || 3000}/health`);
});
