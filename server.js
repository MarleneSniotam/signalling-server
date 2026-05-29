// PeerLink Signalling Server
// Deploy on Railway — handles room join, WebRTC offer/answer/ICE relay
// Also serves the webapp so join links work: /join/:roomId

const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = process.env.PORT || 3000;

// ── Room registry: roomId → Set of WebSocket clients ──
const rooms = new Map();

function joinRoom(roomId, ws) {
  if (!rooms.has(roomId)) rooms.set(roomId, new Set());
  rooms.get(roomId).add(ws);
  ws._room = roomId;
  console.log(`[ROOM] ${ws._peerId} joined "${roomId}" (${rooms.get(roomId).size} peers)`);
}

function leaveRoom(ws) {
  const roomId = ws._room;
  if (!roomId || !rooms.has(roomId)) return;
  rooms.get(roomId).delete(ws);
  console.log(`[ROOM] ${ws._peerId} left "${roomId}" (${rooms.get(roomId).size} peers)`);
  if (rooms.get(roomId).size === 0) rooms.delete(roomId);
  else broadcastToRoom(roomId, { type: 'peer-left', peerId: ws._peerId }, ws);
}

function broadcastToRoom(roomId, msg, excludeWs = null) {
  const room = rooms.get(roomId);
  if (!room) return;
  const json = JSON.stringify(msg);
  room.forEach(client => {
    if (client !== excludeWs && client.readyState === WebSocket.OPEN) {
      client.send(json);
    }
  });
}

function sendToPeer(roomId, msg, senderWs) {
  // Relay to all others in the room (for 2-person calls, that's just the other peer)
  broadcastToRoom(roomId, msg, senderWs);
}

// ── HTTP server — serves the webapp ──
const WEBAPP_PATH = path.join(__dirname, 'webapp', 'index.html');

function serveWebApp(res, roomId) {
  let html;
  try {
    html = fs.readFileSync(WEBAPP_PATH, 'utf8');
  } catch {
    res.writeHead(404);
    res.end('Webapp not found');
    return;
  }
  // Inject room ID into the page via meta tag for client to pick up
  html = html.replace('<head>', `<head><meta name="peerlink-room" content="${roomId}" />`);
  res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-store' });
  res.end(html);
}

const server = http.createServer((req, res) => {
  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;

  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }

  // Health check
  if (pathname === '/health' || pathname === '/') {
    const activeRooms = rooms.size;
    const activePeers = [...rooms.values()].reduce((a, s) => a + s.size, 0);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', activeRooms, activePeers, uptime: process.uptime() }));
    return;
  }

  // Join link: /join/:roomId
  const joinMatch = pathname.match(/^\/join\/([a-zA-Z0-9_-]+)$/);
  if (joinMatch) {
    serveWebApp(res, joinMatch[1]);
    return;
  }

  // API: room info
  const roomMatch = pathname.match(/^\/api\/room\/([a-zA-Z0-9_-]+)$/);
  if (roomMatch) {
    const r = rooms.get(roomMatch[1]);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ room: roomMatch[1], peers: r ? r.size : 0 }));
    return;
  }

  res.writeHead(404);
  res.end('Not found');
});

// ── WebSocket Server ──
const wss = new WebSocket.Server({ server });
let peerCounter = 0;

wss.on('connection', (ws, req) => {
  ws._peerId = `peer-${++peerCounter}`;
  ws._room = null;

  console.log(`[WS] New connection: ${ws._peerId}`);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    const room = msg.room;
    if (!room && msg.type !== 'ping') return;

    switch (msg.type) {
      case 'join':
        joinRoom(room, ws);
        // Notify existing peers
        const roomSize = rooms.get(room)?.size || 0;
        if (roomSize > 1) {
          // Tell existing peers someone joined
          broadcastToRoom(room, { type: 'peer-joined', peerId: ws._peerId, peerCount: roomSize }, ws);
          // Tell the newcomer how many peers are already there
          ws.send(JSON.stringify({ type: 'room-state', peerCount: roomSize - 1 }));
        }
        break;

      case 'offer':
        sendToPeer(room, { type: 'offer', sdp: msg.sdp, from: ws._peerId }, ws);
        break;

      case 'answer':
        sendToPeer(room, { type: 'answer', sdp: msg.sdp, from: ws._peerId }, ws);
        break;

      case 'ice':
        sendToPeer(room, { type: 'ice', candidate: msg.candidate, from: ws._peerId }, ws);
        break;

      case 'leave':
        leaveRoom(ws);
        break;

      case 'ping':
        ws.send(JSON.stringify({ type: 'pong' }));
        break;
    }
  });

  ws.on('close', () => leaveRoom(ws));
  ws.on('error', (e) => console.error(`[WS] Error ${ws._peerId}:`, e.message));

  // Keepalive
  ws.isAlive = true;
  ws.on('pong', () => ws.isAlive = true);
});

// Heartbeat to clean up dead connections
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) { leaveRoom(ws); ws.terminate(); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

server.listen(PORT, () => {
  console.log(`\n🚀 PeerLink Signalling Server running on port ${PORT}`);
  console.log(`   WebSocket: ws://localhost:${PORT}`);
  console.log(`   HTTP:      http://localhost:${PORT}`);
  console.log(`   Join URL:  http://localhost:${PORT}/join/:roomId\n`);
});
