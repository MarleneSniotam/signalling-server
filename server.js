/*
  PeerLink Signalling Server — Extended
  ======================================
  Supports two protocols on the same port:

  1. CyberPhon.link — 1-to-1 peer video call (unchanged)
     Messages: create, join, offer, answer, ice-candidate, leave

  2. Signal Tower (CyberTheatron.LGBT) — 1-to-many broadcast
     Messages: register-host, join-room, offer*, answer*, ice-candidate*, kick, host-update
     (* routed by msg.to peerId instead of msg.room)

  The two protocols share the same server.js and the same Railway service.
  No changes needed to the existing cyberphon.link client code.
*/

const WebSocket = require('ws');
const http      = require('http');
const fs        = require('fs');
const path      = require('path');
const url       = require('url');

const PORT = process.env.PORT || 8080;

// ── CyberPhon 1-to-1 rooms ────────────────────────────────────────────────────
// roomId → { host: ws, guest: ws|null }
const cpRooms = new Map();

// ── Signal Tower 1-to-many rooms ─────────────────────────────────────────────
// nodeId  → { hostPeerId, hostWs, viewers: Map<peerId, {ws, handle}> }
const stRooms = new Map();
// peerId  → { ws, nodeId, role: 'host'|'viewer'|null }
const stPeers = new Map();

function uid() {
  if (typeof crypto !== 'undefined' && crypto.randomUUID) return crypto.randomUUID();
  return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

function send(ws, data) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(data)); } catch (_) {}
  }
}

// ── HTTP server ───────────────────────────────────────────────────────────────
const server = http.createServer((req, res) => {
  const parsed   = url.parse(req.url, true);
  const pathname = parsed.pathname;

  if (pathname === '/join' || pathname === '/join.html') {
    const filePath = path.join(__dirname, 'join.html');
    fs.readFile(filePath, (err, data) => {
      if (err) {
        res.writeHead(500, { 'Content-Type': 'text/plain' });
        res.end('Error: join.html not found on server.');
        return;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
      res.end(data);
    });
    return;
  }

  if (pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status  : 'ok',
      service : 'PeerLink + Signal Tower',
      cpRooms : cpRooms.size,
      stRooms : stRooms.size,
      stPeers : stPeers.size,
    }));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('PeerLink Signalling Server is running ✓');
});

// ── WebSocket server ──────────────────────────────────────────────────────────
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  console.log('New connection');

  // CyberPhon state (on ws directly — existing behaviour unchanged)
  ws.roomId = null;
  ws.role   = null;

  // Signal Tower state — each connection gets a unique peerId
  const stPeerId = uid();
  stPeers.set(stPeerId, { ws, nodeId: null, role: null });
  ws._stPeerId = stPeerId;

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch { return; }

    console.log('MSG:', msg.type, '| room:', msg.room || msg.nodeId || '-');

    // offer/answer/ice-candidate with msg.to  → Signal Tower peer routing
    // offer/answer/ice-candidate with msg.room → CyberPhon room routing
    const isSTRelay = ['offer','answer','ice-candidate'].includes(msg.type) && msg.to;
    const isCPRelay = ['offer','answer','ice-candidate','leave'].includes(msg.type) && !msg.to;

    // ── CYBERPHON 1-to-1 ──────────────────────────────────────────────────
    if (msg.type === 'create') {
      const roomId = msg.room;
      if (!roomId) return;
      if (cpRooms.has(roomId)) {
        ws.send(JSON.stringify({ type: 'error', message: 'Room already exists. Try again.' }));
        return;
      }
      cpRooms.set(roomId, { host: ws, guest: null });
      ws.roomId = roomId;
      ws.role   = 'host';
      ws.send(JSON.stringify({ type: 'room-created', room: roomId }));
      console.log('CP room created:', roomId);
      return;
    }

    if (msg.type === 'join' && msg.room) {
      const roomId = msg.room;
      const room   = cpRooms.get(roomId);
      if (!room) {
        ws.send(JSON.stringify({ type: 'error', message: 'Room not found. Check the code.' }));
        return;
      }
      if (room.guest) {
        ws.send(JSON.stringify({ type: 'error', message: 'Room is full.' }));
        return;
      }
      room.guest = ws;
      ws.roomId  = roomId;
      ws.role    = 'guest';
      ws.send(JSON.stringify({ type: 'guest-joined', room: roomId }));
      if (room.host && room.host.readyState === WebSocket.OPEN) {
        room.host.send(JSON.stringify({ type: 'peer-arrived', room: roomId }));
      }
      console.log('CP guest joined room:', roomId);
      return;
    }

    if (isCPRelay) {
      const roomId = msg.room || ws.roomId;
      if (!roomId) return;
      const room = cpRooms.get(roomId);
      if (!room) return;
      const other = ws.role === 'host' ? room.guest : room.host;
      if (other && other.readyState === WebSocket.OPEN) other.send(JSON.stringify(msg));
      if (msg.type === 'leave') cpCleanup(ws);
      return;
    }

    // ── SIGNAL TOWER 1-to-many ────────────────────────────────────────────
    const peer = stPeers.get(stPeerId);
    if (!peer) return;

    if (msg.type === 'register-host') {
      const nodeId = String(msg.nodeId || '').toLowerCase().trim();
      if (!nodeId) return;
      // Notify existing viewers if host reconnects
      if (stRooms.has(nodeId)) {
        stRooms.get(nodeId).viewers.forEach(({ ws: vws }) =>
          send(vws, { type: 'host-reconnected' })
        );
      }
      stRooms.set(nodeId, { hostPeerId: stPeerId, hostWs: ws, viewers: new Map() });
      peer.nodeId = nodeId;
      peer.role   = 'host';
      send(ws, { type: 'registered', peerId: stPeerId, nodeId });
      console.log('[ST] HOST registered:', nodeId);
      return;
    }

    if (msg.type === 'join-room') {
      const nodeId = String(msg.nodeId || '').toLowerCase().trim();
      const handle = String(msg.handle || 'ANONYMOUS').slice(0, 30).toUpperCase();
      if (!nodeId) return;
      if (!stRooms.has(nodeId)) {
        send(ws, { type: 'error', code: 'NO_ROOM', message: 'Node is not currently live.' });
        return;
      }
      const room = stRooms.get(nodeId);
      room.viewers.set(stPeerId, { ws, handle });
      peer.nodeId = nodeId;
      peer.role   = 'viewer';
      send(ws, { type: 'joined', peerId: stPeerId, nodeId, hostPeerId: room.hostPeerId, handle });
      send(room.hostWs, { type: 'viewer-joined', viewerId: stPeerId, handle, viewerCount: room.viewers.size });
      console.log('[ST] VIEWER', handle, 'joined', nodeId);
      return;
    }

    if (isSTRelay) {
      const target = stPeers.get(msg.to);
      if (target) send(target.ws, { ...msg, from: stPeerId });
      return;
    }

    if (msg.type === 'kick') {
      if (peer.role !== 'host') return;
      const room = stRooms.get(peer.nodeId);
      if (!room || room.hostPeerId !== stPeerId) return;
      const viewer = room.viewers.get(msg.viewerId);
      if (viewer) {
        send(viewer.ws, { type: 'kicked', reason: String(msg.reason || 'Removed by host') });
        room.viewers.delete(msg.viewerId);
        const tp = stPeers.get(msg.viewerId);
        if (tp) { tp.nodeId = null; tp.role = null; }
        send(ws, { type: 'viewer-count', count: room.viewers.size });
      }
      return;
    }

    if (msg.type === 'host-update') {
      if (peer.role !== 'host') return;
      const room = stRooms.get(peer.nodeId);
      if (!room) return;
      room.viewers.forEach(({ ws: vws }) =>
        send(vws, { type: 'host-update', data: msg.data })
      );
      return;
    }
  });

  ws.on('close', () => {
    cpCleanup(ws);
    stCleanup(stPeerId);
  });

  ws.on('error', (err) => {
    console.error('WS error:', err.message);
    cpCleanup(ws);
    stCleanup(stPeerId);
  });
});

// ── Cleanup ───────────────────────────────────────────────────────────────────

function cpCleanup(ws) {
  const roomId = ws.roomId;
  if (!roomId) return;
  const room = cpRooms.get(roomId);
  if (!room) return;
  const other = ws.role === 'host' ? room.guest : room.host;
  if (other && other.readyState === WebSocket.OPEN) {
    other.send(JSON.stringify({ type: 'peer-left' }));
  }
  if (ws.role === 'host') {
    cpRooms.delete(roomId);
    console.log('CP host left, room deleted:', roomId);
  } else {
    room.guest = null;
    console.log('CP guest left room:', roomId);
  }
  ws.roomId = null;
  ws.role   = null;
}

function stCleanup(peerId) {
  const peer = stPeers.get(peerId);
  if (!peer) return;
  if (peer.role === 'host' && peer.nodeId) {
    const room = stRooms.get(peer.nodeId);
    if (room && room.hostPeerId === peerId) {
      room.viewers.forEach(({ ws: vws }) => send(vws, { type: 'host-left' }));
      stRooms.delete(peer.nodeId);
      console.log('[ST] HOST offline:', peer.nodeId);
    }
  } else if (peer.role === 'viewer' && peer.nodeId) {
    const room = stRooms.get(peer.nodeId);
    if (room) {
      room.viewers.delete(peerId);
      send(room.hostWs, { type: 'viewer-left', viewerId: peerId, viewerCount: room.viewers.size });
    }
  }
  stPeers.delete(peerId);
}

server.listen(PORT, () => {
  console.log(`PeerLink + Signal Tower signalling server running on port ${PORT}`);
});
