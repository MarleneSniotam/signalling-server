/*
  PeerLink Signalling Server
  --------------------------
  Serves join.html at /join?room=XXXXXX
  Handles WebSocket signalling for WebRTC
*/

const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');
const url = require('url');

const PORT = process.env.PORT || 8080;

const rooms = new Map();

const server = http.createServer((req, res) => {
  const parsed = url.parse(req.url, true);
  const pathname = parsed.pathname;

  // Serve join.html for anyone clicking a room link
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

  // Root health check
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('PeerLink Signalling Server is running ✓');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  console.log('New connection from:', req.socket.remoteAddress);
  ws.roomId = null;

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); }
    catch { return; }

    console.log('Message type:', msg.type, '| Room:', msg.room || '-');

    switch (msg.type) {

      case 'create': {
        const roomId = msg.room;
        if (!roomId) return;
        if (rooms.has(roomId)) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room already exists. Try again.' }));
          return;
        }
        rooms.set(roomId, [ws]);
        ws.roomId = roomId;
        ws.send(JSON.stringify({ type: 'room-created', room: roomId }));
        console.log('Room created:', roomId);
        break;
      }

      case 'join': {
        const roomId = msg.room;
        if (!roomId) return;
        const room = rooms.get(roomId);
        if (!room) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room not found. Check the code.' }));
          return;
        }
        if (room.length >= 2) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room is full.' }));
          return;
        }
        room.push(ws);
        ws.roomId = roomId;
        ws.send(JSON.stringify({ type: 'room-joined', room: roomId }));
        const host = room[0];
        if (host && host.readyState === WebSocket.OPEN) {
          host.send(JSON.stringify({ type: 'room-joined', room: roomId }));
        }
        console.log('Peer joined room:', roomId);
        break;
      }

      case 'offer':
      case 'answer':
      case 'ice-candidate':
      case 'leave': {
        const roomId = msg.room || ws.roomId;
        if (!roomId) return;
        const room = rooms.get(roomId);
        if (!room) return;
        room.forEach(peer => {
          if (peer !== ws && peer.readyState === WebSocket.OPEN) {
            peer.send(JSON.stringify(msg));
          }
        });
        if (msg.type === 'leave') cleanup(ws);
        break;
      }
    }
  });

  ws.on('close', () => { console.log('Connection closed, room:', ws.roomId); cleanup(ws); });
  ws.on('error', (err) => { console.error('WS error:', err.message); cleanup(ws); });
});

function cleanup(ws) {
  const roomId = ws.roomId;
  if (!roomId) return;
  const room = rooms.get(roomId);
  if (!room) return;
  room.forEach(peer => {
    if (peer !== ws && peer.readyState === WebSocket.OPEN) {
      peer.send(JSON.stringify({ type: 'peer-left' }));
    }
  });
  const updated = room.filter(p => p !== ws);
  if (updated.length === 0) { rooms.delete(roomId); console.log('Room deleted:', roomId); }
  else rooms.set(roomId, updated);
  ws.roomId = null;
}

server.listen(PORT, () => {
  console.log(`PeerLink signalling server running on port ${PORT}`);
});
