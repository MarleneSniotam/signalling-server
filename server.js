/*
  PeerLink Signalling Server
  Serves join.html at /join and handles WebSocket signalling.
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

  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('PeerLink Signalling Server is running ✓');
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  console.log('New connection');
  ws.roomId = null;
  ws.role = null;

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); }
    catch { return; }

    console.log('MSG:', msg.type, '| room:', msg.room || '-');

    switch (msg.type) {

      case 'create': {
        const roomId = msg.room;
        if (!roomId) return;
        if (rooms.has(roomId)) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room already exists. Try again.' }));
          return;
        }
        rooms.set(roomId, { host: ws, guest: null });
        ws.roomId = roomId;
        ws.role = 'host';
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
        if (room.guest) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room is full.' }));
          return;
        }
        room.guest = ws;
        ws.roomId = roomId;
        ws.role = 'guest';

        // Tell guest they joined successfully
        ws.send(JSON.stringify({ type: 'guest-joined', room: roomId }));

        // Tell host a guest arrived — host makes the offer
        if (room.host && room.host.readyState === WebSocket.OPEN) {
          room.host.send(JSON.stringify({ type: 'peer-arrived', room: roomId }));
        }

        console.log('Guest joined room:', roomId);
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

        // Send to the other person
        const other = ws.role === 'host' ? room.guest : room.host;
        if (other && other.readyState === WebSocket.OPEN) {
          other.send(JSON.stringify(msg));
        }

        if (msg.type === 'leave') cleanup(ws);
        break;
      }
    }
  });

  ws.on('close', () => { cleanup(ws); });
  ws.on('error', (err) => { console.error('WS error:', err.message); cleanup(ws); });
});

function cleanup(ws) {
  const roomId = ws.roomId;
  if (!roomId) return;
  const room = rooms.get(roomId);
  if (!room) return;

  // Notify the other person
  const other = ws.role === 'host' ? room.guest : room.host;
  if (other && other.readyState === WebSocket.OPEN) {
    other.send(JSON.stringify({ type: 'peer-left' }));
  }

  // Remove from room
  if (ws.role === 'host') {
    rooms.delete(roomId);
    console.log('Host left, room deleted:', roomId);
  } else {
    room.guest = null;
    console.log('Guest left room:', roomId);
  }
  ws.roomId = null;
  ws.role = null;
}

server.listen(PORT, () => {
  console.log(`PeerLink signalling server running on port ${PORT}`);
});
