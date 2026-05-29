/*
  PeerLink Signalling Server
  --------------------------
  Deploy this on Railway (or any Node.js host).

  HOW TO DEPLOY ON RAILWAY:
  1. Go to https://railway.app
  2. Click "New Project" → "Deploy from GitHub repo"
      OR click "New Project" → "Empty Project" → Add service → "Node.js"
  3. Create these two files in your project:
       - server.js  (this file)
       - package.json  (see bottom of this file)
  4. Railway will auto-deploy. Done!

  What this server does:
  - Accepts WebSocket connections
  - Lets one person CREATE a room with a 6-letter code
  - Lets another person JOIN that room
  - Passes messages (offer, answer, ICE candidates) between the two people
  - Does NOT touch the actual video/audio data — that goes peer-to-peer
*/

const WebSocket = require('ws');
const http = require('http');

const PORT = process.env.PORT || 3000;

// Store active rooms: roomId → array of WebSocket connections (max 2)
const rooms = new Map();

// Create HTTP server (Railway needs this for health checks)
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('PeerLink Signalling Server is running ✓');
});

// Attach WebSocket server
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  console.log('New connection from:', req.socket.remoteAddress);

  ws.roomId = null;

  ws.on('message', (data) => {
    let msg;
    try {
      msg = JSON.parse(data.toString());
    } catch {
      return; // ignore malformed messages
    }

    console.log('Message type:', msg.type, '| Room:', msg.room || '-');

    switch (msg.type) {

      // ── Someone wants to CREATE a room ──────────────────
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

      // ── Someone wants to JOIN a room ─────────────────────
      case 'join': {
        const roomId = msg.room;
        if (!roomId) return;

        const room = rooms.get(roomId);
        if (!room) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room not found. Check the code.' }));
          return;
        }
        if (room.length >= 2) {
          ws.send(JSON.stringify({ type: 'error', message: 'Room is full (2 people max).' }));
          return;
        }

        room.push(ws);
        ws.roomId = roomId;

        // Tell the joiner they're in
        ws.send(JSON.stringify({ type: 'room-joined', room: roomId }));

        // Tell the host someone joined (host makes the offer)
        const host = room[0];
        if (host && host.readyState === WebSocket.OPEN) {
          host.send(JSON.stringify({ type: 'room-joined', room: roomId }));
        }

        console.log('Peer joined room:', roomId);
        break;
      }

      // ── Relay: offer, answer, ice-candidate, leave ───────
      case 'offer':
      case 'answer':
      case 'ice-candidate':
      case 'leave': {
        const roomId = msg.room || ws.roomId;
        if (!roomId) return;

        const room = rooms.get(roomId);
        if (!room) return;

        // Send to the OTHER person in the room
        room.forEach(peer => {
          if (peer !== ws && peer.readyState === WebSocket.OPEN) {
            peer.send(JSON.stringify(msg));
          }
        });

        if (msg.type === 'leave') {
          cleanup(ws);
        }
        break;
      }
    }
  });

  ws.on('close', () => {
    console.log('Connection closed, room:', ws.roomId);
    cleanup(ws);
  });

  ws.on('error', (err) => {
    console.error('WebSocket error:', err.message);
    cleanup(ws);
  });
});

function cleanup(ws) {
  const roomId = ws.roomId;
  if (!roomId) return;

  const room = rooms.get(roomId);
  if (!room) return;

  // Notify the other person
  room.forEach(peer => {
    if (peer !== ws && peer.readyState === WebSocket.OPEN) {
      peer.send(JSON.stringify({ type: 'peer-left' }));
    }
  });

  // Remove this person from the room
  const updated = room.filter(p => p !== ws);
  if (updated.length === 0) {
    rooms.delete(roomId);
    console.log('Room deleted:', roomId);
  } else {
    rooms.set(roomId, updated);
  }

  ws.roomId = null;
}

server.listen(PORT, () => {
  console.log(`PeerLink signalling server running on port ${PORT}`);
});

/*
  ─────────────────────────────────────────────────────
  PACKAGE.JSON — create this as a separate file too:
  ─────────────────────────────────────────────────────

  {
    "name": "peerlink-signalling",
    "version": "1.0.0",
    "main": "server.js",
    "scripts": {
      "start": "node server.js"
    },
    "dependencies": {
      "ws": "^8.16.0"
    }
  }
*/
