/**
 * CyberTheatron Signal Tower — Cloudflare Worker + Durable Object
 * ================================================================
 * Drop-in replacement for the Railway / Node.js signalling server.
 *
 * SAME message protocol as server.js — no changes needed to the
 * WebRTC or signalling logic in broadcaster.html, viewer.html,
 * admin.html, security.html.  Only the WebSocket URL changes.
 *
 * Supports all three protocols from the original server:
 *   1. CyberPhon 1-to-1 video calls
 *   2. Signal Tower 1-to-many broadcast (CyberTheatron.LGBT)
 *   3. Admin messaging + Security staff alerts
 *
 * Deploy:  npx wrangler deploy
 * Dev:     npx wrangler dev
 */

// ── Worker entry point ────────────────────────────────────────────────────────
// Routes every request to the single shared SignalTower Durable Object.

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Health check (HTTP GET /health)
    if (url.pathname === '/health') {
      const id   = env.SIGNAL_TOWER.idFromName('main');
      const stub = env.SIGNAL_TOWER.get(id);
      return stub.fetch(request);
    }

    // WebSocket upgrade → forward to the Durable Object
    const upgrade = request.headers.get('Upgrade') || '';
    if (upgrade.toLowerCase() === 'websocket') {
      const id   = env.SIGNAL_TOWER.idFromName('main');
      const stub = env.SIGNAL_TOWER.get(id);
      return stub.fetch(request);
    }

    // Default response for plain HTTP
    return new Response('CyberTheatron Signal Tower ✓\n', {
      headers: { 'Content-Type': 'text/plain' }
    });
  }
};

// ── SignalTower Durable Object ────────────────────────────────────────────────
// All WebSocket connections (broadcasters, viewers, admins, security) share
// this single DO instance so they can relay messages to each other.

export class SignalTower {
  constructor(state, env) {
    this.ctx = state;
    this.env = env;

    // ── CyberPhon 1-to-1 ─────────────────────────────────────────────────
    // roomId → { hostPeerId, guestPeerId }
    this.cpRooms = new Map();

    // ── Signal Tower 1-to-many ────────────────────────────────────────────
    // nodeId  → { hostPeerId, viewers: Map<peerId, handle> }
    this.stRooms = new Map();
    // peerId  → { nodeId, role }  (role: 'host'|'viewer'|'admin'|'security'|null)
    this.stPeers = new Map();
    // nodeId  → [{ peerId, handle }]  viewers waiting for host to reconnect
    this.waitingViewers = new Map();

    // ── Admin / security ──────────────────────────────────────────────────
    this.stAdmins    = new Set();  // Set of peerIds with admin role
    this.secSessions = new Set();  // Set of peerIds with security role
    // Current broadcast state (sent to new security sessions on connect)
    this.secCurrentAlert = null;   // { message } or null
    this.secCurrentMotd  = null;   // { message } or null

    // ── Persistent storage (survives DO hibernation) ──────────────────────
    // Lazy-loaded from ctx.storage on first message that needs them.
    // pendingMotd:          nodeId → [{ id, message, sentAt }]
    // pendingContacts:      contactId → contact object
    // pendingContactByNode: nodeId → contactId
    this._storageReady       = false;
    this.pendingMotd         = new Map();
    this.pendingContacts     = new Map();
    this.pendingContactByNode= new Map();

    this.BEACON_URL = (env && env.BEACON_URL)
      || 'https://cybertheatron-beacon.andreaoxygen.workers.dev';
  }

  // ── Internal helpers ───────────────────────────────────────────────────────

  _uid() { return crypto.randomUUID(); }

  /** Safe JSON send to a WebSocket */
  _send(ws, data) {
    try {
      if (ws && ws.readyState === WebSocket.OPEN)
        ws.send(JSON.stringify(data));
    } catch (_) {}
  }

  /**
   * Find the live WebSocket for a given peerId.
   * Searches ctx.getWebSockets() — works even after hibernation.
   */
  _wsFor(peerId) {
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment();
      if (att && att.peerId === peerId) return ws;
    }
    return null;
  }

  /**
   * Get (or rebuild) the in-memory peer entry for a peerId.
   * Falls back to reading the WS attachment if Maps were cleared by hibernation.
   */
  _peer(peerId) {
    if (this.stPeers.has(peerId)) return this.stPeers.get(peerId);
    const ws = this._wsFor(peerId);
    if (!ws) return null;
    const att = ws.deserializeAttachment();
    const peer = { nodeId: att.stNodeId || null, role: att.stRole || null };
    this.stPeers.set(peerId, peer);
    return peer;
  }

  /**
   * Get (or rebuild) the room for a nodeId.
   * After hibernation stRooms is empty — rebuild it from WS attachments.
   */
  _room(nodeId) {
    if (this.stRooms.has(nodeId)) return this.stRooms.get(nodeId);

    // First pass: find the host for this nodeId
    let room = null;
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment();
      if (att && att.stRole === 'host' && att.stNodeId === nodeId) {
        room = { hostPeerId: att.peerId, viewers: new Map() };
        this.stRooms.set(nodeId, room);
        break;
      }
    }
    // Second pass: add viewers to the rebuilt room
    if (room) {
      for (const ws of this.ctx.getWebSockets()) {
        const att = ws.deserializeAttachment();
        if (att && att.stRole === 'viewer' && att.stNodeId === nodeId) {
          room.viewers.set(att.peerId, att.stHandle || 'ANONYMOUS');
        }
      }
    }
    return room;
  }

  /**
   * Rebuild stAdmins / secSessions Sets from WS attachments.
   * Called before any broadcast to admins/security in case we woke from hibernation.
   */
  _rebuildRoleSets() {
    // Only rebuild if the sets look empty but connections exist
    if (this.stAdmins.size > 0 || this.secSessions.size > 0) return;
    for (const ws of this.ctx.getWebSockets()) {
      const att = ws.deserializeAttachment();
      if (!att) continue;
      if (att.stRole === 'admin')    this.stAdmins.add(att.peerId);
      if (att.stRole === 'security') this.secSessions.add(att.peerId);
    }
  }

  /** Lazy-load persistent queues from DO storage */
  async _ensureStorage() {
    if (this._storageReady) return;
    const [motd, contacts, contactByNode, alert, motdMsg, cpRoomsData] = await Promise.all([
      this.ctx.storage.get('pendingMotd'),
      this.ctx.storage.get('pendingContacts'),
      this.ctx.storage.get('pendingContactByNode'),
      this.ctx.storage.get('secCurrentAlert'),
      this.ctx.storage.get('secCurrentMotd'),
      this.ctx.storage.get('cpRooms'),
    ]);
    if (motd)          this.pendingMotd          = new Map(Object.entries(motd));
    if (contacts)      this.pendingContacts       = new Map(Object.entries(contacts));
    if (contactByNode) this.pendingContactByNode  = new Map(Object.entries(contactByNode));
    if (alert)         this.secCurrentAlert       = alert;
    if (motdMsg)       this.secCurrentMotd        = motdMsg;
    // Restore cpRooms — hostPeerId/guestPeerId refs won't be valid after hibernation
    // but the room structure is preserved so create/join logic works correctly
    if (cpRoomsData) {
      for (const [roomId, room] of Object.entries(cpRoomsData)) {
        this.cpRooms.set(roomId, {
          hostPeerId: room.hostPeerId || null,
          guestPeerId: room.guestPeerId || null,
          hostReconnecting: room.hostReconnecting || false,
          hostLeftAt: room.hostLeftAt || null,
        });
      }
    }
    this._storageReady = true;
  }

  /** Persist queues + security state to DO storage */
  async _saveCpRooms() {
    const obj = {};
    for (const [roomId, room] of this.cpRooms.entries()) {
      obj[roomId] = {
        hostPeerId: room.hostPeerId,
        guestPeerId: room.guestPeerId,
        hostReconnecting: room.hostReconnecting || false,
        hostLeftAt: room.hostLeftAt || null,
      };
    }
    await this.ctx.storage.put('cpRooms', obj);
  }

  async _saveStorage() {
    await Promise.all([
      this.ctx.storage.put('pendingMotd',          Object.fromEntries(this.pendingMotd)),
      this.ctx.storage.put('pendingContacts',       Object.fromEntries(this.pendingContacts)),
      this.ctx.storage.put('pendingContactByNode',  Object.fromEntries(this.pendingContactByNode)),
      this.ctx.storage.put('secCurrentAlert',       this.secCurrentAlert),
      this.ctx.storage.put('secCurrentMotd',        this.secCurrentMotd),
    ]);
  }

  // ── HTTP / WebSocket entry ─────────────────────────────────────────────────

  async fetch(request) {
    const url = new URL(request.url);

    // Health check — returns live stats
    if (url.pathname === '/health') {
      return Response.json({
        status          : 'ok',
        service         : 'CyberTheatron Signal Tower (Cloudflare DO)',
        activeWebSockets: this.ctx.getWebSockets().length,
        stRooms         : this.stRooms.size,
        cpRooms         : this.cpRooms.size,
        stAdmins        : this.stAdmins.size,
        secSessions     : this.secSessions.size,
      });
    }

    // Must be a WebSocket upgrade
    const upgrade = request.headers.get('Upgrade') || '';
    if (upgrade.toLowerCase() !== 'websocket') {
      return new Response('Expected WebSocket upgrade', { status: 426 });
    }

    // Create the WebSocket pair and accept it
    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    const peerId = this._uid();
    // Attach metadata to the server-side socket — survives DO hibernation
    server.serializeAttachment({
      peerId   : peerId,
      stRole   : null,
      stNodeId : null,
      stHandle : null,
      cpRoomId : null,
      cpRole   : null,
    });

    this.ctx.acceptWebSocket(server);
    this.stPeers.set(peerId, { nodeId: null, role: null });

    return new Response(null, { status: 101, webSocket: client });
  }

  // ── Message handler ────────────────────────────────────────────────────────

  async webSocketMessage(ws, rawMessage) {
    let msg;
    try { msg = JSON.parse(rawMessage); } catch { return; }

    const att = ws.deserializeAttachment();
    if (!att) return;
    const peerId = att.peerId;

    // Load persisted queues if not already done (first message after hibernation)
    await this._ensureStorage();

    // Determine routing type for offer/answer/ice-candidate messages
    const isSTRelay = ['offer','answer','ice-candidate'].includes(msg.type) && msg.to;
    const isCPRelay = ['offer','answer','ice-candidate','leave'].includes(msg.type) && !msg.to;

    // ════════════════════════════════════════════════════════════════════════
    // CYBERPHON 1-to-1 VIDEO CALLS
    // ════════════════════════════════════════════════════════════════════════

    if (msg.type === 'create') {
      const roomId = msg.room;
      if (!roomId) return;
      const existing = this.cpRooms.get(roomId);
      if (existing) {
        if (existing.hostReconnecting && !existing.hostPeerId) {
          // Host is reclaiming their room after disconnect — allow it
          existing.hostPeerId = peerId;
          existing.hostReconnecting = false;
          existing.hostLeftAt = null;
          ws.serializeAttachment({ ...att, cpRoomId: roomId, cpRole: 'host' });
          this._send(ws, { type: 'room-created', room: roomId });
          // If guest is still connected, notify both sides to reconnect
          if (existing.guestPeerId) {
            const gws = this._wsFor(existing.guestPeerId);
            if (gws) this._send(gws, { type: 'host-reconnected', room: roomId });
            this._send(ws, { type: 'peer-arrived', room: roomId });
          }
          return;
        }
        this._send(ws, { type: 'error', message: 'Room already exists. Try again.' });
        return;
      }
      this.cpRooms.set(roomId, { hostPeerId: peerId, guestPeerId: null });
      ws.serializeAttachment({ ...att, cpRoomId: roomId, cpRole: 'host' });
      this._send(ws, { type: 'room-created', room: roomId });
      await this._saveCpRooms();
      return;
    }

    if (msg.type === 'join' && msg.room) {
      const roomId = msg.room;
      const room   = this.cpRooms.get(roomId);
      if (!room) {
        this._send(ws, { type: 'error', message: 'Room not found. Check the code.' });
        return;
      }
      if (room.guestPeerId) {
        this._send(ws, { type: 'error', message: 'Room is full.' });
        return;
      }
      room.guestPeerId = peerId;
      ws.serializeAttachment({ ...att, cpRoomId: roomId, cpRole: 'guest' });
      this._send(ws, { type: 'guest-joined', room: roomId });
      const hostWs = this._wsFor(room.hostPeerId);
      if (hostWs) this._send(hostWs, { type: 'peer-arrived', room: roomId });
      await this._saveCpRooms();
      return;
    }

    if (isCPRelay) {
      const roomId = msg.room || att.cpRoomId;
      if (!roomId) return;
      const room = this.cpRooms.get(roomId);
      if (!room) return;
      const otherPeerId = att.cpRole === 'host' ? room.guestPeerId : room.hostPeerId;
      if (otherPeerId) {
        const otherWs = this._wsFor(otherPeerId);
        if (otherWs) this._send(otherWs, msg);
      }
      if (msg.type === 'leave') this._cpCleanup(peerId, att);
      return;
    }

    // ════════════════════════════════════════════════════════════════════════
    // SIGNAL TOWER 1-to-many BROADCAST
    // ════════════════════════════════════════════════════════════════════════

    if (msg.type === 'register-host') {
      const nodeId = String(msg.nodeId || '').toLowerCase().trim();
      if (!nodeId) return;

      // Notify viewers waiting from a previous session that host is back
      const waiting = this.waitingViewers.get(nodeId) || [];
      waiting.forEach(({ peerId: vpid }) => {
        const vws = this._wsFor(vpid);
        if (vws) this._send(vws, { type: 'host-reconnected' });
      });
      this.waitingViewers.delete(nodeId);

      // Also notify any viewers still in the old room (edge case)
      const oldRoom = this.stRooms.get(nodeId);
      if (oldRoom) {
        oldRoom.viewers.forEach((handle, vpid) => {
          const vws = this._wsFor(vpid);
          if (vws) this._send(vws, { type: 'host-reconnected' });
        });
      }

      this.stRooms.set(nodeId, { hostPeerId: peerId, viewers: new Map() });

      const peer = this._peer(peerId);
      if (peer) { peer.nodeId = nodeId; peer.role = 'host'; }
      ws.serializeAttachment({ ...att, stRole: 'host', stNodeId: nodeId });

      const contactPending = this.pendingContactByNode.has(nodeId);
      this._send(ws, { type: 'registered', peerId, nodeId, contactPending });

      // Deliver first queued operator message immediately
      const queued = this.pendingMotd.get(nodeId) || [];
      if (queued.length > 0) {
        this._send(ws, { type: 'operator-message', id: queued[0].id, message: queued[0].message, sentAt: queued[0].sentAt });
      }
      return;
    }

    if (msg.type === 'join-room') {
      const nodeId = String(msg.nodeId || '').toLowerCase().trim();
      const handle = String(msg.handle || 'ANONYMOUS').slice(0, 30).toUpperCase();
      if (!nodeId) return;
      const room = this._room(nodeId);
      if (!room) {
        this._send(ws, { type: 'error', code: 'NO_ROOM', message: 'Node is not currently live.' });
        return;
      }
      room.viewers.set(peerId, handle);
      const peer = this._peer(peerId);
      if (peer) { peer.nodeId = nodeId; peer.role = 'viewer'; }
      ws.serializeAttachment({ ...att, stRole: 'viewer', stNodeId: nodeId, stHandle: handle });
      this._send(ws, { type: 'joined', peerId, nodeId, hostPeerId: room.hostPeerId, handle });
      const hostWs = this._wsFor(room.hostPeerId);
      if (hostWs) this._send(hostWs, { type: 'viewer-joined', viewerId: peerId, handle, viewerCount: room.viewers.size });
      return;
    }

    // SDP offer / answer / ICE candidate relay (Signal Tower — msg.to is set)
    if (isSTRelay) {
      const targetWs = this._wsFor(msg.to);
      if (targetWs) this._send(targetWs, { ...msg, from: peerId });
      return;
    }

    if (msg.type === 'kick') {
      const peer = this._peer(peerId);
      if (!peer || peer.role !== 'host') return;
      const room = this._room(peer.nodeId);
      if (!room || room.hostPeerId !== peerId) return;
      if (room.viewers.has(msg.viewerId)) {
        const vws = this._wsFor(msg.viewerId);
        if (vws) this._send(vws, { type: 'kicked', reason: String(msg.reason || 'Removed by host') });
        room.viewers.delete(msg.viewerId);
        const vp = this._peer(msg.viewerId);
        if (vp) { vp.nodeId = null; vp.role = null; }
        this._send(ws, { type: 'viewer-count', count: room.viewers.size });
      }
      return;
    }

    if (msg.type === 'host-update') {
      const peer = this._peer(peerId);
      if (!peer || peer.role !== 'host') return;
      const room = this._room(peer.nodeId);
      if (!room) return;
      room.viewers.forEach((handle, vpid) => {
        const vws = this._wsFor(vpid);
        if (vws) this._send(vws, { type: 'host-update', data: msg.data });
      });
      return;
    }

    // ════════════════════════════════════════════════════════════════════════
    // ADMIN MESSAGING
    // ════════════════════════════════════════════════════════════════════════

    if (msg.type === 'register-admin') {
      const key = String(msg.key || '');
      if (!key) {
        this._send(ws, { type: 'admin-auth-failed', reason: 'No key provided' });
        return;
      }
      // Verify the key against the beacon worker
      try {
        const resp = await fetch(this.BEACON_URL + '/admin/nodes', {
          headers: { 'Authorization': 'Bearer ' + key }
        });
        if (!resp.ok) {
          this._send(ws, { type: 'admin-auth-failed', reason: 'Invalid key' });
          return;
        }
      } catch {
        this._send(ws, { type: 'admin-auth-failed', reason: 'Beacon unreachable' });
        return;
      }

      const peer = this._peer(peerId);
      if (peer) peer.role = 'admin';
      ws.serializeAttachment({ ...att, stRole: 'admin' });
      this.stAdmins.add(peerId);

      const contacts = Array.from(this.pendingContacts.values()).filter(c => !c.resolved);
      this._send(ws, {
        type            : 'admin-registered',
        pendingContacts : contacts,
        secAlert        : this.secCurrentAlert ? this.secCurrentAlert.message : null,
        secMotd         : this.secCurrentMotd  ? this.secCurrentMotd.message  : null,
      });
      return;
    }

    if (msg.type === 'admin-to-node') {
      const peer = this._peer(peerId);
      if (!peer || peer.role !== 'admin') return;
      const targetNodeId = String(msg.nodeId || '').toLowerCase().trim();
      const message      = String(msg.message || '').slice(0, 500);
      if (!targetNodeId || !message) return;
      const msgId  = msg.id || this._uid();
      const sentAt = new Date().toISOString();
      const room   = this._room(targetNodeId);

      if (room) {
        const hostWs = this._wsFor(room.hostPeerId);
        if (hostWs) {
          // Node is live — deliver directly
          this._send(hostWs, { type: 'operator-message', id: msgId, message, sentAt });
          this._send(ws, { type: 'admin-msg-status', nodeId: targetNodeId, status: 'live', id: msgId });
        } else {
          // Room exists but host WS not found — queue it
          if (!this.pendingMotd.has(targetNodeId)) this.pendingMotd.set(targetNodeId, []);
          this.pendingMotd.get(targetNodeId).push({ id: msgId, message, sentAt });
          await this._saveStorage();
          this._send(ws, { type: 'admin-msg-status', nodeId: targetNodeId, status: 'queued', id: msgId });
        }
      } else {
        // Node offline — queue
        if (!this.pendingMotd.has(targetNodeId)) this.pendingMotd.set(targetNodeId, []);
        this.pendingMotd.get(targetNodeId).push({ id: msgId, message, sentAt });
        await this._saveStorage();
        this._send(ws, { type: 'admin-msg-status', nodeId: targetNodeId, status: 'queued', id: msgId });
      }
      return;
    }

    if (msg.type === 'motd-read') {
      const peer = this._peer(peerId);
      if (!peer || peer.role !== 'host') return;
      const queue = this.pendingMotd.get(peer.nodeId);
      if (queue) {
        const idx = queue.findIndex(m => m.id === msg.id);
        if (idx !== -1) queue.splice(idx, 1);
        if (queue.length === 0) this.pendingMotd.delete(peer.nodeId);
        await this._saveStorage();
      }
      // Notify all connected admins
      this._rebuildRoleSets();
      this.stAdmins.forEach(adminPeerId => {
        const aws = this._wsFor(adminPeerId);
        if (aws) this._send(aws, { type: 'motd-read-receipt', nodeId: peer.nodeId, msgId: msg.id });
      });
      // Deliver next queued message if any
      const remaining = this.pendingMotd.get(peer.nodeId) || [];
      if (remaining.length > 0) {
        this._send(ws, { type: 'operator-message', id: remaining[0].id, message: remaining[0].message, sentAt: remaining[0].sentAt });
      }
      return;
    }

    if (msg.type === 'node-to-admin') {
      const peer = this._peer(peerId);
      if (!peer || peer.role !== 'host') return;
      if (this.pendingContactByNode.has(peer.nodeId)) {
        this._send(ws, { type: 'contact-status', pending: true });
        return;
      }
      const contactId = this._uid();
      const contact = {
        id       : contactId,
        nodeId   : peer.nodeId,
        callsign : String(msg.callsign || peer.nodeId).toUpperCase(),
        message  : String(msg.message  || '').slice(0, 500),
        email    : msg.email ? String(msg.email).slice(0, 120) : null,
        sentAt   : new Date().toISOString(),
        resolved : false,
      };
      this.pendingContacts.set(contactId, contact);
      this.pendingContactByNode.set(peer.nodeId, contactId);
      await this._saveStorage();
      this._rebuildRoleSets();
      this.stAdmins.forEach(adminPeerId => {
        const aws = this._wsFor(adminPeerId);
        if (aws) this._send(aws, { type: 'new-contact', contact });
      });
      this._send(ws, { type: 'contact-status', pending: true, id: contactId });
      return;
    }

    if (msg.type === 'resolve-contact') {
      const peer = this._peer(peerId);
      if (!peer || peer.role !== 'admin') return;
      const contact = this.pendingContacts.get(msg.contactId);
      if (!contact) return;
      contact.resolved = true;
      this.pendingContactByNode.delete(contact.nodeId);
      this.pendingContacts.delete(msg.contactId);
      await this._saveStorage();
      // Notify the node if connected
      const room = this._room(contact.nodeId);
      if (room) {
        const hostWs = this._wsFor(room.hostPeerId);
        if (hostWs) this._send(hostWs, { type: 'contact-status', pending: false });
      }
      // Notify all admins so other open admin tabs update
      this._rebuildRoleSets();
      this.stAdmins.forEach(adminPeerId => {
        const aws = this._wsFor(adminPeerId);
        if (aws) this._send(aws, { type: 'contact-resolved', contactId: msg.contactId });
      });
      return;
    }

    // ════════════════════════════════════════════════════════════════════════
    // SECURITY STAFF
    // ════════════════════════════════════════════════════════════════════════

    if (msg.type === 'register-security') {
      const peer = this._peer(peerId);
      if (peer) peer.role = 'security';
      ws.serializeAttachment({ ...att, stRole: 'security' });
      this.secSessions.add(peerId);
      this._send(ws, { type: 'security-registered' });
      if (this.secCurrentAlert) this._send(ws, { type: 'security-alert', message: this.secCurrentAlert.message });
      if (this.secCurrentMotd)  this._send(ws, { type: 'security-motd',  message: this.secCurrentMotd.message  });
      return;
    }

    if (msg.type === 'security-broadcast-alert') {
      const peer = this._peer(peerId);
      if (!peer || peer.role !== 'admin') return;
      const message = String(msg.message || '').slice(0, 300);
      if (!message) return;
      this.secCurrentAlert = { message };
      await this._saveStorage();
      this._rebuildRoleSets();
      let count = 0;
      this.secSessions.forEach(spid => {
        const sws = this._wsFor(spid);
        if (sws) { this._send(sws, { type: 'security-alert', message }); count++; }
      });
      this._send(ws, { type: 'security-alert-ack', status: 'ok', sessions: count });
      return;
    }

    if (msg.type === 'clear-security-alert') {
      const peer = this._peer(peerId);
      if (!peer || peer.role !== 'admin') return;
      this.secCurrentAlert = null;
      await this._saveStorage();
      this._rebuildRoleSets();
      this.secSessions.forEach(spid => {
        const sws = this._wsFor(spid);
        if (sws) this._send(sws, { type: 'clear-security-alert' });
      });
      return;
    }

    if (msg.type === 'security-broadcast-motd') {
      const peer = this._peer(peerId);
      if (!peer || peer.role !== 'admin') return;
      const message = String(msg.message || '').slice(0, 300);
      if (!message) return;
      this.secCurrentMotd = { message };
      await this._saveStorage();
      this._rebuildRoleSets();
      let count = 0;
      this.secSessions.forEach(spid => {
        const sws = this._wsFor(spid);
        if (sws) { this._send(sws, { type: 'security-motd', message }); count++; }
      });
      this._send(ws, { type: 'security-motd-ack', status: 'ok' });
      return;
    }

    if (msg.type === 'clear-security-motd') {
      const peer = this._peer(peerId);
      if (!peer || peer.role !== 'admin') return;
      this.secCurrentMotd = null;
      await this._saveStorage();
      return;
    }
  }

  // ── WebSocket close / error ────────────────────────────────────────────────

  webSocketClose(ws) {
    const att = ws.deserializeAttachment();
    if (!att) return;
    this._stCleanup(att);
    this._cpCleanup(att);
  }

  webSocketError(ws) {
    const att = ws.deserializeAttachment();
    if (!att) return;
    this._stCleanup(att);
    this._cpCleanup(att);
  }

  // ── Cleanup helpers ────────────────────────────────────────────────────────

  _stCleanup(att) {
    const peerId = att.peerId;
    // Use in-memory peer entry first, fall back to attachment
    const peer   = this.stPeers.get(peerId);
    const role   = peer?.role   || att.stRole;
    const nodeId = peer?.nodeId || att.stNodeId;

    if (role === 'host' && nodeId) {
      const room = this.stRooms.get(nodeId);
      if (room && room.hostPeerId === peerId) {
        // Park viewers in waiting list and tell them host left
        const waiting = [];
        room.viewers.forEach((handle, vpid) => {
          const vws = this._wsFor(vpid);
          if (vws) {
            this._send(vws, { type: 'host-left' });
            if (vws.readyState === WebSocket.OPEN) waiting.push({ peerId: vpid, handle });
          }
        });
        if (waiting.length > 0) this.waitingViewers.set(nodeId, waiting);
        this.stRooms.delete(nodeId);
      }
    } else if (role === 'viewer' && nodeId) {
      const room = this.stRooms.get(nodeId);
      if (room) {
        room.viewers.delete(peerId);
        const hostWs = this._wsFor(room.hostPeerId);
        if (hostWs) this._send(hostWs, { type: 'viewer-left', viewerId: peerId, viewerCount: room.viewers.size });
      }
      // Also remove from waiting list if they were in it
      const wl = this.waitingViewers.get(nodeId);
      if (wl) {
        const filtered = wl.filter(v => v.peerId !== peerId);
        if (filtered.length > 0) this.waitingViewers.set(nodeId, filtered);
        else this.waitingViewers.delete(nodeId);
      }
    } else if (role === 'admin') {
      this.stAdmins.delete(peerId);
    } else if (role === 'security') {
      this.secSessions.delete(peerId);
    }

    this.stPeers.delete(peerId);
  }

  _cpCleanup(att) {
    const peerId = att.peerId;
    const roomId = att.cpRoomId;
    if (!roomId) return;
    const room = this.cpRooms.get(roomId);
    if (!room) return;

    if (att.cpRole === 'host') {
      // Host left — notify guest if present but keep room alive for 30s
      // so host can reconnect and reclaim without guest needing to rejoin
      if (room.guestPeerId) {
        const gws = this._wsFor(room.guestPeerId);
        if (gws) this._send(gws, { type: 'peer-left' });
      }
      // Mark room as host-reconnecting instead of deleting immediately
      room.hostPeerId = null;
      room.hostReconnecting = true;
      room.hostLeftAt = Date.now();
      await this._saveCpRooms();
      // Delete after 30 second grace period
      setTimeout(async () => {
        const r = this.cpRooms.get(roomId);
        if (r && r.hostReconnecting && !r.hostPeerId) {
          this.cpRooms.delete(roomId);
          await this._saveCpRooms();
        }
      }, 30000);
    } else if (att.cpRole === 'guest') {
      // Guest left — notify host if still connected
      if (room.hostPeerId) {
        const hws = this._wsFor(room.hostPeerId);
        if (hws) this._send(hws, { type: 'peer-left' });
      }
      room.guestPeerId = null;
      this._saveCpRooms();
    }
  }
}
