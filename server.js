/**
 * Kitty WorkTimer Backend Server
 * Dynamic Device ID Registration, Session Sync, WebRTC Signaling & Heartbeats
 */
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const fs = require('fs');
const path = require('path');
const session = require('express-session');
const { elapsed, pause, resume } = require('./timer-state');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, { cors: { origin: '*' } });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));
app.use(session({
  secret: process.env.SESSION_SECRET || 'kitty-secret-key-render',
  resave: false,
  saveUninitialized: false
}));

// In-memory or file-backed storage for devices, timers, tasks, messages
const DB_FILE = path.join(__dirname, 'kitty-database.json');
let db = { devices: {}, timers: {}, tasks: [], messages: [] };

try {
  if (fs.existsSync(DB_FILE)) {
    db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
  }
} catch (e) {
  console.error('Error loading database:', e);
}

function persistDb() {
  try {
    fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
  } catch (e) {
    console.error('Database save error:', e);
  }
}

// Device Registration Endpoints
app.post('/api/device-registration/request', (req, res) => {
  const { deviceId, registrationSecret, label } = req.body;
  if (!deviceId) return res.status(400).json({ error: 'Device ID required' });

  db.devices[deviceId] = {
    deviceId,
    registrationSecret,
    label: label || 'Android Device',
    status: 'pending',
    requestedAt: Date.now()
  };
  persistDb();
  io.emit('admin-device-requested', db.devices[deviceId]);
  res.json({ status: 'pending', deviceId });
});

app.post('/api/device-registration/status', (req, res) => {
  const { deviceId } = req.body;
  const dev = db.devices[deviceId];
  if (!dev) return res.status(404).json({ error: 'Device not found' });
  res.json(dev);
});

app.post('/api/admin/devices/:deviceId/approve', (req, res) => {
  const { deviceId } = req.params;
  const { workerName } = req.body;
  const dev = db.devices[deviceId];
  if (!dev) return res.status(404).json({ error: 'Device not found' });

  dev.status = 'approved';
  dev.workerName = workerName || 'Worker';
  dev.deviceToken = 'token_' + Math.random().toString(36).substring(2);
  dev.approvedAt = Date.now();
  persistDb();
  io.emit('device-approved', dev);
  res.json({ success: true, device: dev });
});

app.post('/api/admin/devices/:deviceId/reject', (req, res) => {
  const { deviceId } = req.params;
  const dev = db.devices[deviceId];
  if (dev) {
    dev.status = 'rejected';
    persistDb();
    io.emit('device-rejected', { deviceId });
  }
  res.json({ success: true });
});

app.post('/api/admin/devices/:deviceId/revoke', (req, res) => {
  const { deviceId } = req.params;
  const dev = db.devices[deviceId];
  if (dev) {
    dev.status = 'revoked';
    persistDb();
    io.emit('device-revoked', { deviceId });
  }
  res.json({ success: true });
});

// RTC Config
app.get('/api/rtc-config', (req, res) => {
  res.json({
    iceServers: [
      { urls: 'stun:stun.l.google.com:19302' },
      { urls: 'stun:stun1.l.google.com:19302' }
    ]
  });
});

// Socket IO real-time handling
io.on('connection', socket => {
  socket.on('worker-capture-ready', () => {
    socket.broadcast.emit('worker-online', { socketId: socket.id });
  });

  socket.on('worker-heartbeat', () => {
    socket.emit('worker-connection-status', {
      serverNow: Date.now(),
      connectedAt: socket.handshake.issued || Date.now()
    });
  });

  socket.on('rtc-offer', data => socket.broadcast.emit('rtc-offer', data));
  socket.on('rtc-answer', data => socket.broadcast.emit('rtc-answer', data));
  socket.on('rtc-ice', data => socket.broadcast.emit('rtc-ice', data));
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log('Kitty WorkTimer backend active on port ' + PORT);
});
