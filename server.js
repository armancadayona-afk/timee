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
const crypto = require('crypto');
const { elapsed, pause, resume } = require('./timer-state');

const app = express();
app.set('trust proxy', 1);
const server = http.createServer(app);
const io = socketIo(server, { cors: { origin: '*' } });

app.use(express.json());
app.use(session({
  secret: process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex'),
  resave: false,
  saveUninitialized: false,
  cookie: { httpOnly: true, sameSite: 'lax', secure: process.env.NODE_ENV === 'production', maxAge: 86400000 }
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

// Administrator authentication. Set ADMIN_USERNAME, ADMIN_PASSWORD and SESSION_SECRET on Render.
function requireAdmin(req, res, next) {
  if (req.session && req.session.admin === true) return next();
  return res.status(401).json({ error: 'Administrator sign-in required' });
}

app.post('/api/login', (req, res) => {
  const expectedUser = process.env.ADMIN_USERNAME || 'admin';
  const expectedPass = process.env.ADMIN_PASSWORD;
  if (!expectedPass) return res.status(503).json({ error: 'ADMIN_PASSWORD is not configured on the server' });
  const username = String(req.body?.username || '');
  const password = String(req.body?.password || '');
  const safeEqual = (a, b) => {
    const x = crypto.createHash('sha256').update(a).digest();
    const y = crypto.createHash('sha256').update(b).digest();
    return crypto.timingSafeEqual(x, y);
  };
  if (!safeEqual(username, expectedUser) || !safeEqual(password, expectedPass)) {
    return res.status(401).json({ error: 'Incorrect username or password' });
  }
  req.session.regenerate(err => {
    if (err) return res.status(500).json({ error: 'Could not start session' });
    req.session.admin = true;
    req.session.save(err => err
      ? res.status(500).json({ error: 'Could not save session' })
      : res.json({ success: true, role: 'admin', redirect: '/admin.html' }));
  });
});

app.get('/api/me', (req, res) => {
  if (req.session?.admin) return res.json({ authenticated: true, role: 'admin' });
  return res.status(401).json({ authenticated: false, error: 'Authentication required' });
});

app.post('/api/logout', (req, res) => {
  req.session.destroy(() => res.json({ success: true }));
});

// Device Registration Endpoints
app.post('/api/device-registration/request', (req, res) => {
  const { deviceId, registrationSecret, label } = req.body;
  if (!deviceId || typeof deviceId !== 'string' || deviceId.length > 128 || typeof registrationSecret !== 'string' || registrationSecret.length < 16) return res.status(400).json({ error: 'Device ID and registration secret (minimum 16 characters) required' });
  if (db.devices[deviceId]) return res.status(409).json({ error: 'Device already registered' });

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
  if (!dev || !req.body?.registrationSecret || req.body.registrationSecret !== dev.registrationSecret) return res.status(404).json({ error: 'Device not found or credentials invalid' });
  res.json({ deviceId: dev.deviceId, status: dev.status, workerName: dev.workerName || null });
});

app.post('/api/admin/devices/:deviceId/approve', requireAdmin, (req, res) => {
  const { deviceId } = req.params;
  const { workerName } = req.body;
  const dev = db.devices[deviceId];
  if (!dev) return res.status(404).json({ error: 'Device not found' });

  dev.status = 'approved';
  dev.workerName = workerName || 'Worker';
  dev.deviceToken = crypto.randomBytes(32).toString('hex');
  dev.approvedAt = Date.now();
  persistDb();
  io.emit('device-approved', { deviceId, status: dev.status });
  res.json({ success: true, device: { deviceId, status: dev.status, workerName: dev.workerName } });
});

app.post('/api/admin/devices/:deviceId/reject', requireAdmin, (req, res) => {
  const { deviceId } = req.params;
  const dev = db.devices[deviceId];
  if (dev) {
    dev.status = 'rejected';
    persistDb();
    io.emit('device-rejected', { deviceId });
  }
  res.json({ success: true });
});

app.post('/api/admin/devices/:deviceId/revoke', requireAdmin, (req, res) => {
  const { deviceId } = req.params;
  const dev = db.devices[deviceId];
  if (dev) {
    dev.status = 'revoked';
    persistDb();
    io.emit('device-revoked', { deviceId });
  }
  res.json({ success: true });
});

app.use(express.static(path.join(__dirname, 'public')));

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
