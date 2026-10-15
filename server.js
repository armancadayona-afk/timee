require('dotenv').config();

const path = require('path');
const fs = require('fs');
const http = require('http');
const crypto = require('crypto');
const timerState = require('./timer-state');
const express = require('express');
const session = require('express-session');
const FileStore = require('session-file-store')(session);
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 2e6,
  pingTimeout: 20000,
  pingInterval: 25000
});

const PORT = Number(process.env.PORT || 3000);
const IS_PRODUCTION = process.env.NODE_ENV === 'production';

const DATA_DIR = path.join(__dirname, 'data');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
const SESSIONS_FILE = path.join(DATA_DIR, 'sessions.json');
const TASKS_FILE = path.join(DATA_DIR, 'tasks.json');
const CHAT_FILE = path.join(DATA_DIR, 'chat.json');
const USERS_FILE = path.join(DATA_DIR, 'users.json');
const DEVICE_REQUESTS_FILE = path.join(DATA_DIR, 'device_requests.json');
const HTTP_SESSION_DIR = path.join(DATA_DIR, 'http-sessions');

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(HTTP_SESSION_DIR, { recursive: true });
// Preserve existing records across deployments. No automatic data reset.

if (IS_PRODUCTION) app.set('trust proxy', 1);

app.disable('x-powered-by');
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('Permissions-Policy', 'camera=(self), microphone=(self), display-capture=(self)');
  res.setHeader('Cross-Origin-Opener-Policy', 'same-origin');
  next();
});

app.use(express.json({ limit: '2mb' }));
app.use(express.urlencoded({ extended: true }));

const SESSION_MAX_AGE_MS = 12 * 60 * 60 * 1000;

const sessionMiddleware = session({
  name: 'worker_timer_sid',
  secret: process.env.SESSION_SECRET || 'dev-only-change-this-secret',
  store: new FileStore({
    path: HTTP_SESSION_DIR,
    ttl: Math.floor(SESSION_MAX_AGE_MS / 1000),
    retries: 0,
    reapInterval: 60 * 60,
    logFn: () => {}
  }),
  rolling: true,
  resave: false,
  saveUninitialized: false,
  cookie: {
    httpOnly: true,
    sameSite: 'lax',
    secure: IS_PRODUCTION,
    maxAge: SESSION_MAX_AGE_MS
  }
});

app.use(sessionMiddleware);
io.engine.use(sessionMiddleware);

// Prefilled worker accounts, restored for simple selection without device registration.
const users = [
  { role: 'admin', username: process.env.ADMIN_USERNAME || 'admin', password: process.env.ADMIN_PASSWORD || 'admin123', name: process.env.ADMIN_NAME || 'Administrator' },
  { role: 'worker', id: 'worker1', username: process.env.WORKER1_USERNAME || 'john', password: process.env.WORKER1_PASSWORD || 'john123', name: process.env.WORKER1_NAME || 'Worker 1' },
  { role: 'worker', id: 'worker2', username: process.env.WORKER2_USERNAME || 'mark', password: process.env.WORKER2_PASSWORD || 'mark123', name: process.env.WORKER2_NAME || 'Worker 2' },
  { role: 'worker', id: 'worker3', username: process.env.WORKER3_USERNAME || 'anne', password: process.env.WORKER3_PASSWORD || 'anne123', name: process.env.WORKER3_NAME || 'Worker 3' }
];

const savedUsers = readJson(USERS_FILE, []);
savedUsers.forEach(u => {
  if (u.username && !users.some(x => x.username.toLowerCase() === u.username.toLowerCase())) {
    users.push(u);
  }
});

const LIVE_NONE = 'NONE';
const LIVE_CAMERA_PENDING = 'CAMERA_PENDING';
const LIVE_CAMERA = 'CAMERA';
const LIVE_SCREEN_PENDING = 'SCREEN_PENDING';
const LIVE_SCREEN = 'SCREEN';

function initialWorkerState(user) {
  return {
    id: user.id,
    name: user.name,
    online: false,
    captureReady: false,
    status: 'OFFLINE',
    startedAt: null,
    accumulatedMs: 0,
    socketId: null,
    mediaSocketId: null,
    sessionId: null,
    lastSeenAt: null,

    liveMode: LIVE_NONE,
    desiredLiveMode: LIVE_NONE,
    liveAdminSocketId: null,
    liveRequestedAt: null
  };
}

function readJson(file, fallback) {
  try {
    return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch (_) {
    return fallback;
  }
}

const persisted = readJson(STATE_FILE, {});
const workSessions = readJson(SESSIONS_FILE, []);
const states = new Map();

for (const worker of users.filter(user => user.role === 'worker')) {
  const state = initialWorkerState(worker);
  const old = persisted[worker.id];

  if (old) {
    if (Number.isFinite(Number(old.lastSeenAt))) {
      state.lastSeenAt = Number(old.lastSeenAt);
    }
    if (['WORKING', 'PAUSED', 'STOPPED', 'OFFLINE'].includes(old.status)) {
      state.status = old.status;
    }
    if (old.startedAt != null) {
      state.startedAt = Number(old.startedAt);
    }
    if (Number.isFinite(Number(old.accumulatedMs))) {
      state.accumulatedMs = Number(old.accumulatedMs);
    }
    if (old.sessionId) {
      state.sessionId = String(old.sessionId);
    }
    if (['NONE', 'CAMERA', 'SCREEN'].includes(old.desiredLiveMode)) {
      state.desiredLiveMode = old.desiredLiveMode;
    }
  }

  states.set(worker.id, state);
}

function writeJsonAtomic(file, value) {
  const temp = `${file}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(value, null, 2));
  fs.renameSync(temp, file);
}

function persistState() {
  const output = {};

  for (const [id, state] of states.entries()) {
    output[id] = {
      status: state.status,
      startedAt: state.startedAt,
      accumulatedMs: state.accumulatedMs,
      sessionId: state.sessionId,
      lastSeenAt: state.lastSeenAt,
      desiredLiveMode: state.desiredLiveMode || LIVE_NONE
    };
  }

  writeJsonAtomic(STATE_FILE, output);
}

const tasks = readJson(TASKS_FILE, []);
const chatMessages = readJson(CHAT_FILE, []);
const deviceRequests = readJson(DEVICE_REQUESTS_FILE, []);

function persistSessions() {
  writeJsonAtomic(SESSIONS_FILE, workSessions.slice(-1000));
}

function persistTasks() {
  writeJsonAtomic(TASKS_FILE, tasks);
}

function persistChat() {
  writeJsonAtomic(CHAT_FILE, chatMessages);
}

function persistUsers() {
  const dynamicUsers = users.filter(u => u.id && u.id.startsWith('worker_'));
  writeJsonAtomic(USERS_FILE, dynamicUsers);
}

function persistDeviceRequests() {
  writeJsonAtomic(DEVICE_REQUESTS_FILE, deviceRequests);
}

function safeWorkerState(state) {
  return {
    serverNow: Date.now(),
    id: state.id,
    name: state.name,
    online: state.online,
    captureReady: state.captureReady,
    status: state.status,
    startedAt: state.startedAt,
    accumulatedMs: state.accumulatedMs,
    lastSeenAt: state.lastSeenAt,
    liveMode: state.liveMode,
    desiredLiveMode: state.desiredLiveMode || LIVE_NONE,
    liveRequestedAt: state.liveRequestedAt
  };
}

function checkAutoReconnectLive(state) {
  if (!state || !state.captureReady || !(state.mediaSocketId || state.socketId)) return;
  if (!state.desiredLiveMode || state.desiredLiveMode === LIVE_NONE) return;
  if (state.liveMode !== LIVE_NONE) return;

  const adminSockets = io.sockets.adapter.rooms.get('admins');
  if (!adminSockets || adminSockets.size === 0) return;

  const targetAdminSocketId = state.liveAdminSocketId && io.sockets.sockets.get(state.liveAdminSocketId)
    ? state.liveAdminSocketId
    : [...adminSockets][0];

  const modeStr = state.desiredLiveMode === LIVE_SCREEN ? 'screen' : 'camera';
  state.liveMode = modeStr === 'camera' ? LIVE_CAMERA_PENDING : LIVE_SCREEN_PENDING;
  state.liveAdminSocketId = targetAdminSocketId;
  state.liveRequestedAt = Date.now();

  broadcastStates();

  io.to(state.mediaSocketId || state.socketId).emit('live-request', {
    adminSocketId: targetAdminSocketId,
    workerId: state.id,
    mode: modeStr
  });
}

function broadcastStates() {
  io.to('admins').emit(
    'workers-state',
    [...states.values()].map(safeWorkerState)
  );
}

function clearDisconnectTimer(state) {
  if (state?.disconnectTimer) clearTimeout(state.disconnectTimer);
  if (state) state.disconnectTimer = null;
}

function resetLiveState(state) {
  if (!state) return;
  state.liveMode = LIVE_NONE;
  state.liveAdminSocketId = null;
  state.liveRequestedAt = null;
}

function stopWorkerLive(state, reason = 'stopped', notifyWorker = true) {
  if (!state) return;

  const adminSocketId = state.liveAdminSocketId;
  const previousMode = state.liveMode;

  if (notifyWorker && state.socketId) {
    io.to(state.mediaSocketId || state.socketId).emit('stop-live', {
      adminSocketId,
      reason
    });
  }

  resetLiveState(state);

  if (adminSocketId) {
    io.to(adminSocketId).emit('live-status', {
      workerId: state.id,
      mode: LIVE_NONE,
      previousMode,
      reason
    });
  }
}

function startShift(state) {
  if (!state) return;
  if (state.sessionId) return;

  clearDisconnectTimer(state);

  state.status = 'WORKING';
  state.startedAt = Date.now();
  state.accumulatedMs = 0;
  state.sessionId = crypto.randomUUID();

  workSessions.push({
    id: state.sessionId,
    workerId: state.id,
    workerName: state.name,
    startedAt: state.startedAt,
    endedAt: null,
    durationMs: null,
    reason: null
  });

  persistState();
  persistSessions();
}

function finishShift(state, reason = 'stopped', nextStatus = 'STOPPED') {
  if (!state) return null;

  const activeSocketId = state.socketId;

  clearDisconnectTimer(state);
  stopWorkerLive(state, reason, true);

  const durationMs = timerState.elapsed(state);

  const record = workSessions.find(
    item => item.id === state.sessionId && item.endedAt === null
  );

  if (record) {
    record.endedAt = Date.now();
    record.durationMs = durationMs;
    record.reason = reason;
  }

  state.status = nextStatus;
  state.startedAt = null;
  state.accumulatedMs = 0;
  state.sessionId = null;
  state.online = false;
  state.socketId = null;

  persistState();
  persistSessions();

  return activeSocketId;
}

function secureEqual(a, b) {
  const left = crypto.createHash('sha256').update(String(a)).digest();
  const right = crypto.createHash('sha256').update(String(b)).digest();
  return crypto.timingSafeEqual(left, right);
}

function requireAuth(req, res, next) {
  if (!req.session.user) {
    return res.status(401).json({ ok: false, error: 'Not authenticated' });
  }
  next();
}

function requireRole(role) {
  return (req, res, next) => {
    if (!req.session.user || req.session.user.role !== role) {
      return res.status(403).json({ ok: false, error: 'Forbidden' });
    }
    next();
  };
}

const loginAttempts = new Map();

function loginRateLimit(req, res, next) {
  const key = req.ip || 'unknown';
  const now = Date.now();

  const entry = loginAttempts.get(key) || {
    count: 0,
    resetAt: now + 10 * 60 * 1000
  };

  if (now > entry.resetAt) {
    entry.count = 0;
    entry.resetAt = now + 10 * 60 * 1000;
  }

  if (entry.count >= 20) {
    return res.status(429).json({
      ok: false,
      error: 'Too many login attempts. Try again later.'
    });
  }

  req.loginAttempt = { key, entry };
  next();
}

// ==========================================
// WAKE-UP ROUTER LOGIC ("KUMUSTA" KEYWORD)
// ==========================================
function processWakeupKeyword(sender, text, channel) {
  if (!text) return false;
  const content = String(text).toLowerCase();

  if (content.includes('kumusta')) {
    console.log(`[WAKE-UP TRIGGERED] Channel: ${channel} | From: ${sender} | Msg: "${text}"`);
    io.emit('worker-wakeup', {
      channel,
      sender: String(sender || 'Unknown'),
      message: text,
      timestamp: Date.now()
    });
    return true;
  }
  return false;
}

// 1. Android SMS / Direct Gateway Webhook
app.post(['/api/webhook/sms-wakeup', '/api/webhook/android-sms'], (req, res) => {
  const payload = req.body.data || req.body;
  const sender = payload.from || payload.contact || payload.sender || 'SMS';
  const message = payload.content || payload.message || payload.body || '';

  const triggered = processWakeupKeyword(sender, message, 'SMS');
  return res.json({ ok: true, triggered });
});

// 2. Facebook Messenger Webhook Handlers
app.get('/api/webhook/messenger-wakeup', (req, res) => {
  const VERIFY_TOKEN = process.env.MESSENGER_VERIFY_TOKEN || 'kitty_verify_token';
  const mode = req.query['hub.mode'];
  const token = req.query['hub.verify_token'];
  const challenge = req.query['hub.challenge'];

  if (mode === 'subscribe' && token === VERIFY_TOKEN) {
    return res.status(200).send(challenge);
  }
  return res.sendStatus(403);
});

app.post('/api/webhook/messenger-wakeup', (req, res) => {
  const body = req.body;

  if (body.object === 'page') {
    (body.entry || []).forEach(entry => {
      const event = entry.messaging && entry.messaging[0];
      if (event && event.message && event.message.text) {
        processWakeupKeyword(event.sender.id, event.message.text, 'Messenger');
      }
    });
    return res.status(200).send('EVENT_RECEIVED');
  }
  return res.sendStatus(404);
});

// ==========================================
// CORE ROUTES & APIS
// ==========================================

app.get('/', (_req, res) => {
  const indexPath = path.join(__dirname, 'public', 'index.html');

  if (!fs.existsSync(indexPath)) {
    return res.status(500).send('Work Timer homepage is missing.');
  }

  res.sendFile(indexPath);
});

app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    service: 'worker-timer-monitor',
    version: '3.3.0-three-workers'
  });
});

app.post('/api/login', loginRateLimit, (req, res) => {
  const username = String(req.body.username || '').trim();
  const password = String(req.body.password || '');

  const user = users.find(
    candidate =>
      secureEqual(candidate.username, username) &&
      secureEqual(candidate.password, password)
  );

  if (!user) {
    req.loginAttempt.entry.count += 1;
    loginAttempts.set(
      req.loginAttempt.key,
      req.loginAttempt.entry
    );

    return res.status(401).json({
      ok: false,
      error: 'Invalid username or password'
    });
  }

  loginAttempts.delete(req.loginAttempt.key);

  req.session.user = {
    role: user.role,
    id: user.id || null,
    username: user.username,
    name: user.name
  };

  if (user.role === 'worker') {
    const state = states.get(user.id);
    const oldSocketId = state?.socketId;

    if (oldSocketId) {
      io.to(oldSocketId).emit('session-replaced');
      io.sockets.sockets.get(oldSocketId)?.disconnect(true);
      if (state) state.socketId = null;
    }

    broadcastStates();
  }

  res.json({
    ok: true,
    user: req.session.user,
    redirect: user.role === 'admin'
      ? '/admin.html'
      : '/worker.html'
  });
});

// --- Device / User Registration API ---
app.post('/api/register', (req, res) => {
  const { username, password, name, role } = req.body || {};
  const cleanUsername = String(username || '').trim().toLowerCase();
  const cleanPassword = String(password || '').trim();
  const cleanName = String(name || '').trim() || cleanUsername;
  const cleanRole = (role === 'admin') ? 'admin' : 'worker';

  if (!cleanUsername || !cleanPassword) {
    return res.status(400).json({ ok: false, error: 'Username and password are required' });
  }

  if (users.some(u => u.username.toLowerCase() === cleanUsername)) {
    return res.status(400).json({ ok: false, error: 'Username already registered' });
  }

  const newId = cleanRole === 'worker' ? 'worker_' + Date.now().toString(36) : null;
  const newUser = {
    role: cleanRole,
    id: newId,
    username: cleanUsername,
    password: cleanPassword,
    name: cleanName
  };

  users.push(newUser);
  persistUsers();

  if (cleanRole === 'worker') {
    states.set(newId, initialWorkerState(newUser));
    broadcastStates();
  }

  req.session.user = {
    role: newUser.role,
    id: newUser.id,
    username: newUser.username,
    name: newUser.name
  };

  res.json({
    ok: true,
    user: req.session.user,
    redirect: newUser.role === 'admin' ? '/admin.html' : '/worker.html'
  });
});

// --- Device Registration & Approval System (Section 4, 5, 6) ---

// POST /api/devices/register
app.post('/api/devices/register', (req, res) => {
  const { deviceId, deviceName, platform, appVersion, publicKey, workerName } = req.body || {};
  const cleanDeviceId = String(deviceId || '').trim();
  const cleanName = String(deviceName || workerName || 'Android Device').trim();

  if (!cleanDeviceId) {
    return res.status(400).json({ success: false, error: 'deviceId is required' });
  }

  let device = deviceRequests.find(d => d.deviceId === cleanDeviceId);
  if (!device) {
    device = {
      deviceId: cleanDeviceId,
      deviceName: cleanName,
      platform: platform || 'android',
      appVersion: appVersion || '1.0.0',
      publicKey: publicKey || '',
      status: 'pending',
      registeredAt: Date.now(),
      workerId: null,
      workerName: cleanName
    };
    deviceRequests.push(device);
  } else {
    device.deviceName = cleanName;
    device.publicKey = publicKey || device.publicKey;
    if (device.status === 'rejected') {
      device.status = 'pending';
    }
  }

  persistDeviceRequests();
  io.to('admins').emit('device:requested', device);

  res.json({
    success: true,
    deviceId: device.deviceId,
    status: device.status
  });
});

// GET /api/devices/status & /api/device/status/:deviceId
const handleDeviceStatus = (req, res) => {
  const deviceId = req.params.deviceId || req.headers['x-device-id'] || req.query.deviceId;
  if (!deviceId) {
    return res.status(400).json({ error: 'Device ID required' });
  }

  const device = deviceRequests.find(d => d.deviceId === deviceId);
  if (!device) {
    const existingUser = users.find(u => u.deviceId === deviceId);
    if (existingUser) {
      return res.json({
        deviceId,
        status: 'approved',
        workerId: existingUser.id,
        workerName: existingUser.name
      });
    }
    return res.json({
      deviceId,
      status: 'not_registered',
      workerId: null
    });
  }

  res.json({
    deviceId: device.deviceId,
    status: device.status,
    workerId: device.workerId || ('worker_' + device.deviceId.substring(0, 8)),
    workerName: device.workerName || device.deviceName
  });
};

app.get('/api/devices/status', handleDeviceStatus);
app.get('/api/devices/status/:deviceId', handleDeviceStatus);
app.get('/api/device/status/:deviceId', handleDeviceStatus);

// GET /api/admin/devices
app.get('/api/admin/devices', requireRole('admin'), (req, res) => {
  res.json({ success: true, devices: deviceRequests });
});

// POST /api/admin/devices/:deviceId/approve
app.post('/api/admin/devices/:deviceId/approve', requireRole('admin'), (req, res) => {
  const deviceId = req.params.deviceId;
  const { workerName } = req.body || {};
  const device = deviceRequests.find(d => d.deviceId === deviceId);

  if (!device) {
    return res.status(404).json({ success: false, error: 'Device not found' });
  }

  const assignedWorkerId = device.workerId || ('worker_' + device.deviceId.substring(0, 10));
  const assignedWorkerName = String(workerName || device.workerName || device.deviceName || 'Worker Device').trim();

  device.status = 'approved';
  device.workerId = assignedWorkerId;
  device.workerName = assignedWorkerName;
  device.approvedAt = Date.now();
  persistDeviceRequests();

  let user = users.find(u => u.deviceId === deviceId || u.id === assignedWorkerId);
  if (!user) {
    user = {
      role: 'worker',
      id: assignedWorkerId,
      username: 'dev_' + deviceId.substring(0, 8),
      password: 'dev_auth_token',
      name: assignedWorkerName,
      deviceId: deviceId
    };
    users.push(user);
    persistUsers();
  } else {
    user.name = assignedWorkerName;
  }

  if (!states.has(assignedWorkerId)) {
    states.set(assignedWorkerId, initialWorkerState(user));
  } else {
    states.get(assignedWorkerId).name = assignedWorkerName;
  }
  broadcastStates();

  io.emit('device:approved', { deviceId: device.deviceId, workerId: assignedWorkerId, workerName: assignedWorkerName });
  res.json({ success: true, device });
});

// POST /api/admin/devices/:deviceId/reject
app.post('/api/admin/devices/:deviceId/reject', requireRole('admin'), (req, res) => {
  const deviceId = req.params.deviceId;
  const device = deviceRequests.find(d => d.deviceId === deviceId);

  if (!device) {
    return res.status(404).json({ success: false, error: 'Device not found' });
  }

  device.status = 'rejected';
  persistDeviceRequests();

  io.emit('device:rejected', { deviceId: device.deviceId });
  res.json({ success: true, device });
});

// POST /api/admin/devices/:deviceId/revoke
app.post('/api/admin/devices/:deviceId/revoke', requireRole('admin'), (req, res) => {
  const deviceId = req.params.deviceId;
  const device = deviceRequests.find(d => d.deviceId === deviceId);

  if (!device) {
    return res.status(404).json({ success: false, error: 'Device not found' });
  }

  device.status = 'revoked';
  persistDeviceRequests();

  const user = users.find(u => u.deviceId === deviceId);
  if (user && user.id) {
    const state = states.get(user.id);
    if (state && state.socketId) {
      io.to(state.socketId).emit('device:revoked');
      io.sockets.sockets.get(state.socketId)?.disconnect(true);
      state.socketId = null;
      state.status = 'OFFLINE';
      state.online = false;
    }
  }

  io.emit('device:revoked', { deviceId: device.deviceId });
  broadcastStates();
  res.json({ success: true, device });
});

// PATCH /api/admin/devices/:deviceId
app.patch('/api/admin/devices/:deviceId', requireRole('admin'), (req, res) => {
  const deviceId = req.params.deviceId;
  const { workerName, deviceName } = req.body || {};
  const device = deviceRequests.find(d => d.deviceId === deviceId);

  if (!device) {
    return res.status(404).json({ success: false, error: 'Device not found' });
  }

  if (workerName) device.workerName = String(workerName).trim();
  if (deviceName) device.deviceName = String(deviceName).trim();
  persistDeviceRequests();

  res.json({ success: true, device });
});

app.post('/api/logout', requireAuth, (req, res) => {
  const user = req.session.user;

  if (user.role === 'worker' && user.id) {
    const state = states.get(user.id);
    if (state?.sessionId) {
      finishShift(state, 'logout', 'OFFLINE');
    } else if (state) {
      stopWorkerLive(state, 'logout', true);
      state.status = 'OFFLINE';
      state.online = false;
    }
    broadcastStates();
  }

  req.session.destroy(() => {
    res.json({ ok: true });
  });
});

app.get('/api/me', requireAuth, (req, res) => {
  res.json({
    ok: true,
    user: req.session.user
  });
});

app.get('/api/worker/state', requireRole('worker'), (req, res) => {
  const state = states.get(req.session.user.id);

  if (!state) {
    return res.status(404).json({
      ok: false,
      error: 'Unknown worker'
    });
  }

  res.json({
    ok: true,
    state: safeWorkerState(state)
  });
});

app.post('/api/worker/start', requireRole('worker'), (req, res) => {
  const state = states.get(req.session.user.id);

  if (!state) {
    return res.status(404).json({
      ok: false,
      error: 'Unknown worker'
    });
  }

  if (state.status === 'PAUSED') {
    timerState.resume(state);
    persistState();
    broadcastStates();
  } else if (!state.sessionId) {
    startShift(state);
    broadcastStates();
  }

  res.json({
    ok: true,
    state: safeWorkerState(state)
  });
});

for (const action of ['pause', 'resume']) {
  app.post(`/api/worker/${action}`, requireRole('worker'), (req, res) => {
    const state = states.get(req.session.user.id);
    if (!state) return res.status(404).json({ok:false, error:'Unknown worker'});
    timerState[action](state);
    persistState();
    broadcastStates();
    res.json({ok:true, state:safeWorkerState(state)});
  });
}

app.post('/api/worker/stop', requireRole('worker'), (req, res) => {
  const state = states.get(req.session.user.id);

  if (!state) {
    return res.status(404).json({
      ok: false,
      error: 'Unknown worker'
    });
  }

  if (state.sessionId) {
    const socketId = finishShift(state, 'worker-stop', 'STOPPED');

    if (socketId) {
      io.to(socketId).emit('timer-stopped', {
        reason: 'worker-stop'
      });
    }

    state.socketId = null;
    broadcastStates();
  } else {
    state.status = 'STOPPED';
    state.online = false;
    resetLiveState(state);
    broadcastStates();
  }

  res.json({
    ok: true,
    state: safeWorkerState(state)
  });
});

app.get('/api/workers', requireRole('admin'), (_req, res) => {
  res.json({
    ok: true,
    workers: [...states.values()].map(safeWorkerState)
  });
});

app.get('/api/work-sessions/:workerId', requireRole('admin'), (req, res) => {
  const workerId = req.params.workerId;

  if (!states.has(workerId)) {
    return res.status(404).json({
      ok: false,
      error: 'Unknown worker'
    });
  }

  const sessions = workSessions
    .filter(item => item.workerId === workerId)
    .slice(-50)
    .reverse();

  res.json({
    ok: true,
    sessions
  });
});

app.post('/api/admin/worker/timer', requireRole('admin'), (req, res) => {
  const { workerId, action } = req.body || {};
  const state = states.get(workerId);

  if (!state) {
    return res.status(404).json({
      ok: false,
      error: 'Unknown worker'
    });
  }

  if (action === 'start') {
    if (state.status === 'PAUSED') {
      timerState.resume(state);
    } else if (!state.sessionId) {
      startShift(state);
    }
  } else if (action === 'pause') {
    if (state.status === 'WORKING') {
      timerState.pause(state);
    }
  } else if (action === 'resume') {
    if (state.status === 'PAUSED') {
      timerState.resume(state);
    }
  } else if (action === 'stop') {
    if (state.sessionId) {
      const socketId = finishShift(state, 'admin-stop', 'STOPPED');
      if (socketId) {
        io.to(socketId).emit('timer-stopped', {
          reason: 'admin-stop'
        });
      }
    } else {
      state.status = 'STOPPED';
      state.online = false;
      resetLiveState(state);
    }
  }

  persistState();
  broadcastStates();

  if (state.socketId) {
    io.to(state.socketId).emit('timer-updated', safeWorkerState(state));
  }

  res.json({
    ok: true,
    state: safeWorkerState(state)
  });
});

app.post('/api/admin/workers/:workerId/reset-time', requireRole('admin'), (req, res) => {
  const workerId = req.params.workerId;
  const state = states.get(workerId);

  if (!state) {
    return res.status(404).json({
      ok: false,
      error: 'Unknown worker'
    });
  }

  const previousAccumulatedMs = timerState.elapsed(state);

  if (state.status === 'WORKING') {
    state.accumulatedMs = 0;
    state.startedAt = Date.now();
  } else {
    state.accumulatedMs = 0;
    state.startedAt = null;
  }

  persistState();
  broadcastStates();

  if (state.socketId) {
    io.to(state.socketId).emit('timer-updated', safeWorkerState(state));
  }

  res.json({
    ok: true,
    previousAccumulatedMs,
    state: safeWorkerState(state)
  });
});

app.get('/api/rtc-config', requireAuth, (_req, res) => {
  let iceServers = [
    {
      urls: 'stun:stun.l.google.com:19302'
    }
  ];

  if (process.env.RTC_ICE_SERVERS_JSON) {
    try {
      const parsed = JSON.parse(
        process.env.RTC_ICE_SERVERS_JSON
      );

      if (Array.isArray(parsed) && parsed.length) {
        iceServers = parsed;
      }
    } catch (_) {
    }
  }

  res.json({
    ok: true,
    iceServers
  });
});

// Task and earnings API: server-authoritative records, no hourly-rate calculation.
const LEDGER_FILE = path.join(DATA_DIR, 'earnings.json');
const earningsLedger = readJson(LEDGER_FILE, []);
function saveLedger(){writeJsonAtomic(LEDGER_FILE,earningsLedger);}
function money(x){const v=Number(x);return Number.isFinite(v)&&v>=0?Math.round(v*100)/100:null;}
function taskIdOf(t){return t.taskId||t.id;}
function notifyTask(t,event='task:updated'){
  io.to('admins').emit(event,t);
  if(t.workerId)io.to(`worker:${t.workerId}`).emit(event,t);
  io.to('admins').emit('earnings:updated',{workerId:t.workerId});
  if(t.workerId)io.to(`worker:${t.workerId}`).emit('earnings:updated',{});
}
function visibleTasks(user){
  return user.role==='admin'?tasks:tasks.filter(t=>t.workerId===user.id || (t.assignmentType==='OPEN'&&t.status==='AVAILABLE'));
}
function publicTask(t){return t;}
app.get('/api/tasks',requireAuth,(req,res)=>{
 let list=visibleTasks(req.session.user);
 if(req.session.user.role==='admin'&&req.query.workerId)list=list.filter(t=>t.workerId===req.query.workerId);
 res.json({ok:true,tasks:list.map(publicTask)});
});
app.get('/api/worker/tasks',requireRole('worker'),(req,res)=>res.json({ok:true,tasks:visibleTasks(req.session.user)}));
app.post('/api/tasks',requireRole('admin'),(req,res)=>{
 const {workerId,title,description,dueAt,assignmentType,allowConcurrent}=req.body||{};
 const type=assignmentType==='OPEN'||workerId==='OPEN'?'OPEN':'ASSIGNED';
 if(!String(title||'').trim()||(type==='ASSIGNED'&&!states.has(workerId)))return res.status(400).json({ok:false,error:'Valid title and worker required'});
 const price=money(req.body.price??0);
 if(price===null)return res.status(400).json({ok:false,error:'Invalid task price'});
 const id=crypto.randomUUID();
 const task={id,taskId:id,title:String(title).trim().slice(0,200),description:String(description||'').slice(0,3000),price,
   assignmentType:type,workerId:type==='OPEN'?null:workerId,allowConcurrent:allowConcurrent===true,
   status:type==='OPEN'?'AVAILABLE':'ASSIGNED',createdAt:Date.now(),assignedAt:type==='ASSIGNED'?Date.now():null,dueAt:dueAt||null,
   acceptedAt:null,submittedAt:null,completedAt:null,recordedMs:null,approvedAmount:null,adminNote:''};
 tasks.push(task);persistTasks();notifyTask(task,'task:assigned');io.to('workers').emit('task:updated',task);
 res.json({ok:true,task});
});
// Admin-recorded completed tasks: immediately approved, with an auditable earnings entry.
app.post('/api/admin/tasks/completed',requireRole('admin'),(req,res)=>{
 const {workerId,title,description}=req.body||{};
 const price=money(req.body.amount);
 const hours=Number(req.body.hours??0);
 if(!states.has(workerId)||!String(title||'').trim()||price===null||!Number.isFinite(hours)||hours<0||hours>100000)
   return res.status(400).json({ok:false,error:'Worker, task title, nonnegative payment and valid hours required'});
 const now=Date.now(),id=crypto.randomUUID();
 const task={id,taskId:id,title:String(title).trim().slice(0,200),description:String(description||'').slice(0,3000),price,
 assignmentType:'ASSIGNED',workerId,allowConcurrent:false,status:'APPROVED',createdAt:now,assignedAt:now,
 acceptedAt:now,submittedAt:now,completedAt:now,recordedMs:Math.round(hours*3600000),approvedAmount:price,
 adminNote:'Recorded as completed by admin',recordedByAdmin:true};
 const entry={id:crypto.randomUUID(),workerId,taskId:id,type:'TASK_APPROVED',amount:price,createdAt:now,reason:'Admin-recorded completed task',admin:req.session.user.username};
 try {tasks.push(task);persistTasks();earningsLedger.push(entry);saveLedger();}
 catch(e){tasks.splice(tasks.indexOf(task),1);earningsLedger.splice(earningsLedger.indexOf(entry),1);return res.status(500).json({ok:false,error:'Could not save completed task'});}
 notifyTask(task,'task:approved');res.json({ok:true,task,entry});
});
// Clear historical task logs for one worker or all workers; preserve active tasks and manual adjustments.
app.post('/api/admin/tasks/reset-logs',requireRole('admin'),(req,res)=>{
 const workerId=String(req.body?.workerId||'');
 if(workerId && !states.has(workerId))return res.status(400).json({ok:false,error:'Unknown worker'});
 if(req.body?.confirmation!=='RESET TASK LOGS')return res.status(400).json({ok:false,error:'Confirmation required'});
 const archived=new Set(['APPROVED','REJECTED']);
 const deleted=tasks.filter(t=>archived.has(t.status)&&(!workerId||t.workerId===workerId));
 const deletedIds=new Set(deleted.map(taskIdOf));
 const keptTasks=tasks.filter(t=>!deletedIds.has(taskIdOf(t)));
 const keptLedger=earningsLedger.filter(e=>!(e.taskId&&deletedIds.has(e.taskId)));
 const oldTasks=tasks.slice(),oldLedger=earningsLedger.slice();
 try {
   tasks.splice(0,tasks.length,...keptTasks);earningsLedger.splice(0,earningsLedger.length,...keptLedger);
   persistTasks();saveLedger();
 }catch(e){
   tasks.splice(0,tasks.length,...oldTasks);earningsLedger.splice(0,earningsLedger.length,...oldLedger);
   try{persistTasks();saveLedger();}catch(_){}
   return res.status(500).json({ok:false,error:'Unable to clear task logs'});
 }
 io.to('admins').emit('tasks:reset',{workerId:workerId||null,count:deleted.length});
 io.to('workers').emit('tasks:reset',{workerId:workerId||null,count:deleted.length});
 io.emit('earnings:updated',{workerId:workerId||null});
 res.json({ok:true,deletedTasks:deleted.length,deletedEarningsEntries:oldLedger.length-keptLedger.length});
});
app.post('/api/tasks/:taskId/accept',requireRole('worker'),(req,res)=>{
 const t=tasks.find(x=>taskIdOf(x)===req.params.taskId);const wid=req.session.user.id;
 if(!t)return res.status(404).json({ok:false,error:'Task not found'});
 if(t.status!=='AVAILABLE'&&t.status!=='ASSIGNED')return res.status(409).json({ok:false,error:'Task already claimed or submitted'});
 if(t.assignmentType!=='OPEN'&&t.workerId!==wid)return res.status(403).json({ok:false,error:'Not assigned to this worker'});
 if(!t.allowConcurrent&&tasks.some(x=>x.workerId===wid&&x.status==='IN_PROGRESS'&&!x.allowConcurrent))
   return res.status(409).json({ok:false,error:'Submit your current regular task first'});
 // Lock synchronously before any asynchronous operations: prevents double claim.
 t.workerId=wid;t.status='IN_PROGRESS';t.acceptedAt=Date.now();t.assignedAt??=Date.now();persistTasks();notifyTask(t);io.to('workers').emit('task:updated',t);
 res.json({ok:true,task:t});
});
app.post('/api/tasks/:taskId/submit',requireRole('worker'),(req,res)=>{
 const t=tasks.find(x=>taskIdOf(x)===req.params.taskId);const wid=req.session.user.id;
 if(!t)return res.status(404).json({ok:false,error:'Task not found'});
 if(t.workerId!==wid)return res.status(403).json({ok:false,error:'Not your task'});
 if(t.status!=='IN_PROGRESS')return res.status(409).json({ok:false,error:'Task is not in progress; submission already recorded or task not accepted'});
 const state=states.get(wid);
 if(!state)return res.status(404).json({ok:false,error:'Worker timer unavailable'});
 const duration=timerState.elapsed(state);
 if(duration<0||!Number.isFinite(duration))return res.status(422).json({ok:false,error:'Invalid timer duration'});
 // Capture time first, persist submission before resetting timer.
 const now=Date.now();const record={taskId:taskIdOf(t),workerId:wid,recordedMs:duration,submittedAt:now,status:'PENDING_APPROVAL'};
 t.recordedMs=duration;t.submittedAt=now;t.status='PENDING_APPROVAL';t.submissionId=crypto.randomUUID();
 try{persistTasks();}catch(e){Object.assign(t,{status:'IN_PROGRESS',recordedMs:null,submittedAt:null,submissionId:null});return res.status(500).json({ok:false,error:'Unable to save submission; timer preserved'});}
 // New timer segment begins at zero; previous clock history is part of the task record.
 state.accumulatedMs=0;
 if(state.status==='WORKING')state.startedAt=Date.now();
 else state.startedAt=null;
 persistState();broadcastStates();notifyTask(t,'task:submitted');
 res.json({ok:true,task:t,submission:record,state:safeWorkerState(state)});
});
app.post('/api/tasks/:taskId/approve',requireRole('admin'),(req,res)=>{
 const t=tasks.find(x=>taskIdOf(x)===req.params.taskId);
 if(!t)return res.status(404).json({ok:false,error:'Task not found'});
 if(t.status!=='PENDING_APPROVAL'&&t.status!=='SUBMITTED')return res.status(409).json({ok:false,error:'Task is not pending approval'});
 const payment=money(req.body.amount??t.price??0);
 if(payment===null)return res.status(400).json({ok:false,error:'Invalid payment amount'});
 if(earningsLedger.some(x=>x.taskId===taskIdOf(t)&&x.type==='TASK_APPROVED'))return res.status(409).json({ok:false,error:'Already credited'});
 const entry={id:crypto.randomUUID(),workerId:t.workerId,taskId:taskIdOf(t),type:'TASK_APPROVED',amount:payment,createdAt:Date.now(),reason:String(req.body.note||'Task approved').slice(0,500),admin:req.session.user.username};
 earningsLedger.push(entry);saveLedger();t.status='APPROVED';t.approvedAmount=payment;t.completedAt=Date.now();t.adminNote=entry.reason;persistTasks();notifyTask(t,'task:approved');res.json({ok:true,task:t,entry});
});
app.post('/api/tasks/:taskId/return',requireRole('admin'),(req,res)=>{
 const t=tasks.find(x=>taskIdOf(x)===req.params.taskId);
 if(!t)return res.status(404).json({ok:false,error:'Task not found'});
 if(t.status!=='PENDING_APPROVAL'&&t.status!=='SUBMITTED')return res.status(409).json({ok:false,error:'Task is not pending approval'});
 t.status='REJECTED';t.adminNote=String(req.body.adminNote||'').slice(0,1000);persistTasks();notifyTask(t,'task:returned');res.json({ok:true,task:t});
});
app.post('/api/admin/earnings/adjust',requireRole('admin'),(req,res)=>{
 const {workerId,amount,reason}=req.body||{};const val=Number(amount);
 if(!states.has(workerId)||!Number.isFinite(val)||val===0||!String(reason||'').trim())return res.status(400).json({ok:false,error:'Worker, nonzero amount and reason are required'});
 const entry={id:crypto.randomUUID(),workerId,type:'ADJUSTMENT',amount:Math.round(val*100)/100,reason:String(reason).slice(0,500),createdAt:Date.now(),admin:req.session.user.username};
 earningsLedger.push(entry);saveLedger();io.to('admins').to(`worker:${workerId}`).emit('earnings:updated',{workerId});res.json({ok:true,entry});
});
app.get('/api/earnings',requireAuth,(req,res)=>{
 const me=req.session.user;
 const workerId=me.role==='admin'?(req.query.workerId||null):me.id;
 const records=tasks.filter(t=>!workerId||t.workerId===workerId);
 const ledger=earningsLedger.filter(e=>!workerId||e.workerId===workerId);
 const approved=ledger.reduce((total,e)=>total+e.amount,0);
 const pending=records.filter(t=>t.status==='PENDING_APPROVAL').reduce((total,t)=>total+Number(t.price||0),0);
 res.json({ok:true,workerId,records,transactions:ledger,summary:{submitted:records.filter(t=>t.submittedAt).length,approvedTasks:records.filter(t=>t.status==='APPROVED').length,recordedMs:records.reduce((sum,t)=>sum+(t.recordedMs||0),0),approvedEarnings:Math.round(approved*100)/100,pendingEarnings:Math.round(pending*100)/100}});
});

// Chat API
app.get('/api/chat/:workerId', requireAuth, (req, res) => {
  const workerId = req.params.workerId;
  const user = req.session.user;
  if (user.role === 'worker' && user.id !== workerId) {
    return res.status(403).json({ ok: false, error: 'Forbidden' });
  }

  const messages = chatMessages.filter(m => m.workerId === workerId);
  res.json({ ok: true, messages });
});

app.post('/api/chat/:workerId/messages', requireAuth, (req, res) => {
  const workerId = req.params.workerId;
  const user = req.session.user;
  if (user.role === 'worker' && user.id !== workerId) {
    return res.status(403).json({ ok: false, error: 'Forbidden' });
  }

  const msgText = String(req.body.message || '').trim();
  if (!msgText) return res.status(400).json({ ok: false, error: 'Message cannot be empty' });

  const newMsg = {
    messageId: 'msg_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7),
    workerId,
    senderId: user.id || 'admin',
    senderRole: user.role.toUpperCase(),
    message: msgText,
    createdAt: Date.now(),
    readAt: null
  };

  chatMessages.push(newMsg);
  persistChat();

  io.to(`worker:${workerId}`).to('admins').emit('chat:message', newMsg);

  res.json({ ok: true, message: newMsg });
});

app.post('/api/chat/:workerId/read', requireAuth, (req, res) => {
  const workerId = req.params.workerId;
  const user = req.session.user;
  if (user.role === 'worker' && user.id !== workerId) {
    return res.status(403).json({ ok: false, error: 'Forbidden' });
  }

  let count = 0;
  chatMessages.forEach(m => {
    if (m.workerId === workerId && !m.readAt && m.senderRole !== user.role.toUpperCase()) {
      m.readAt = Date.now();
      count++;
    }
  });
  if (count > 0) persistChat();

  io.to(`worker:${workerId}`).to('admins').emit('chat:read', { workerId });
  res.json({ ok: true, count });
});

app.use(
  express.static(path.join(__dirname, 'public'), {
    extensions: ['html'],
    maxAge: 0,
    etag: true,
    setHeaders(res, filePath) {
      if (filePath.endsWith('.html')) {
        res.setHeader('Cache-Control', 'no-store');
      }
    }
  })
);

// Admin-requested device diagnostics. The browser cannot run ADB; this is a
// request/approval/result transport for a future explicitly authorized native bridge.
const terminalAllowed = new Set(['device-info', 'battery-status', 'network-status', 'app-status']);
const terminalPending = new Map();
const terminalAudit = [];
function terminalAuditRecord(record) {
  terminalAudit.push({ at: new Date().toISOString(), ...record });
  if (terminalAudit.length > 200) terminalAudit.shift();
}

io.on('connection', socket => {
  const user = socket.request.session?.user;

  if (!user) {
    return socket.disconnect(true);
  }

  if (user.role === 'admin') {
    socket.join('admins');

    socket.emit(
      'workers-state',
      [...states.values()].map(safeWorkerState)
    );
  } else if (user.role === 'worker' && user.id && socket.handshake?.query?.nativeMedia === '1') {
    const state = states.get(user.id);
    if (!state) return socket.disconnect(true);
    if (state.mediaSocketId && state.mediaSocketId !== socket.id) { const previous = state.mediaSocketId; state.mediaSocketId = null; io.sockets.sockets.get(previous)?.disconnect(true); }
    state.mediaSocketId = socket.id;
    state.captureReady = false;
    socket.emit('native-media-connected', { workerId:user.id });
    broadcastStates();
  } else if (user.role === 'worker' && user.id) {
    socket.join(`worker:${user.id}`);
    socket.join('workers');
    const state = states.get(user.id);

    if (!state) {
      socket.emit('timer-stopped', {
        reason: 'Timer is not running.'
      });
      return socket.disconnect(true);
    }

    if (state.socketId && state.socketId !== socket.id) {
      io.to(state.socketId).emit('session-replaced');
      io.sockets.sockets.get(state.socketId)?.disconnect(true);
    }

    clearDisconnectTimer(state);
    state.online = true;
    state.captureReady = Boolean(state.mediaSocketId);
    state.socketId = socket.id;
    state.lastSeenAt = Date.now();
    persistState();
    broadcastStates();
  }

  socket.on('terminal:request', (payload = {}, ack) => {
    if (user.role !== 'admin') return ack?.({ ok: false, error: 'Admin only' });
    const workerId = String(payload.workerId || '');
    const command = String(payload.command || '');
    if (!terminalAllowed.has(command)) return ack?.({ ok: false, error: 'Only predefined diagnostic commands are permitted' });
    const state = states.get(workerId);
    if (!state?.socketId || !io.sockets.sockets.has(state.socketId)) return ack?.({ ok: false, error: 'Worker is offline' });
    const id = crypto.randomUUID();
    terminalPending.set(id, { adminSocketId: socket.id, workerSocketId: state.socketId, workerId, command, expires: Date.now() + 60000 });
    terminalAuditRecord({ id, workerId, command, status: 'requested' });
    io.to(state.socketId).emit('terminal:request', { id, command, expiresAt: Date.now() + 60000 });
    ack?.({ ok: true, id });
    setTimeout(() => {
      const request = terminalPending.get(id);
      if (!request) return;
      terminalPending.delete(id);
      terminalAuditRecord({ id, workerId, command, status: 'timeout' });
      io.to(request.adminSocketId).emit('terminal:result', { id, workerId, command, ok: false, output: 'Request expired without worker approval.' });
    }, 60000).unref?.();
  });

  socket.on('terminal:received', (payload = {}) => {
    if (user.role !== 'worker') return;
    const request = terminalPending.get(String(payload.id || ''));
    if (!request || request.workerId !== user.id || request.workerSocketId !== socket.id) return;
    io.to(request.adminSocketId).emit('terminal:received', { id: String(payload.id), workerId: user.id, command: request.command });
  });

  socket.on('terminal:result', (payload = {}) => {
    if (user.role !== 'worker') return;
    const id = String(payload.id || '');
    const request = terminalPending.get(id);
    if (!request || request.workerId !== user.id || request.workerSocketId !== socket.id || Date.now() > request.expires) return;
    terminalPending.delete(id);
    const output = String(payload.output || '').slice(0, 4000);
    const ok = payload.ok === true;
    terminalAuditRecord({ id, workerId: user.id, command: request.command, status: ok ? 'completed' : 'denied-or-error', output });
    io.to(request.adminSocketId).emit('terminal:result', { id, workerId: user.id, command: request.command, ok, output });
  });

  socket.on('worker-capture-ready', () => {
    if (user.role !== 'worker') return;
    const state = states.get(user.id);
    if (!state || (state.socketId !== socket.id && state.mediaSocketId !== socket.id)) return;
    if (state.mediaSocketId && socket.id !== state.mediaSocketId) return;
    state.captureReady = true;
    broadcastStates();
    checkAutoReconnectLive(state);
  });

  socket.on('worker-heartbeat', () => {
    if (user.role !== 'worker' || !user.id) return;

    const state = states.get(user.id);

    if (
      !state ||
      state.socketId !== socket.id
    ) {
      return;
    }

    state.online = true;
    state.lastSeenAt = Date.now();
    persistState();
    broadcastStates();
    checkAutoReconnectLive(state);
  });

  socket.on('chat:message', ({ workerId, message }) => {
    const msgText = String(message || '').trim();
    if (!msgText) return;

    let targetWorkerId = workerId;
    if (user.role === 'worker') {
      targetWorkerId = user.id;
    }

    if (!targetWorkerId) return;

    const newMsg = {
      messageId: 'msg_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7),
      workerId: targetWorkerId,
      senderId: user.id || 'admin',
      senderRole: user.role.toUpperCase(),
      message: msgText,
      createdAt: Date.now(),
      readAt: null
    };

    chatMessages.push(newMsg);
    persistChat();

    io.to(`worker:${targetWorkerId}`).to('admins').emit('chat:message', newMsg);
  });

  socket.on('admin-ping-worker', ({ workerId, nonce } = {}) => {
    if (user.role !== 'admin') return;

    const state = states.get(workerId);

    if (
      !state?.socketId ||
      !io.sockets.sockets.get(state.socketId)
    ) {
      socket.emit('worker-ping-result', {
        workerId,
        nonce,
        ok: false,
        message: 'No active Worker app connection.'
      });
      return;
    }

    io.to(state.socketId).emit('worker-ping', {
      adminSocketId: socket.id,
      workerId,
      nonce,
      sentAt: Date.now()
    });
  });

  socket.on(
    'worker-pong',
    ({
      adminSocketId,
      workerId,
      nonce,
      workerReceivedAt
    } = {}) => {
      if (
        user.role !== 'worker' ||
        workerId !== user.id ||
        !adminSocketId ||
        !nonce
      ) {
        return;
      }

      const state = states.get(workerId);

      if (state && state.socketId === socket.id) {
        state.online = true;
        state.lastSeenAt = Date.now();
        persistState();
        broadcastStates();
      }

      io.to(adminSocketId).emit('worker-ping-result', {
        workerId,
        nonce,
        ok: true,
        workerReceivedAt,
        serverReceivedAt: Date.now()
      });
    }
  );

  socket.on(
    'admin-live-request',
    ({ workerId, mode } = {}) => {
      if (user.role !== 'admin') return;

      const state = states.get(workerId);
      const normalizedMode =
        String(mode || '').toLowerCase();

      if (!['camera', 'screen'].includes(normalizedMode)) {
        socket.emit('live-error', {
          workerId,
          mode: normalizedMode,
          message: 'Unknown live-feed mode.'
        });
        return;
      }

      if (
        !state ||
        !state.captureReady ||
        !state.online ||
        !(state.mediaSocketId || state.socketId) ||
        !io.sockets.sockets.get(state.mediaSocketId || state.socketId)
      ) {
        socket.emit('live-error', {
          workerId,
          mode: normalizedMode,
          message:
            'Camera is not ready or the Worker app is not connected.'
        });
        return;
      }

      state.desiredLiveMode =
        normalizedMode === 'camera'
          ? LIVE_CAMERA
          : LIVE_SCREEN;

      if (state.liveMode !== LIVE_NONE) {
        stopWorkerLive(state, 'mode-switch', true);
      }

      state.liveMode =
        normalizedMode === 'camera'
          ? LIVE_CAMERA_PENDING
          : LIVE_SCREEN_PENDING;

      state.liveAdminSocketId = socket.id;
      state.liveRequestedAt = Date.now();

      persistState();
      broadcastStates();

      io.to(state.mediaSocketId || state.socketId).emit('live-request', {
        adminSocketId: socket.id,
        workerId,
        mode: normalizedMode
      });

      socket.emit('live-status', {
        workerId,
        mode: state.liveMode
      });

      const expectedPending = state.liveMode;
      const timeoutMs = normalizedMode === 'screen' ? 90000 : 30000;

      setTimeout(() => {
        const latest = states.get(workerId);

        if (
          latest &&
          latest.liveAdminSocketId === socket.id &&
          latest.liveMode === expectedPending
        ) {
          resetLiveState(latest);
          broadcastStates();

          socket.emit('live-error', {
            workerId,
            mode: normalizedMode,
            message:
              normalizedMode === 'screen'
                ? 'Screen-share request timed out before Worker approval.'
                : 'Camera request timed out before the Worker live feed started.'
          });
        }
      }, timeoutMs);
    }
  );

  socket.on(
    'admin-live-stop',
    ({ workerId } = {}) => {
      if (user.role !== 'admin') return;

      const state = states.get(workerId);

      if (!state) return;

      state.desiredLiveMode = LIVE_NONE;
      if (state.liveMode !== LIVE_NONE) {
        stopWorkerLive(state, 'admin-stop', true);
      }
      persistState();
      broadcastStates();
    }
  );

  socket.on(
    'admin-switch-camera',
    ({ workerId, camera, facing } = {}) => {
      if (user.role !== 'admin') return;

      const state = states.get(workerId);

      if (!state || !state.socketId) {
        socket.emit('live-error', {
          workerId,
          message: 'Worker app is not connected.'
        });
        return;
      }

      const targetFacing = String(camera || facing || 'switch').toLowerCase();

      io.to(state.mediaSocketId || state.socketId).emit('switch-camera', {
        workerId: state.id,
        camera: targetFacing,
        facing: targetFacing
      });
    }
  );

  socket.on(
    'admin-timer-control',
    ({ workerId, action } = {}) => {
      if (user.role !== 'admin') return;

      const state = states.get(workerId);

      if (!state) return;

      if (action === 'start') {
        if (state.status === 'PAUSED') {
          timerState.resume(state);
        } else if (!state.sessionId) {
          startShift(state);
        }
      } else if (action === 'pause') {
        if (state.status === 'WORKING') {
          timerState.pause(state);
        }
      } else if (action === 'resume') {
        if (state.status === 'PAUSED') {
          timerState.resume(state);
        }
      } else if (action === 'stop') {
        if (state.sessionId) {
          const socketId = finishShift(state, 'admin-stop', 'STOPPED');
          if (socketId) {
            io.to(socketId).emit('timer-stopped', {
              reason: 'admin-stop'
            });
          }
        } else {
          state.status = 'STOPPED';
          state.online = false;
          resetLiveState(state);
        }
      }

      persistState();
      broadcastStates();

      if (state.socketId) {
        io.to(state.socketId).emit('timer-updated', safeWorkerState(state));
      }
    }
  );

  socket.on(
    'admin-reset-time',
    ({ workerId } = {}) => {
      if (user.role !== 'admin') return;

      const state = states.get(workerId);
      if (!state) return;

      if (state.status === 'WORKING') {
        state.accumulatedMs = 0;
        state.startedAt = Date.now();
      } else {
        state.accumulatedMs = 0;
        state.startedAt = null;
      }

      persistState();
      broadcastStates();

      if (state.socketId) {
        io.to(state.socketId).emit('timer-updated', safeWorkerState(state));
      }
    }
  );

  socket.on(
    'worker-live-ack',
    ({
      adminSocketId,
      workerId,
      mode,
      message
    } = {}) => {
      if (
        user.role !== 'worker' ||
        workerId !== user.id
      ) {
        return;
      }

      const state = states.get(workerId);

      if (
        !state ||
        (state.mediaSocketId || state.socketId) !== socket.id ||
        adminSocketId !== state.liveAdminSocketId
      ) {
        return;
      }

      io.to(adminSocketId).emit('live-ack', {
        workerId,
        mode,
        message: message || 'Worker received the live request.'
      });
    }
  );

  socket.on(
    'worker-live-started',
    ({
      adminSocketId,
      workerId,
      mode
    } = {}) => {
      if (
        user.role !== 'worker' ||
        workerId !== user.id
      ) {
        return;
      }

      const state = states.get(workerId);
      const normalizedMode =
        String(mode || '').toLowerCase();

      if (
        !state ||
        (state.mediaSocketId || state.socketId) !== socket.id ||
        adminSocketId !== state.liveAdminSocketId
      ) {
        return;
      }

      const expectedPending =
        normalizedMode === 'camera'
          ? LIVE_CAMERA_PENDING
          : LIVE_SCREEN_PENDING;

      if (state.liveMode !== expectedPending) {
        return;
      }

      state.liveMode =
        normalizedMode === 'camera'
          ? LIVE_CAMERA
          : LIVE_SCREEN;

      broadcastStates();

      io.to(adminSocketId).emit('live-status', {
        workerId,
        mode: state.liveMode
      });
    }
  );

  socket.on(
    'worker-live-error',
    ({
      adminSocketId,
      workerId,
      mode,
      message
    } = {}) => {
      if (
        user.role !== 'worker' ||
        workerId !== user.id
      ) {
        return;
      }

      const state = states.get(workerId);

      if (!state || state.socketId !== socket.id) return;

      const targetAdmin =
        adminSocketId ||
        state.liveAdminSocketId;

      resetLiveState(state);
      broadcastStates();

      if (targetAdmin) {
        io.to(targetAdmin).emit('live-error', {
          workerId,
          mode,
          message: message || 'Worker live feed could not start.'
        });
      }
    }
  );

  socket.on(
    'worker-live-stopped',
    ({
      adminSocketId,
      workerId,
      mode,
      reason
    } = {}) => {
      if (
        user.role !== 'worker' ||
        workerId !== user.id
      ) {
        return;
      }

      const state = states.get(workerId);

      if (!state || state.socketId !== socket.id) return;

      const targetAdmin =
        adminSocketId ||
        state.liveAdminSocketId;

      resetLiveState(state);
      broadcastStates();

      if (targetAdmin) {
        io.to(targetAdmin).emit('live-status', {
          workerId,
          mode: LIVE_NONE,
          previousMode: mode,
          reason: reason || 'stopped'
        });
      }
    }
  );

  socket.on(
    'rtc-offer',
    ({
      targetSocketId,
      workerId,
      mode,
      offer
    } = {}) => {
      if (
        user.role !== 'worker' ||
        workerId !== user.id ||
        !targetSocketId ||
        !offer
      ) {
        return;
      }

      const state = states.get(workerId);

      if (
        !state ||
        (state.mediaSocketId || state.socketId) !== socket.id ||
        state.liveAdminSocketId !== targetSocketId
      ) {
        return;
      }

      io.to(targetSocketId).emit('rtc-offer', {
        workerId,
        workerSocketId: socket.id,
        mode,
        offer
      });
    }
  );

  socket.on(
    'rtc-answer',
    ({
      targetSocketId,
      workerId,
      answer
    } = {}) => {
      if (
        user.role !== 'admin' ||
        !targetSocketId ||
        !answer
      ) {
        return;
      }

      const state = states.get(workerId);

      if (
        !state ||
        state.liveAdminSocketId !== socket.id ||
        (state.mediaSocketId || state.socketId) !== targetSocketId
      ) {
        return;
      }

      io.to(targetSocketId).emit('rtc-answer', {
        workerId,
        adminSocketId: socket.id,
        answer
      });
    }
  );

  socket.on(
    'rtc-ice',
    ({
      targetSocketId,
      workerId,
      candidate
    } = {}) => {
      if (!targetSocketId || !candidate) return;

      if (user.role === 'worker') {
        const state = states.get(workerId);

        if (
          !state ||
          user.id !== workerId ||
          (state.mediaSocketId || state.socketId) !== socket.id ||
          state.liveAdminSocketId !== targetSocketId
        ) {
          return;
        }
      } else if (user.role === 'admin') {
        const state = states.get(workerId);

        if (
          !state ||
          state.liveAdminSocketId !== socket.id ||
          (state.mediaSocketId || state.socketId) !== targetSocketId
        ) {
          return;
        }
      } else {
        return;
      }

      io.to(targetSocketId).emit('rtc-ice', {
        workerId,
        fromSocketId: socket.id,
        candidate
      });
    }
  );

  socket.on('disconnect', () => {
    if (user.role === 'worker' && user.id) {
      const state = states.get(user.id);

      if (state && state.mediaSocketId === socket.id) {
        state.mediaSocketId = null; state.captureReady = false;
        stopWorkerLive(state, "native-media-disconnected", false);
        broadcastStates();
      }
      if (state && state.socketId === socket.id) {
        const adminSocketId = state.liveAdminSocketId;

        if (adminSocketId) {
          io.to(adminSocketId).emit('live-error', {
            workerId: state.id,
            mode: state.liveMode,
            message: 'Worker app disconnected.'
          });
        }

        resetLiveState(state);
        state.online = false;
        state.captureReady = Boolean(state.mediaSocketId);
        state.socketId = null;
        persistState();
        broadcastStates();
      }
    }

    if (user.role === 'admin') {
      let changed = false;

      for (const state of states.values()) {
        if (state.liveAdminSocketId === socket.id) {
          if (state.socketId) {
            io.to(state.mediaSocketId || state.socketId).emit('stop-live', {
              adminSocketId: socket.id,
              reason: 'admin-disconnected'
            });
          }

          resetLiveState(state);
          changed = true;
        }
      }

      if (changed) broadcastStates();
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(
    `Work Timer on-demand server v3.2 listening on http://localhost:${PORT}`
  );
});
