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
  maxHttpBufferSize: 10e6, // 10mb for media/file attachments
  pingTimeout: 20000,
  pingInterval: 25000
});

const PORT = Number(process.env.PORT || 3000);
const IS_PRODUCTION = process.env.NODE_ENV === 'production';

const DATA_DIR = path.join(__dirname, 'data');
const STATE_FILE = path.join(DATA_DIR, 'state.json');
const SESSIONS_FILE = path.join(DATA_DIR, 'sessions.json');
const HTTP_SESSION_DIR = path.join(DATA_DIR, 'http-sessions');
const TASKS_FILE = path.join(DATA_DIR, 'tasks.json');
const EARNINGS_FILE = path.join(DATA_DIR, 'earnings.json');
const WITHDRAWALS_FILE = path.join(DATA_DIR, 'withdrawals.json');
const CONVERSATIONS_FILE = path.join(DATA_DIR, 'conversations.json');
const MESSAGES_FILE = path.join(DATA_DIR, 'messages.json');
const NOTIFICATIONS_FILE = path.join(DATA_DIR, 'notifications.json');
const CHAT_SETTINGS_FILE = path.join(DATA_DIR, 'chat-settings.json');
const BEHAVIOR_SETTINGS_FILE = path.join(DATA_DIR, 'behavior-settings.json');
const UPLOADS_DIR = path.join(DATA_DIR, 'uploads');

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(HTTP_SESSION_DIR, { recursive: true });
fs.mkdirSync(UPLOADS_DIR, { recursive: true });

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

app.use(express.json({ limit: '10mb' }));
app.use(express.urlencoded({ extended: true, limit: '10mb' }));
app.use('/uploads', express.static(UPLOADS_DIR));

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

const users = [
  {
    role: 'admin',
    id: 'admin',
    username: process.env.ADMIN_USERNAME || 'admin',
    password: process.env.ADMIN_PASSWORD || 'admin123',
    name: process.env.ADMIN_NAME || 'Administrator'
  },
  {
    role: 'worker',
    id: 'worker1',
    username: process.env.WORKER1_USERNAME || 'john',
    password: process.env.WORKER1_PASSWORD || 'john123',
    name: 'Worker 1'
  },
  {
    role: 'worker',
    id: 'worker2',
    username: process.env.WORKER2_USERNAME || 'mark',
    password: process.env.WORKER2_PASSWORD || 'mark123',
    name: 'Worker 2'
  },
  {
    role: 'worker',
    id: 'worker3',
    username: process.env.WORKER3_USERNAME || 'anne',
    password: process.env.WORKER3_PASSWORD || 'anne123',
    name: 'Worker 3'
  },
  {
    role: 'worker',
    id: 'worker4',
    username: process.env.WORKER4_USERNAME || 'thing',
    password: process.env.WORKER4_PASSWORD || 'thing123',
    name: 'Worker 4'
  }
];

function resolveWorkerId(input) {
  if (!input) return '';
  const s = String(input).trim().toLowerCase();
  if (['all', 'open', 'unassigned', 'none', 'null', 'undefined'].includes(s)) return '';
  const u = users.find(u => u.role === 'worker' && (u.id.toLowerCase() === s || u.username.toLowerCase() === s));
  return u ? u.username : s;
}

function findUserByIdOrUsername(idOrUsername) {
  if (!idOrUsername) return null;
  const s = String(idOrUsername).trim().toLowerCase();
  return users.find(u => u.id.toLowerCase() === s || u.username.toLowerCase() === s) || null;
}

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
    sessionId: null,
    lastSeenAt: null,
    disconnectTimer: null,

    liveMode: LIVE_NONE,
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

function writeJsonAtomic(file, value) {
  const temp = `${file}.tmp`;
  fs.writeFileSync(temp, JSON.stringify(value, null, 2));
  fs.renameSync(temp, file);
}

const initialSampleTasks = [
  {
    id: 'sample-task-1',
    title: 'Data Entry & Summary Report',
    description: 'Compile monthly records into the master spreadsheet and format the output.',
    deadline: new Date(Date.now() + 2 * 24 * 60 * 60 * 1000).toISOString(),
    type: 'DIRECT',
    assignedWorkerId: 'worker1',
    status: 'ASSIGNED',
    rewardAmount: 50.00,
    createdBy: 'admin',
    createdAt: Date.now(),
    submission: null,
    review: null
  },
  {
    id: 'sample-task-2',
    title: 'Image Annotation & Tagging',
    description: 'Tag dataset images for training the recognition pipeline.',
    deadline: new Date(Date.now() + 3 * 24 * 60 * 60 * 1000).toISOString(),
    type: 'OPEN',
    assignedWorkerId: null,
    status: 'UNASSIGNED',
    rewardAmount: 35.00,
    createdBy: 'admin',
    createdAt: Date.now(),
    submission: null,
    review: null
  },
  {
    id: 'sample-task-3',
    title: 'Code Base Security Review',
    description: 'Review API authentication middleware and log security checks.',
    deadline: new Date(Date.now() + 4 * 24 * 60 * 60 * 1000).toISOString(),
    type: 'OPEN',
    assignedWorkerId: null,
    status: 'UNASSIGNED',
    rewardAmount: 75.00,
    createdBy: 'admin',
    createdAt: Date.now(),
    submission: null,
    review: null
  }
];

const persisted = readJson(STATE_FILE, {});
const workSessions = readJson(SESSIONS_FILE, []);
const tasks = readJson(TASKS_FILE, initialSampleTasks);
const earnings = readJson(EARNINGS_FILE, []);
const withdrawals = readJson(WITHDRAWALS_FILE, []);
const conversations = readJson(CONVERSATIONS_FILE, []);
const messages = readJson(MESSAGES_FILE, []);
const notifications = readJson(NOTIFICATIONS_FILE, []);
const chatSettings = readJson(CHAT_SETTINGS_FILE, { allowWorkerToWorkerChat: false });
const behaviorSettings = readJson(BEHAVIOR_SETTINGS_FILE, {});

const states = new Map();

for (const worker of users.filter(user => user.role === 'worker')) {
  const state = initialWorkerState(worker);
  const old = persisted[worker.id] || persisted[worker.username];

  if (old && Number.isFinite(Number(old.lastSeenAt))) {
    state.lastSeenAt = Number(old.lastSeenAt);
  }
  if (old && typeof old.sessionId === 'string' && old.sessionId &&
      workSessions.some(record => record.id === old.sessionId && record.endedAt === null)) {
    state.sessionId = old.sessionId;
    state.accumulatedMs = Math.max(0, Number(old.accumulatedMs) || 0);
    if (old.status === 'WORKING' && Number.isFinite(old.startedAt) && old.startedAt > 0) {
      state.status = 'WORKING';
      state.startedAt = old.startedAt;
    } else {
      state.status = 'PAUSED';
      state.startedAt = null;
    }
  }

  states.set(worker.id, state);
  if (worker.username && worker.username !== worker.id) {
    states.set(worker.username, state);
  }
}

function persistState() {
  const output = {};
  for (const [id, state] of states.entries()) {
    output[id] = {
      lastSeenAt: state.lastSeenAt,
      displayName: state.name,
      sessionId: state.sessionId,
      status: state.status,
      startedAt: state.startedAt,
      accumulatedMs: state.accumulatedMs
    };
  }
  writeJsonAtomic(STATE_FILE, output);
}

function persistSessions() { writeJsonAtomic(SESSIONS_FILE, workSessions.slice(-1000)); }
function persistTasks() { writeJsonAtomic(TASKS_FILE, tasks); }
function persistEarnings() { writeJsonAtomic(EARNINGS_FILE, earnings); }
function persistWithdrawals() { writeJsonAtomic(WITHDRAWALS_FILE, withdrawals); }
function persistConversations() { writeJsonAtomic(CONVERSATIONS_FILE, conversations); }
function persistMessages() { writeJsonAtomic(MESSAGES_FILE, messages); }
function persistNotifications() { writeJsonAtomic(NOTIFICATIONS_FILE, notifications.slice(-2000)); }
function persistChatSettings() { writeJsonAtomic(CHAT_SETTINGS_FILE, chatSettings); }
function persistBehaviorSettings() { writeJsonAtomic(BEHAVIOR_SETTINGS_FILE, behaviorSettings); }

if (!fs.existsSync(TASKS_FILE)) persistTasks();

function createNotification(userId, type, title, messageText, extra = {}) {
  const notif = {
    id: crypto.randomUUID(),
    userId,
    type,
    title,
    message: messageText,
    taskId: extra.taskId || null,
    conversationId: extra.conversationId || null,
    createdAt: Date.now(),
    read: false
  };
  notifications.push(notif);
  persistNotifications();

  io.to('user:' + userId).emit('new-notification', notif);
  io.to('worker:' + userId).emit('new-notification', notif);
  if (userId === 'admin') {
    io.to('admins').emit('new-notification', notif);
  }
  return notif;
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
    liveRequestedAt: state.liveRequestedAt
  };
}

function uniqueWorkerStates() {
  const map = new Map();
  for (const state of states.values()) {
    if (state && state.id && !map.has(state.id)) {
      map.set(state.id, safeWorkerState(state));
    }
  }
  return [...map.values()];
}

function broadcastStates() {
  io.to('admins').emit('workers-state', uniqueWorkerStates());
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
    io.to(state.socketId).emit('stop-live', { adminSocketId, reason });
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

function resetWorkerTimer(state) {
  if (!state) return;
  clearDisconnectTimer(state);
  const durationMs = timerState.elapsed(state);
  const record = workSessions.find(item => item.id === state.sessionId && item.endedAt === null);
  if (record) {
    record.endedAt = Date.now();
    record.durationMs = durationMs;
    record.reason = 'admin-reset';
    persistSessions();
  }
  state.status = 'STOPPED';
  state.startedAt = null;
  state.accumulatedMs = 0;
  state.sessionId = null;
  persistState();
}

function finishShift(state, reason = 'stopped', nextStatus = 'STOPPED') {
  if (!state) return null;
  const activeSocketId = state.socketId;

  clearDisconnectTimer(state);
  stopWorkerLive(state, reason, true);

  const durationMs = timerState.elapsed(state);
  const record = workSessions.find(item => item.id === state.sessionId && item.endedAt === null);

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

// Balance calculation helper with 48-hour hold
function getWorkerEarnings(workerId) {
  const now = Date.now();
  const workerLedger = earnings.filter(item => item.workerId === workerId);
  const workerWithdrawals = withdrawals.filter(w => w.workerId === workerId);

  let totalEarnings = 0;
  let pendingBalance = 0;
  let grossAvailable = 0;

  const ledgerWithStatus = workerLedger.map(item => {
    totalEarnings += Number(item.amount) || 0;
    const isAvailable = now >= item.availableAt;
    if (isAvailable) {
      grossAvailable += Number(item.amount) || 0;
    } else {
      pendingBalance += Number(item.amount) || 0;
    }
    return {
      ...item,
      status: isAvailable ? 'AVAILABLE' : 'HOLD_48H',
      remainingHoldMs: isAvailable ? 0 : Math.max(0, item.availableAt - now)
    };
  });

  const totalWithdrawn = workerWithdrawals
    .filter(w => w.status === 'APPROVED' || w.status === 'PENDING')
    .reduce((sum, w) => sum + (Number(w.amount) || 0), 0);

  const availableBalance = Math.max(0, grossAvailable - totalWithdrawn);

  return {
    workerId,
    totalEarnings,
    pendingBalance,
    availableBalance,
    grossAvailable,
    totalWithdrawn,
    ledger: ledgerWithStatus,
    withdrawals: workerWithdrawals
  };
}

function getOrCreateConversation(userA, userB, taskId = null) {
  let conv = conversations.find(c => {
    const hasA = c.participants.includes(userA);
    const hasB = c.participants.includes(userB);
    return hasA && hasB && c.participants.length === 2;
  });

  if (!conv) {
    const taskObj = taskId ? tasks.find(t => t.id === taskId) : null;
    conv = {
      id: crypto.randomUUID(),
      participants: [userA, userB],
      taskId: taskId || null,
      taskTitle: taskObj ? taskObj.title : null,
      createdAt: Date.now(),
      updatedAt: Date.now()
    };
    conversations.push(conv);
    persistConversations();
  } else if (taskId && !conv.taskId) {
    const taskObj = tasks.find(t => t.id === taskId);
    if (taskObj) {
      conv.taskId = taskId;
      conv.taskTitle = taskObj.title;
      conv.updatedAt = Date.now();
      persistConversations();
    }
  }

  return conv;
}

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
    version: '5.0.0-chat-notifications'
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
    loginAttempts.set(req.loginAttempt.key, req.loginAttempt.entry);
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
    const oldSocketId = state.socketId;
    if (oldSocketId) {
      stopWorkerLive(state, 'relogin', true);
      io.to(oldSocketId).emit('session-replaced');
      io.sockets.sockets.get(oldSocketId)?.disconnect(true);
    }
    state.online = false;
    state.socketId = null;
    state.captureReady = false;
    clearDisconnectTimer(state);
    persistState();
    broadcastStates();
  }

  res.json({
    ok: true,
    user: req.session.user,
    redirect: user.role === 'admin' ? '/admin.html' : '/worker.html'
  });
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
    return res.status(404).json({ ok: false, error: 'Unknown worker' });
  }
  res.json({ ok: true, state: safeWorkerState(state) });
});

app.post('/api/worker/start', requireRole('worker'), (req, res) => {
  const state = states.get(req.session.user.id);
  if (!state) return res.status(404).json({ ok: false, error: 'Unknown worker' });

  if (state.status === 'PAUSED') {
    timerState.resume(state);
    persistState();
    broadcastStates();
  } else if (!state.sessionId) {
    startShift(state);
    broadcastStates();
  }

  res.json({ ok: true, state: safeWorkerState(state) });
});

for (const action of ['pause', 'resume']) {
  app.post(`/api/worker/${action}`, requireRole('worker'), (req, res) => {
    const state = states.get(req.session.user.id);
    if (!state) return res.status(404).json({ ok: false, error: 'Unknown worker' });
    if (action === 'resume' && !state.sessionId) {
      startShift(state);
    } else {
      timerState[action](state);
    }
    persistState();
    broadcastStates();
    res.json({ ok: true, state: safeWorkerState(state) });
  });
}

app.post('/api/worker/stop', requireRole('worker'), (req, res) => {
  const state = states.get(req.session.user.id);
  if (!state) return res.status(404).json({ ok: false, error: 'Unknown worker' });

  if (state.sessionId) {
    const socketId = finishShift(state, 'worker-stop', 'STOPPED');
    if (socketId) {
      io.to(socketId).emit('timer-stopped', { reason: 'worker-stop' });
    }
    state.socketId = null;
    broadcastStates();
  } else {
    state.status = 'STOPPED';
    state.online = false;
    resetLiveState(state);
    broadcastStates();
  }

  res.json({ ok: true, state: safeWorkerState(state) });
});

app.get('/api/workers', requireRole('admin'), (_req, res) => {
  res.json({ ok: true, workers: uniqueWorkerStates() });
});

app.post('/api/admin/worker/timer', requireRole('admin'), (req, res) => {
  const { workerId, action } = req.body || {};
  const state = states.get(workerId);

  if (!state) return res.status(404).json({ ok: false, error: 'Unknown worker' });

  if (action === 'start' || action === 'resume') {
    if (state.status === 'PAUSED') {
      timerState.resume(state);
    } else if (!state.sessionId) {
      startShift(state);
    }
  } else if (action === 'pause') {
    if (state.status === 'WORKING') {
      timerState.pause(state);
    }
  } else if (action === 'reset') {
    resetWorkerTimer(state);
  } else if (action === 'stop') {
    if (state.sessionId) {
      const socketId = finishShift(state, 'admin-stop', 'STOPPED');
      if (socketId) {
        io.to(socketId).emit('timer-stopped', { reason: 'admin-stop' });
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

  res.json({ ok: true, state: safeWorkerState(state) });
});

app.get('/api/rtc-config', requireAuth, (_req, res) => {
  let iceServers = [{ urls: 'stun:stun.l.google.com:19302' }];
  if (process.env.RTC_ICE_SERVERS_JSON) {
    try {
      const parsed = JSON.parse(process.env.RTC_ICE_SERVERS_JSON);
      if (Array.isArray(parsed) && parsed.length) iceServers = parsed;
    } catch (_) {}
  }
  res.json({ ok: true, iceServers });
});

// --- TASK MANAGEMENT ENDPOINTS WITH REAL-TIME NOTIFICATIONS ---

app.get('/api/tasks', requireRole('admin'), (_req, res) => {
  res.json({
    ok: true,
    tasks: tasks.map(t => {
      const worker = users.find(u => u.id === t.assignedWorkerId);
      return {
        ...t,
        assignedWorkerName: worker ? worker.name : (t.assignedWorkerId || 'Unassigned')
      };
    })
  });
});

app.get('/api/tasks/available', requireRole('worker'), (_req, res) => {
  const availableTasks = tasks.filter(t => t.type === 'OPEN' && t.status === 'UNASSIGNED');
  res.json({ ok: true, tasks: availableTasks });
});

app.get('/api/tasks/my', requireRole('worker'), (req, res) => {
  const workerId = req.session.user.id;
  const myTasks = tasks.filter(t => t.assignedWorkerId === workerId);
  res.json({ ok: true, tasks: myTasks });
});

app.post('/api/tasks', requireRole('admin'), (req, res) => {
  const { title, description, deadline, type, assignedWorkerId, rewardAmount } = req.body || {};

  if (!title || !title.trim()) {
    return res.status(400).json({ ok: false, error: 'Task title is required' });
  }

  const normalizedType = String(type || 'OPEN').toUpperCase();
  if (!['DIRECT', 'OPEN'].includes(normalizedType)) {
    return res.status(400).json({ ok: false, error: 'Type must be DIRECT or OPEN' });
  }

  let assignedWorker = null;
  let status = 'UNASSIGNED';

  if (normalizedType === 'DIRECT') {
    if (!assignedWorkerId) {
      return res.status(400).json({ ok: false, error: 'Assigned worker is required for Direct tasks' });
    }
    const targetUser = users.find(u => u.role === 'worker' && u.id === assignedWorkerId);
    if (!targetUser) {
      return res.status(400).json({ ok: false, error: 'Selected worker does not exist' });
    }
    assignedWorker = targetUser.id;
    status = 'ASSIGNED';
  }

  const newTask = {
    id: crypto.randomUUID(),
    title: title.trim(),
    description: (description || '').trim(),
    deadline: deadline || null,
    type: normalizedType,
    assignedWorkerId: assignedWorker,
    status: status,
    rewardAmount: Math.max(0, Number(rewardAmount) || 0),
    createdBy: req.session.user.username,
    createdAt: Date.now(),
    submission: null,
    review: null
  };

  tasks.push(newTask);
  persistTasks();

  if (assignedWorker) {
    createNotification(assignedWorker, 'TASK_ASSIGNED', 'New Direct Task Assigned', `Admin assigned you task: "${newTask.title}"`, { taskId: newTask.id });
  } else {
    users.filter(u => u.role === 'worker').forEach(w => {
      createNotification(w.id, 'OPEN_TASK_AVAILABLE', 'New Open Task Available', `New task available to grab: "${newTask.title}"`, { taskId: newTask.id });
    });
  }

  io.to('admins').emit('tasks-updated');
  if (assignedWorker) {
    io.to('worker:' + assignedWorker).emit('tasks-updated');
  } else {
    io.emit('tasks-updated');
  }

  return res.json({ ok: true, task: newTask });
});

app.post('/api/tasks/:id/claim', requireRole('worker'), (req, res) => {
  const taskId = req.params.id;
  const workerId = req.session.user.id;

  const task = tasks.find(t => t.id === taskId);
  if (!task) return res.status(404).json({ ok: false, error: 'Task not found' });

  if (task.type !== 'OPEN' || task.status !== 'UNASSIGNED' || task.assignedWorkerId != null) {
    return res.status(409).json({ ok: false, error: 'This task has already been claimed or is unavailable.' });
  }

  task.assignedWorkerId = workerId;
  task.status = 'IN_PROGRESS';
  task.claimedAt = Date.now();
  persistTasks();

  createNotification('admin', 'TASK_CLAIMED', 'Open Task Claimed', `${req.session.user.name} claimed open task "${task.title}"`, { taskId: task.id });

  io.to('admins').emit('tasks-updated');
  io.emit('tasks-updated');

  return res.json({ ok: true, task });
});

app.post('/api/tasks/:id/respond', requireRole('worker'), (req, res) => {
  const taskId = req.params.id;
  const workerId = req.session.user.id;
  const { action } = req.body || {};

  const task = tasks.find(t => t.id === taskId);
  if (!task) return res.status(404).json({ ok: false, error: 'Task not found' });

  if (task.assignedWorkerId !== workerId || task.status !== 'ASSIGNED') {
    return res.status(400).json({ ok: false, error: 'Task is not assigned to you or cannot be responded to.' });
  }

  if (action === 'accept') {
    task.status = 'IN_PROGRESS';
    task.acceptedAt = Date.now();
    createNotification('admin', 'TASK_ACCEPTED', 'Task Assignment Accepted', `${req.session.user.name} accepted task "${task.title}"`, { taskId: task.id });
  } else if (action === 'deny') {
    task.status = 'REJECTED_BY_WORKER';
    task.deniedAt = Date.now();
    createNotification('admin', 'TASK_DENIED', 'Task Assignment Denied', `${req.session.user.name} denied task "${task.title}"`, { taskId: task.id });
  } else {
    return res.status(400).json({ ok: false, error: 'Invalid action' });
  }

  persistTasks();
  io.to('admins').emit('tasks-updated');
  io.to('worker:' + workerId).emit('tasks-updated');

  return res.json({ ok: true, task });
});

app.post('/api/tasks/:id/submit', requireRole('worker'), (req, res) => {
  const taskId = req.params.id;
  const workerId = req.session.user.id;
  const { note } = req.body || {};

  const task = tasks.find(t => t.id === taskId);
  if (!task) return res.status(404).json({ ok: false, error: 'Task not found' });

  if (task.assignedWorkerId !== workerId) {
    return res.status(403).json({ ok: false, error: 'You are not assigned to this task.' });
  }

  if (task.status !== 'IN_PROGRESS' && task.status !== 'REVISION_REQUIRED') {
    return res.status(400).json({ ok: false, error: 'Task cannot be submitted in its current state.' });
  }

  task.status = 'PENDING_REVIEW';
  task.submission = {
    note: (note || '').trim(),
    submittedAt: Date.now()
  };

  persistTasks();
  createNotification('admin', 'TASK_SUBMITTED', 'Task Submitted for Review', `${req.session.user.name} submitted task "${task.title}" for review`, { taskId: task.id });

  io.to('admins').emit('tasks-updated');
  io.to('worker:' + workerId).emit('tasks-updated');

  return res.json({ ok: true, task });
});

app.post('/api/tasks/:id/review', requireRole('admin'), (req, res) => {
  const { action, paymentAmount, feedback } = req.body || {};
  const taskId = req.params.id;

  const task = tasks.find(t => t.id === taskId);
  if (!task) return res.status(404).json({ ok: false, error: 'Task not found' });

  if (action === 'approve') {
    const amount = Number(paymentAmount) || Number(task.rewardAmount) || 0;
    if (amount <= 0) return res.status(400).json({ ok: false, error: 'Payment amount must be greater than 0' });

    task.status = 'COMPLETED';
    const now = Date.now();
    task.review = {
      approved: true,
      feedback: (feedback || 'Approved').trim(),
      reviewedAt: now,
      paymentAmount: amount
    };

    let ledgerEntry = earnings.find(e => e.taskId === task.id);
    if (!ledgerEntry) {
      ledgerEntry = {
        id: crypto.randomUUID(),
        taskId: task.id,
        taskTitle: task.title,
        workerId: task.assignedWorkerId,
        amount: amount,
        approvedAt: now,
        availableAt: now + (48 * 60 * 60 * 1000)
      };
      earnings.push(ledgerEntry);
      persistEarnings();
    }
    persistTasks();

    if (task.assignedWorkerId) {
      createNotification(task.assignedWorkerId, 'TASK_APPROVED', 'Task Approved & Payment Credited', `Your task "${task.title}" was approved! $${amount.toFixed(2)} credited to 48h pending hold.`, { taskId: task.id });
    }

    io.to('admins').emit('tasks-updated');
    io.to('admins').emit('earnings-updated');
    if (task.assignedWorkerId) {
      io.to('worker:' + task.assignedWorkerId).emit('tasks-updated');
      io.to('worker:' + task.assignedWorkerId).emit('earnings-updated');
    }

    return res.json({ ok: true, task, ledgerEntry });
  } else if (action === 'reject') {
    if (!feedback || !feedback.trim()) {
      return res.status(400).json({ ok: false, error: 'Feedback is required when rejecting a task' });
    }
    task.status = 'REVISION_REQUIRED';
    task.review = {
      approved: false,
      feedback: feedback.trim(),
      reviewedAt: Date.now()
    };
    persistTasks();

    if (task.assignedWorkerId) {
      createNotification(task.assignedWorkerId, 'TASK_REJECTED', 'Task Revision Required', `Admin requested revision for "${task.title}": ${feedback.trim()}`, { taskId: task.id });
    }

    io.to('admins').emit('tasks-updated');
    if (task.assignedWorkerId) {
      io.to('worker:' + task.assignedWorkerId).emit('tasks-updated');
    }

    return res.json({ ok: true, task });
  } else {
    return res.status(400).json({ ok: false, error: 'Invalid review action' });
  }
});

// EARNINGS & WITHDRAWAL ENDPOINTS

app.get('/api/earnings/my', requireRole('worker'), (req, res) => {
  const workerId = req.session.user.id;
  const summary = getWorkerEarnings(workerId);
  res.json({ ok: true, summary });
});

app.post('/api/earnings/withdraw', requireRole('worker'), (req, res) => {
  const workerId = req.session.user.id;
  const { amount, payoutMethod } = req.body || {};

  const withdrawAmount = Number(amount);
  if (!withdrawAmount || withdrawAmount <= 0) {
    return res.status(400).json({ ok: false, error: 'Invalid withdrawal amount' });
  }

  const calc = getWorkerEarnings(workerId);
  if (withdrawAmount > calc.availableBalance) {
    return res.status(400).json({ ok: false, error: 'Requested amount exceeds your available balance. Pending earnings are held for 48 hours.' });
  }

  const newWithdrawal = {
    id: crypto.randomUUID(),
    workerId: workerId,
    amount: withdrawAmount,
    payoutMethod: (payoutMethod || 'Bank Transfer').trim(),
    status: 'PENDING',
    requestedAt: Date.now(),
    reviewedAt: null,
    adminNote: null
  };

  withdrawals.push(newWithdrawal);
  persistWithdrawals();

  createNotification('admin', 'WITHDRAWAL_REQUESTED', 'New Withdrawal Request', `${req.session.user.name} requested a withdrawal of $${withdrawAmount.toFixed(2)}`);

  io.to('admins').emit('earnings-updated');
  io.to('worker:' + workerId).emit('earnings-updated');

  return res.json({ ok: true, withdrawal: newWithdrawal });
});

app.get('/api/admin/earnings', requireRole('admin'), (_req, res) => {
  const workerSummaries = users
    .filter(u => u.role === 'worker')
    .map(u => ({
      worker: { id: u.id, name: u.name, username: u.username },
      summary: getWorkerEarnings(u.id)
    }));

  res.json({
    ok: true,
    workerSummaries,
    allLedger: earnings,
    allWithdrawals: withdrawals.map(w => {
      const u = users.find(user => user.id === w.workerId);
      return { ...w, workerName: u ? u.name : w.workerId };
    })
  });
});

app.post('/api/admin/withdrawals/review', requireRole('admin'), (req, res) => {
  const { withdrawalId, action, adminNote } = req.body || {};

  const w = withdrawals.find(item => item.id === withdrawalId);
  if (!w) return res.status(404).json({ ok: false, error: 'Withdrawal request not found' });

  if (action === 'approve') {
    w.status = 'APPROVED';
    createNotification(w.workerId, 'WITHDRAWAL_APPROVED', 'Withdrawal Approved', `Your withdrawal request of $${w.amount.toFixed(2)} was approved!`);
  } else if (action === 'reject') {
    w.status = 'REJECTED';
    createNotification(w.workerId, 'WITHDRAWAL_REJECTED', 'Withdrawal Rejected', `Your withdrawal request of $${w.amount.toFixed(2)} was declined.`);
  } else {
    return res.status(400).json({ ok: false, error: 'Invalid action' });
  }

  w.reviewedAt = Date.now();
  w.adminNote = (adminNote || '').trim();
  persistWithdrawals();

  io.to('admins').emit('earnings-updated');
  io.to('worker:' + w.workerId).emit('earnings-updated');

  return res.json({ ok: true, withdrawal: w });
});

// --- REAL-TIME CHAT ENDPOINTS & PERMISSIONS ---

// Get list of conversations for current user
app.get('/api/chat/conversations', requireAuth, (req, res) => {
  const currentUser = req.session.user;
  const myUserId = currentUser.id || currentUser.username;

  const myConversations = conversations.filter(c =>
    c.participants.includes(currentUser.id) ||
    c.participants.includes(currentUser.username)
  );

  const enriched = myConversations.map(conv => {
    const partnerId = conv.participants.find(p => p !== currentUser.id && p !== currentUser.username);
    const partnerObj = findUserByIdOrUsername(partnerId);
    const partnerState = partnerObj ? states.get(partnerObj.id) : null;

    const convMessages = messages.filter(m => m.conversationId === conv.id && !m.deleted);
    const latestMessage = convMessages.length ? convMessages[convMessages.length - 1] : null;

    const unreadCount = convMessages.filter(m =>
      m.senderId !== currentUser.id &&
      m.senderId !== currentUser.username &&
      (!m.readBy || (!m.readBy.includes(currentUser.id) && !m.readBy.includes(currentUser.username)))
    ).length;

    return {
      id: conv.id,
      partner: partnerObj ? {
        id: partnerObj.id,
        name: partnerObj.name,
        username: partnerObj.username,
        role: partnerObj.role,
        online: partnerState ? partnerState.online : false
      } : { id: partnerId, name: partnerId, online: false },
      taskId: conv.taskId,
      taskTitle: conv.taskTitle,
      latestMessage: latestMessage ? {
        text: latestMessage.text || (latestMessage.attachment ? `[${latestMessage.attachment.type}] ${latestMessage.attachment.name}` : ''),
        senderName: latestMessage.senderName,
        sentAt: latestMessage.sentAt
      } : null,
      unreadCount,
      updatedAt: conv.updatedAt
    };
  }).sort((a, b) => b.updatedAt - a.updatedAt);

  res.json({ ok: true, conversations: enriched, allowWorkerToWorkerChat: chatSettings.allowWorkerToWorkerChat });
});

// Start or open a conversation with a user
app.post('/api/chat/conversations/start', requireAuth, (req, res) => {
  const currentUser = req.session.user;
  const myUserId = currentUser.id || currentUser.username;
  const { targetUserId, taskId } = req.body || {};

  if (!targetUserId) return res.status(400).json({ ok: false, error: 'Target user is required' });
  const partnerObj = findUserByIdOrUsername(targetUserId);
  if (!partnerObj) return res.status(404).json({ ok: false, error: 'Target user not found' });

  // Worker-to-worker check
  if (currentUser.role === 'worker' && partnerObj.role === 'worker' && !chatSettings.allowWorkerToWorkerChat) {
    return res.status(403).json({ ok: false, error: 'Worker-to-worker direct chat is currently disabled by Admin.' });
  }

  const partnerId = partnerObj.id || partnerObj.username;
  const conv = getOrCreateConversation(myUserId, partnerId, taskId);

  res.json({ ok: true, conversationId: conv.id, partner: { id: partnerObj.id, name: partnerObj.name, role: partnerObj.role } });
});

// Get messages for a conversation
app.get('/api/chat/conversations/:id/messages', requireAuth, (req, res) => {
  const currentUser = req.session.user;
  const myUserId = currentUser.id || currentUser.username;
  const convId = req.params.id;

  const conv = conversations.find(c => c.id === convId);
  if (!conv) return res.status(404).json({ ok: false, error: 'Conversation not found' });

  if (!conv.participants.includes(currentUser.id) && !conv.participants.includes(currentUser.username)) {
    return res.status(403).json({ ok: false, error: 'Access denied to this conversation' });
  }

  // Mark unread messages as read by current user
  let markedAny = false;
  messages.forEach(m => {
    if (m.conversationId === convId && m.senderId !== currentUser.id && m.senderId !== currentUser.username) {
      if (!m.readBy) m.readBy = [];
      if (!m.readBy.includes(myUserId)) {
        m.readBy.push(myUserId);
        markedAny = true;
      }
    }
  });

  if (markedAny) {
    persistMessages();
    io.to('conv:' + convId).emit('message-read', { conversationId: convId, readByUserId: myUserId });
  }

  const convMessages = messages.filter(m => m.conversationId === convId);
  res.json({ ok: true, messages: convMessages, conversation: conv });
});

// Send a message
app.post('/api/chat/messages', requireAuth, (req, res) => {
  const currentUser = req.session.user;
  const myUserId = currentUser.id || currentUser.username;
  const { conversationId, text, attachment, replyTo, taskId } = req.body || {};

  const conv = conversations.find(c => c.id === conversationId);
  if (!conv) return res.status(404).json({ ok: false, error: 'Conversation not found' });

  if (!conv.participants.includes(currentUser.id) && !conv.participants.includes(currentUser.username)) {
    return res.status(403).json({ ok: false, error: 'Access denied to this conversation' });
  }

  if ((!text || !text.trim()) && !attachment) {
    return res.status(400).json({ ok: false, error: 'Message cannot be empty' });
  }

  const taskObj = taskId ? tasks.find(t => t.id === taskId) : null;

  const newMsg = {
    id: crypto.randomUUID(),
    conversationId,
    senderId: myUserId,
    senderName: currentUser.name,
    text: (text || '').trim(),
    attachment: attachment || null,
    replyTo: replyTo || null,
    taskId: taskId || conv.taskId || null,
    taskTitle: taskObj ? taskObj.title : (conv.taskTitle || null),
    sentAt: Date.now(),
    editedAt: null,
    deleted: false,
    readBy: [myUserId]
  };

  messages.push(newMsg);
  conv.updatedAt = Date.now();

  persistMessages();
  persistConversations();

  // Socket.IO real-time message emit
  io.to('conv:' + conversationId).emit('new-message', newMsg);

  // Notify other participant(s)
  conv.participants.forEach(pid => {
    if (pid !== currentUser.id && pid !== currentUser.username) {
      createNotification(pid, 'CHAT_MESSAGE', `Message from ${currentUser.name}`, newMsg.text || 'Sent an attachment', { conversationId });
      io.to('user:' + pid).emit('chat-updated');
      io.to('worker:' + pid).emit('chat-updated');
    }
  });

  io.to('admins').emit('chat-updated');

  res.json({ ok: true, message: newMsg });
});

// Edit a message
app.post('/api/chat/messages/:id/edit', requireAuth, (req, res) => {
  const currentUser = req.session.user;
  const myUserId = currentUser.id || currentUser.username;
  const msgId = req.params.id;
  const { text } = req.body || {};

  const msg = messages.find(m => m.id === msgId);
  if (!msg) return res.status(404).json({ ok: false, error: 'Message not found' });

  if (msg.senderId !== myUserId) {
    return res.status(403).json({ ok: false, error: 'You can only edit your own messages' });
  }

  if (!text || !text.trim()) {
    return res.status(400).json({ ok: false, error: 'Edited message cannot be empty' });
  }

  msg.text = text.trim();
  msg.editedAt = Date.now();
  persistMessages();

  io.to('conv:' + msg.conversationId).emit('message-updated', msg);
  res.json({ ok: true, message: msg });
});

// Delete a message
const deleteMessageHandler = (req, res) => {
  const currentUser = req.session.user;
  const myUserId = currentUser.id || currentUser.username;
  const msgId = req.params.id;

  const msg = messages.find(m => m.id === msgId);
  if (!msg) return res.status(404).json({ ok: false, error: 'Message not found' });

  if (msg.senderId !== myUserId && currentUser.role !== 'admin') {
    return res.status(403).json({ ok: false, error: 'Permission denied' });
  }

  msg.deleted = true;
  msg.text = 'This message was deleted.';
  msg.attachment = null;
  persistMessages();

  io.to('conv:' + msg.conversationId).emit('message-updated', msg);
  res.json({ ok: true, message: msg });
};

app.post('/api/chat/messages/:id/delete', requireAuth, deleteMessageHandler);
if (typeof app.delete === 'function') {
  app.delete('/api/chat/messages/:id', requireAuth, deleteMessageHandler);
}

// File upload endpoint for chat attachments
app.post('/api/chat/upload', requireAuth, (req, res) => {
  const { fileName, fileType, fileData } = req.body || {};
  if (!fileName || !fileData) {
    return res.status(400).json({ ok: false, error: 'Missing file data' });
  }

  try {
    const base64Data = fileData.replace(/^data:[^;]+;base64,/, '');
    const buffer = Buffer.from(base64Data, 'base64');
    const ext = path.extname(fileName) || '.bin';
    const safeName = `${crypto.randomUUID()}${ext}`;
    const filePath = path.join(UPLOADS_DIR, safeName);

    fs.writeFileSync(filePath, buffer);

    const isImage = (fileType || '').startsWith('image/') || ['.png', '.jpg', '.jpeg', '.gif', '.webp'].includes(ext.toLowerCase());
    const fileUrl = `/uploads/${safeName}`;

    res.json({
      ok: true,
      attachment: {
        url: fileUrl,
        name: fileName,
        type: isImage ? 'image' : 'file'
      }
    });
  } catch (err) {
    res.status(500).json({ ok: false, error: 'File upload failed: ' + err.message });
  }
});

// NOTIFICATION ENDPOINTS

app.get('/api/notifications/my', requireAuth, (req, res) => {
  const myUserId = req.session.user.id || req.session.user.username;
  const myNotifs = notifications.filter(n => n.userId === myUserId || (req.session.user.role === 'admin' && n.userId === 'admin'));
  res.json({ ok: true, notifications: myNotifs.sort((a, b) => b.createdAt - a.createdAt) });
});

app.post('/api/notifications/read', requireAuth, (req, res) => {
  const myUserId = req.session.user.id || req.session.user.username;
  const { notificationId, all } = req.body || {};

  if (all) {
    notifications.forEach(n => {
      if (n.userId === myUserId || (req.session.user.role === 'admin' && n.userId === 'admin')) {
        n.read = true;
      }
    });
  } else if (notificationId) {
    const target = notifications.find(n => n.id === notificationId);
    if (target) target.read = true;
  }

  persistNotifications();
  res.json({ ok: true });
});

// BEHAVIOR SETTINGS ENDPOINTS

app.get('/api/settings/behavior', requireAuth, (req, res) => {
  const myUserId = req.session.user.id || req.session.user.username;
  const defaults = {
    taskNotifications: true,
    chatNotifications: true,
    enableSound: true,
    enableVibration: true,
    displayOverOtherApps: false,
    launchOnStartup: false,
    notificationPreviews: true
  };
  const current = behaviorSettings[myUserId] || defaults;
  res.json({ ok: true, settings: current });
});

app.post('/api/settings/behavior', requireAuth, (req, res) => {
  const myUserId = req.session.user.id || req.session.user.username;
  behaviorSettings[myUserId] = {
    ...(behaviorSettings[myUserId] || {}),
    ...(req.body || {})
  };
  persistBehaviorSettings();
  res.json({ ok: true, settings: behaviorSettings[myUserId] });
});

// Admin toggle worker-to-worker chat
app.post('/api/admin/chat-settings', requireRole('admin'), (req, res) => {
  const { allowWorkerToWorkerChat } = req.body || {};
  chatSettings.allowWorkerToWorkerChat = Boolean(allowWorkerToWorkerChat);
  persistChatSettings();

  io.emit('chat-settings-updated', chatSettings);
  res.json({ ok: true, chatSettings });
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

io.on('connection', socket => {
  let user = socket.request.session?.user;

  if (!user && socket.handshake.auth && socket.handshake.auth.user) {
    const authUser = socket.handshake.auth.user;
    const matched = users.find(u => u.username === authUser.username || u.id === authUser.id || u.username === authUser.id);
    if (matched) {
      user = {
        role: matched.role,
        id: matched.id || null,
        username: matched.username,
        name: matched.name
      };
    }
  }

  if (!user) {
    return socket.disconnect(true);
  }

  const userId = user.id || user.username;
  socket.join('user:' + userId);
  if (user.username) socket.join('user:' + user.username);

  if (user.role === 'admin') {
    socket.join('admins');
    socket.emit('workers-state', uniqueWorkerStates());
  } else if (user.role === 'worker' && user.id) {
    const wid = resolveWorkerId(user.username || user.id);
    socket.join('worker:' + wid);
    socket.join('worker:' + user.id);

    const state = states.get(wid) || states.get(user.id);

    if (!state) {
      socket.emit('timer-stopped', { reason: 'Timer is not running.' });
      return socket.disconnect(true);
    }

    if (state.socketId && state.socketId !== socket.id) {
      io.to(state.socketId).emit('session-replaced');
      io.sockets.sockets.get(state.socketId)?.disconnect(true);
    }

    clearDisconnectTimer(state);
    state.online = true;
    state.captureReady = false;
    state.socketId = socket.id;
    state.lastSeenAt = Date.now();
    persistState();
    broadcastStates();
  }

  socket.on('join-conversation', ({ conversationId } = {}) => {
    if (conversationId) {
      socket.join('conv:' + conversationId);
    }
  });

  const diagnosticCommands = new Set(['ping','status','info','time','uptime']);
  const pendingDiagnostics = new Map();
  socket.on('admin-device-command', ({workerId,command,requestId} = {}) => {
    if(user.role !== 'admin') return;
    if(typeof command !== 'string' || !diagnosticCommands.has(command) || typeof requestId !== 'string' || requestId.length>48) return;
    const worker = states.get(workerId);
    if(!worker?.online || !worker.socketId || !io.sockets.sockets.has(worker.socketId)) {
      socket.emit('device-command-result',{workerId,command,requestId,ok:false,result:'Worker offline'});return;
    }
    const key=workerId+':'+requestId;
    pendingDiagnostics.set(key,{workerSocketId:worker.socketId,command});
    socket.data.pendingDiagnostics = pendingDiagnostics;
    io.to(worker.socketId).emit('device-command',{command,requestId,adminSocketId:socket.id});
    setTimeout(()=>{if(pendingDiagnostics.delete(key)) socket.emit('device-command-result',{workerId,command,requestId,ok:false,result:'No response from worker. Browser/APK client may not support diagnostics.'});},6500);
  });
  socket.on('device-command-reply', ({requestId,adminSocketId,command,result}={}) => {
    if(user.role!=='worker' || !user.id || typeof requestId!=='string') return;
    const admin = io.sockets.sockets.get(adminSocketId);
    if(!admin || admin.request.session?.user?.role!=='admin') return;
    const worker=states.get(user.id);
    if(worker?.socketId!==socket.id || !diagnosticCommands.has(command)) return;
    const origin=admin.data.pendingDiagnostics;
    const pending=origin?.get(user.id+':'+requestId);
    if(!pending || pending.workerSocketId!==socket.id || pending.command!==command) return;
    origin.delete(user.id+':'+requestId);
    admin.emit('device-command-result',{workerId:user.id,command,requestId,ok:true,result:String(result??'').slice(0,1800)});
  });

  socket.on('worker-capture-ready', () => {
    if (user.role !== 'worker') return;
    const state = states.get(user.id);
    if (!state || state.socketId !== socket.id) return;
    state.captureReady = true;
    broadcastStates();
  });

  socket.on('worker-heartbeat', () => {
    if (user.role !== 'worker' || !user.id) return;
    const state = states.get(user.id);
    if (!state || state.socketId !== socket.id) return;
    state.online = true;
    state.lastSeenAt = Date.now();
    persistState();
    broadcastStates();
  });

  socket.on('admin-ping-worker', ({ workerId, nonce } = {}) => {
    if (user.role !== 'admin') return;
    const state = states.get(workerId);
    if (!state?.socketId || !io.sockets.sockets.get(state.socketId)) {
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

  socket.on('worker-pong', ({ adminSocketId, workerId, nonce, workerReceivedAt } = {}) => {
    if (user.role !== 'worker' || workerId !== user.id || !adminSocketId || !nonce) return;
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
  });

  socket.on('admin-live-request', ({ workerId, mode } = {}) => {
    if (user.role !== 'admin') return;
    const state = states.get(workerId);
    const normalizedMode = String(mode || '').toLowerCase();

    if (!['camera', 'screen'].includes(normalizedMode)) {
      socket.emit('live-error', { workerId, mode: normalizedMode, message: 'Unknown live-feed mode.' });
      return;
    }

    if (!state || !state.captureReady || !state.online || !state.socketId || !io.sockets.sockets.get(state.socketId)) {
      socket.emit('live-error', { workerId, mode: normalizedMode, message: 'Camera is not ready or the Worker app is not connected.' });
      return;
    }

    if (state.liveMode !== LIVE_NONE) {
      socket.emit('live-error', { workerId, mode: normalizedMode, message: 'Another live feed is already active or waiting for Worker approval. Stop it first.' });
      return;
    }

    state.liveMode = normalizedMode === 'camera' ? LIVE_CAMERA_PENDING : LIVE_SCREEN_PENDING;
    state.liveAdminSocketId = socket.id;
    state.liveRequestedAt = Date.now();

    broadcastStates();

    io.to(state.socketId).emit('live-request', { adminSocketId: socket.id, workerId, mode: normalizedMode });
    socket.emit('live-status', { workerId, mode: state.liveMode });

    const expectedPending = state.liveMode;
    const timeoutMs = normalizedMode === 'screen' ? 90000 : 30000;

    setTimeout(() => {
      const latest = states.get(workerId);
      if (latest && latest.liveAdminSocketId === socket.id && latest.liveMode === expectedPending) {
        resetLiveState(latest);
        broadcastStates();
        socket.emit('live-error', {
          workerId,
          mode: normalizedMode,
          message: normalizedMode === 'screen' ? 'Screen-share request timed out.' : 'Camera request timed out.'
        });
      }
    }, timeoutMs);
  });

  socket.on('admin-live-stop', ({ workerId } = {}) => {
    if (user.role !== 'admin') return;
    const state = states.get(workerId);
    if (!state) return;
    if (state.liveMode !== LIVE_NONE) {
      stopWorkerLive(state, 'admin-stop', true);
      broadcastStates();
    }
  });

  socket.on('admin-switch-camera', ({ workerId, camera } = {}) => {
    if (user.role !== 'admin') return;
    const state = states.get(workerId);
    if (!state || !state.socketId) {
      socket.emit('live-error', { workerId, message: 'Worker app is not connected.' });
      return;
    }
    io.to(state.socketId).emit('switch-camera', { camera });
  });

  socket.on('admin-timer-control', ({ workerId, action } = {}) => {
    if (user.role !== 'admin') return;
    const state = states.get(workerId);
    if (!state) return;

    if (action === 'start' || action === 'resume') {
      if (state.status === 'PAUSED') {
        timerState.resume(state);
      } else if (!state.sessionId) {
        startShift(state);
      }
    } else if (action === 'pause') {
      if (state.status === 'WORKING') {
        timerState.pause(state);
      }
    } else if (action === 'reset') {
      resetWorkerTimer(state);
    } else if (action === 'stop') {
      if (state.sessionId) {
        const socketId = finishShift(state, 'admin-stop', 'STOPPED');
        if (socketId) {
          io.to(socketId).emit('timer-stopped', { reason: 'admin-stop' });
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
  });

  socket.on('worker-live-ack', ({ adminSocketId, workerId, mode, message } = {}) => {
    if (user.role !== 'worker' || workerId !== user.id) return;
    const state = states.get(workerId);
    if (!state || state.socketId !== socket.id || adminSocketId !== state.liveAdminSocketId) return;
    io.to(adminSocketId).emit('live-ack', { workerId, mode, message: message || 'Worker received request.' });
  });

  socket.on('worker-live-started', ({ adminSocketId, workerId, mode } = {}) => {
    if (user.role !== 'worker' || workerId !== user.id) return;
    const state = states.get(workerId);
    const normalizedMode = String(mode || '').toLowerCase();
    if (!state || state.socketId !== socket.id || adminSocketId !== state.liveAdminSocketId) return;

    const expectedPending = normalizedMode === 'camera' ? LIVE_CAMERA_PENDING : LIVE_SCREEN_PENDING;
    if (state.liveMode !== expectedPending) return;

    state.liveMode = normalizedMode === 'camera' ? LIVE_CAMERA : LIVE_SCREEN;
    broadcastStates();
    io.to(adminSocketId).emit('live-status', { workerId, mode: state.liveMode });
  });

  socket.on('worker-live-error', ({ adminSocketId, workerId, mode, message } = {}) => {
    if (user.role !== 'worker' || workerId !== user.id) return;
    const state = states.get(workerId);
    if (!state || state.socketId !== socket.id) return;
    const targetAdmin = adminSocketId || state.liveAdminSocketId;
    resetLiveState(state);
    broadcastStates();
    if (targetAdmin) {
      io.to(targetAdmin).emit('live-error', { workerId, mode, message: message || 'Worker live feed error.' });
    }
  });

  socket.on('worker-live-stopped', ({ adminSocketId, workerId, mode, reason } = {}) => {
    if (user.role !== 'worker' || workerId !== user.id) return;
    const state = states.get(workerId);
    if (!state || state.socketId !== socket.id) return;
    const targetAdmin = adminSocketId || state.liveAdminSocketId;
    resetLiveState(state);
    broadcastStates();
    if (targetAdmin) {
      io.to(targetAdmin).emit('live-status', { workerId, mode: LIVE_NONE, previousMode: mode, reason: reason || 'stopped' });
    }
  });

  socket.on('rtc-offer', ({ targetSocketId, workerId, mode, offer } = {}) => {
    if (user.role !== 'worker' || workerId !== user.id || !targetSocketId || !offer) return;
    const state = states.get(workerId);
    if (!state || state.socketId !== socket.id || state.liveAdminSocketId !== targetSocketId) return;
    io.to(targetSocketId).emit('rtc-offer', { workerId, workerSocketId: socket.id, mode, offer });
  });

  socket.on('rtc-answer', ({ targetSocketId, workerId, answer } = {}) => {
    if (user.role !== 'admin' || !targetSocketId || !answer) return;
    const state = states.get(workerId);
    if (!state || state.liveAdminSocketId !== socket.id || state.socketId !== targetSocketId) return;
    io.to(targetSocketId).emit('rtc-answer', { workerId, adminSocketId: socket.id, answer });
  });

  socket.on('rtc-ice', ({ targetSocketId, workerId, candidate } = {}) => {
    if (!targetSocketId || !candidate) return;
    if (user.role === 'worker') {
      const state = states.get(workerId);
      if (!state || user.id !== workerId || state.socketId !== socket.id || state.liveAdminSocketId !== targetSocketId) return;
    } else if (user.role === 'admin') {
      const state = states.get(workerId);
      if (!state || state.liveAdminSocketId !== socket.id || state.socketId !== targetSocketId) return;
    } else {
      return;
    }
    io.to(targetSocketId).emit('rtc-ice', { workerId, fromSocketId: socket.id, candidate });
  });

  socket.on('disconnect', () => {
    if (user.role === 'worker' && user.id) {
      const state = states.get(user.id);
      if (state && state.socketId === socket.id) {
        const adminSocketId = state.liveAdminSocketId;
        if (adminSocketId) {
          io.to(adminSocketId).emit('live-error', { workerId: state.id, mode: state.liveMode, message: 'Worker app disconnected.' });
        }
        resetLiveState(state);
        state.online = false;
        state.captureReady = false;
        state.socketId = null;
        broadcastStates();
        clearDisconnectTimer(state);
        persistState();
      }
    }

    if (user.role === 'admin') {
      let changed = false;
      for (const state of states.values()) {
        if (state.liveAdminSocketId === socket.id) {
          if (state.socketId) {
            io.to(state.socketId).emit('stop-live', { adminSocketId: socket.id, reason: 'admin-disconnected' });
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
  console.log(`Work Timer v5.0 with Real-Time Chat & Notifications on http://localhost:${PORT}`);

  // Render Keep-Alive Self-Ping: Pings every 12 minutes to prevent Render free instance from sleeping
  const pingUrl = process.env.RENDER_EXTERNAL_URL || process.env.SERVER_URL;
  if (pingUrl) {
    const https = require('https');
    const http = require('http');
    const targetHealthUrl = pingUrl.replace(/\/$/, '') + '/health';
    const client = targetHealthUrl.startsWith('https') ? https : http;

    setInterval(() => {
      client.get(targetHealthUrl, (res) => {
        console.log(`Keep-alive ping to ${targetHealthUrl} -> Status: ${res.statusCode}`);
      }).on('error', (err) => {
        console.warn(`Keep-alive ping error: ${err.message}`);
      });
    }, 12 * 60 * 1000); // 12 minutes
  }
});
