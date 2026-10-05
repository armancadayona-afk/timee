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
const HTTP_SESSION_DIR = path.join(DATA_DIR, 'http-sessions');

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(HTTP_SESSION_DIR, { recursive: true });

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

const users = [
  {
    role: 'admin',
    username: process.env.ADMIN_USERNAME || 'admin',
    password: process.env.ADMIN_PASSWORD || 'admin123',
    name: process.env.ADMIN_NAME || 'Administrator'
  },
  {
    role: 'worker',
    id: 'worker1',
    username: process.env.WORKER1_USERNAME || 'john',
    password: process.env.WORKER1_PASSWORD || 'john123',
    name: process.env.WORKER1_NAME || 'Worker 1'
  },
  {
    role: 'worker',
    id: 'worker2',
    username: process.env.WORKER2_USERNAME || 'mark',
    password: process.env.WORKER2_PASSWORD || 'mark123',
    name: process.env.WORKER2_NAME || 'Worker 2'
  },
  {
    role: 'worker',
    id: 'worker3',
    username: process.env.WORKER3_USERNAME || 'anne',
    password: process.env.WORKER3_PASSWORD || 'anne123',
    name: process.env.WORKER3_NAME || 'Worker 3'
  }
];

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

const persisted = readJson(STATE_FILE, {});
const workSessions = readJson(SESSIONS_FILE, []);
const states = new Map();

for (const worker of users.filter(user => user.role === 'worker')) {
  const state = initialWorkerState(worker);
  const old = persisted[worker.id];

  // Do not restore a previously running live/timer state after a server restart.
  // Historical shift records remain in sessions.json.
  if (old && Number.isFinite(Number(old.lastSeenAt))) {
    state.lastSeenAt = Number(old.lastSeenAt);
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
      lastSeenAt: state.lastSeenAt
    };
  }

  writeJsonAtomic(STATE_FILE, output);
}

const tasks = readJson(TASKS_FILE, []);
const chatMessages = readJson(CHAT_FILE, []);

function persistSessions() {
  writeJsonAtomic(SESSIONS_FILE, workSessions.slice(-1000));
}

function persistTasks() {
  writeJsonAtomic(TASKS_FILE, tasks);
}

function persistChat() {
  writeJsonAtomic(CHAT_FILE, chatMessages);
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
    io.to(state.socketId).emit('stop-live', {
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
  // Starting the clock must not interrupt an existing broadcast.

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

    // Logging in does not start the timer. If a prior session is still marked
    // running, close it so the Worker explicitly starts the next shift.
    if (state.status === 'WORKING' || state.sessionId) {
      const oldSocketId = finishShift(state, 'relogin', 'STOPPED');

      if (oldSocketId) {
        io.to(oldSocketId).emit('session-replaced');
        io.sockets.sockets.get(oldSocketId)?.disconnect(true);
      }
    } else {
      const oldSocketId = state.socketId;
      stopWorkerLive(state, 'relogin', true);
      state.status = 'STOPPED';
      state.online = false;
      state.socketId = null;
      state.startedAt = null;
      state.accumulatedMs = 0;

      if (oldSocketId) {
        io.to(oldSocketId).emit('session-replaced');
        io.sockets.sockets.get(oldSocketId)?.disconnect(true);
      }
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

// Tasks API
app.get('/api/tasks', requireAuth, (req, res) => {
  const user = req.session.user;
  if (user.role === 'worker') {
    const workerTasks = tasks.filter(t => t.workerId === user.id);
    return res.json({ ok: true, tasks: workerTasks });
  } else if (user.role === 'admin') {
    const workerId = req.query.workerId;
    const filtered = workerId ? tasks.filter(t => t.workerId === workerId) : tasks;
    return res.json({ ok: true, tasks: filtered });
  }
  res.status(403).json({ ok: false, error: 'Forbidden' });
});

app.get('/api/worker/tasks', requireRole('worker'), (req, res) => {
  const workerTasks = tasks.filter(t => t.workerId === req.session.user.id);
  res.json({ ok: true, tasks: workerTasks });
});

app.post('/api/tasks', requireRole('admin'), (req, res) => {
  const { workerId, title, description, dueAt } = req.body || {};
  if (!workerId || !title) {
    return res.status(400).json({ ok: false, error: 'workerId and title are required' });
  }
  const idStr = 'task_' + Date.now() + '_' + Math.random().toString(36).substring(2, 7);
  const newTask = {
    taskId: idStr,
    id: idStr,
    workerId,
    title: String(title).trim(),
    description: String(description || '').trim(),
    status: 'ASSIGNED',
    assignedAt: Date.now(),
    assignedBy: req.session.user.name || 'Admin',
    dueAt: dueAt || null,
    submittedAt: null,
    completedAt: null,
    adminNote: ''
  };
  tasks.push(newTask);
  persistTasks();

  io.to(`worker:${workerId}`).to('admins').emit('task:assigned', newTask);
  io.to(`worker:${workerId}`).to('admins').emit('task:updated', newTask);

  res.json({ ok: true, task: newTask });
});

app.post('/api/tasks/:taskId/submit', requireRole('worker'), (req, res) => {
  const taskId = req.params.taskId;
  const task = tasks.find(t => t.taskId === taskId || t.id === taskId);
  if (!task) return res.status(404).json({ ok: false, error: 'Task not found' });
  if (task.workerId !== req.session.user.id) {
    return res.status(403).json({ ok: false, error: 'Cannot submit task assigned to another worker' });
  }

  task.status = 'SUBMITTED';
  task.submittedAt = Date.now();
  persistTasks();

  io.to(`worker:${task.workerId}`).to('admins').emit('task:submitted', task);
  io.to(`worker:${task.workerId}`).to('admins').emit('task:updated', task);

  res.json({ ok: true, task });
});

app.post('/api/tasks/:taskId/approve', requireRole('admin'), (req, res) => {
  const taskId = req.params.taskId;
  const task = tasks.find(t => t.taskId === taskId || t.id === taskId);
  if (!task) return res.status(404).json({ ok: false, error: 'Task not found' });

  task.status = 'COMPLETED';
  task.completedAt = Date.now();
  persistTasks();

  io.to(`worker:${task.workerId}`).to('admins').emit('task:approved', task);
  io.to(`worker:${task.workerId}`).to('admins').emit('task:updated', task);

  res.json({ ok: true, task });
});

app.post('/api/tasks/:taskId/return', requireRole('admin'), (req, res) => {
  const taskId = req.params.taskId;
  const { adminNote } = req.body || {};
  const task = tasks.find(t => t.taskId === taskId || t.id === taskId);
  if (!task) return res.status(404).json({ ok: false, error: 'Task not found' });

  task.status = 'RETURNED';
  task.adminNote = String(adminNote || '').trim();
  persistTasks();

  io.to(`worker:${task.workerId}`).to('admins').emit('task:returned', task);
  io.to(`worker:${task.workerId}`).to('admins').emit('task:updated', task);

  res.json({ ok: true, task });
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
    state.captureReady = false;
    state.socketId = socket.id;
    state.lastSeenAt = Date.now();
    persistState();
    broadcastStates();
  }

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
        !state.socketId ||
        !io.sockets.sockets.get(state.socketId)
      ) {
        socket.emit('live-error', {
          workerId,
          mode: normalizedMode,
          message:
            'Camera is not ready or the Worker app is not connected.'
        });
        return;
      }

      if (state.liveMode !== LIVE_NONE) {
        stopWorkerLive(state, 'mode-switch', true);
      }

      state.liveMode =
        normalizedMode === 'camera'
          ? LIVE_CAMERA_PENDING
          : LIVE_SCREEN_PENDING;

      state.liveAdminSocketId = socket.id;
      state.liveRequestedAt = Date.now();

      broadcastStates();

      io.to(state.socketId).emit('live-request', {
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

      if (state.liveMode !== LIVE_NONE) {
        stopWorkerLive(state, 'admin-stop', true);
        broadcastStates();
      }
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

      io.to(state.socketId).emit('switch-camera', {
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
        state.socketId !== socket.id ||
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
        state.socketId !== socket.id ||
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
        state.socketId !== socket.id ||
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
        state.socketId !== targetSocketId
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
          state.socketId !== socket.id ||
          state.liveAdminSocketId !== targetSocketId
        ) {
          return;
        }
      } else if (user.role === 'admin') {
        const state = states.get(workerId);

        if (
          !state ||
          state.liveAdminSocketId !== socket.id ||
          state.socketId !== targetSocketId
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
        state.captureReady = false;
        state.socketId = null;
        broadcastStates();

        clearDisconnectTimer(state);

        state.disconnectTimer = setTimeout(() => {
          if (
            !state.socketId &&
            state.sessionId
          ) {
            finishShift(
              state,
              'disconnect-timeout',
              'STOPPED'
            );
            broadcastStates();
          }
        }, 30000);
      }
    }

    if (user.role === 'admin') {
      let changed = false;

      for (const state of states.values()) {
        if (state.liveAdminSocketId === socket.id) {
          if (state.socketId) {
            io.to(state.socketId).emit('stop-live', {
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
