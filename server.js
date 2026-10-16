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
    name: process.env.WORKER1_NAME || 'John'
  },
  {
    role: 'worker',
    id: 'worker2',
    username: process.env.WORKER2_USERNAME || 'mark',
    password: process.env.WORKER2_PASSWORD || 'mark123',
    name: process.env.WORKER2_NAME || 'Mark'
  },
  {
    role: 'worker',
    id: 'worker3',
    username: process.env.WORKER3_USERNAME || 'anne',
    password: process.env.WORKER3_PASSWORD || 'anne123',
    name: process.env.WORKER3_NAME || 'Anne'
  },
  {
    role: 'worker',
    id: 'worker4',
    username: process.env.WORKER4_USERNAME || 'thing',
    password: process.env.WORKER4_PASSWORD || 'thing123',
    name: process.env.WORKER4_NAME || 'Thing'
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
  if (old && typeof old.displayName === 'string' && old.displayName.trim()) {
    state.name = old.displayName.trim().slice(0, 60);
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
      lastSeenAt: state.lastSeenAt,
      displayName: state.name
    };
  }

  writeJsonAtomic(STATE_FILE, output);
}

function persistSessions() {
  writeJsonAtomic(SESSIONS_FILE, workSessions.slice(-1000));
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

// Persistent administrator task list.
const TASKS_FILE = path.join(DATA_DIR, 'admin-tasks.json');
const adminTasks = readJson(TASKS_FILE, []);
app.get('/api/worker/tasks', requireRole('worker'), (req,res) => {
  res.json({ok:true,tasks:adminTasks.filter(t => !t.workerId || t.workerId === req.session.user.id)});
});
app.post('/api/worker/tasks/:id/submit', requireRole('worker'), (req,res) => {
  const task=adminTasks.find(t=>t.id===req.params.id && (!t.workerId || t.workerId===req.session.user.id));
  if(!task) return res.status(404).json({error:'Task not found'});
  if(task.status==='completed') return res.status(409).json({error:'Task already completed'});
  if(task.status!=='open' && task.assignedWorkerId!==req.session.user.id) return res.status(409).json({error:'Task is not available'});
  if(task.assignedWorkerId && task.assignedWorkerId!==req.session.user.id) return res.status(409).json({error:'Task claimed by another worker'});
  task.assignedWorkerId=req.session.user.id;task.status='submitted';task.submittedAt=new Date().toISOString();
  fs.writeFileSync(TASKS_FILE,JSON.stringify(adminTasks,null,2));
  io.to('admins').emit('tasks-updated');res.json({ok:true,task});
});
app.post('/api/worker/tasks/:id/accept', requireRole('worker'), (req,res) => {
  const task=adminTasks.find(t=>t.id===req.params.id && (!t.workerId || t.workerId===req.session.user.id));
  if(!task) return res.status(404).json({error:'Task not found'});
  if(task.status!=='open' || (task.assignedWorkerId && task.assignedWorkerId!==req.session.user.id)) return res.status(409).json({error:'Task unavailable'});
  if(adminTasks.some(t=>t.assignedWorkerId===req.session.user.id && t.status==='in_progress')) return res.status(409).json({error:'Submit your current task first'});
  task.assignedWorkerId=req.session.user.id;task.status='in_progress';task.acceptedAt=new Date().toISOString();
  fs.writeFileSync(TASKS_FILE,JSON.stringify(adminTasks,null,2));
  io.to('admins').emit('tasks-updated');res.json({ok:true,task});
});
// Update only the public display name. Login username, worker ID, and records never change.
app.post('/api/admin/workers/:id/display-name', requireRole('admin'), (req, res) => {
  const state = states.get(req.params.id);
  if (!state) return res.status(404).json({ok:false,error:'Worker not found'});
  const name = String(req.body?.name ?? '').trim().replace(/\s+/g, ' ');
  if (!name || name.length > 60 || /[\x00-\x1f\x7f]/.test(name)) {
    return res.status(400).json({ok:false,error:'Enter a display name (1–60 characters).'});
  }
  state.name = name;
  persistState();
  broadcastStates();
  res.json({ok:true,workerId:state.id,name:state.name});
});
app.get('/api/admin/tasks', requireRole('admin'), (_req,res) => res.json({ok:true,tasks:adminTasks}));
app.post('/api/admin/tasks', requireRole('admin'), (req,res) => {
  const title = String(req.body?.title || '').trim().slice(0,160);
  const description = String(req.body?.description || '').trim().slice(0,2000);
  const workerId = String(req.body?.workerId || '');
  if (!title) return res.status(400).json({error:'Title required'});
  if (workerId && !states.has(workerId)) return res.status(400).json({error:'Unknown worker'});
  const task = {id:crypto.randomUUID(),title,description,workerId,status:'open',createdAt:new Date().toISOString()};
  adminTasks.unshift(task); fs.writeFileSync(TASKS_FILE,JSON.stringify(adminTasks,null,2));
  res.json({ok:true,task});
});
app.post('/api/admin/tasks/status',requireRole('admin'),(req,res)=>{
  const task=adminTasks.find(t=>t.id===req.body?.id);
  if(!task || !['open','completed'].includes(req.body?.status)) return res.status(400).json({error:'Invalid task or status'});
  task.status=req.body.status;task.updatedAt=new Date().toISOString();
  fs.writeFileSync(TASKS_FILE,JSON.stringify(adminTasks,null,2));res.json({ok:true,task});
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

const chatMessages = [];

app.get('/api/chat/:workerId', requireAuth, (req, res) => {
  const targetWorkerId = req.session.user.role === 'worker' ? req.session.user.id : req.params.workerId;
  const history = chatMessages.filter(m => m.workerId === targetWorkerId);
  res.json({ ok: true, messages: history });
});

app.post('/api/chat/send', requireAuth, (req, res) => {
  const { workerId, text } = req.body || {};
  const messageText = String(text || '').trim();
  if (!messageText) return res.status(400).json({ ok: false, error: 'Message cannot be empty' });

  const targetWorkerId = req.session.user.role === 'worker' ? req.session.user.id : workerId;
  if (!targetWorkerId) return res.status(400).json({ ok: false, error: 'Target worker required' });

  const msg = {
    id: crypto.randomUUID(),
    workerId: targetWorkerId,
    senderRole: req.session.user.role,
    senderName: req.session.user.name || req.session.user.username,
    text: messageText,
    timestamp: Date.now()
  };

  chatMessages.push(msg);

  io.to('admins').emit('chat-message', msg);
  const workerState = states.get(targetWorkerId);
  if (workerState && workerState.socketId) {
    io.to(workerState.socketId).emit('chat-message', msg);
  }

  res.json({ ok: true, message: msg });
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

  // Allowlisted, authenticated diagnostics only. No remote shell or ADB execution.
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
    // Only match responses to live pending admin requests.
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
        socket.emit('live-error', {
          workerId,
          mode: normalizedMode,
          message:
            'Another live feed is already active or waiting for Worker approval. Stop it first.'
        });
        return;
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
    ({ workerId, camera } = {}) => {
      if (user.role !== 'admin') return;

      const state = states.get(workerId);

      if (!state || !state.socketId) {
        socket.emit('live-error', {
          workerId,
          message: 'Worker app is not connected.'
        });
        return;
      }

      io.to(state.socketId).emit('switch-camera', { camera });
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

  socket.on('send-chat-message', ({ workerId, text } = {}) => {
    const messageText = String(text || '').trim();
    if (!messageText) return;

    const targetWorkerId = user.role === 'worker' ? user.id : workerId;
    if (!targetWorkerId) return;

    const msg = {
      id: crypto.randomUUID(),
      workerId: targetWorkerId,
      senderRole: user.role,
      senderName: user.name || user.username,
      text: messageText,
      timestamp: Date.now()
    };

    chatMessages.push(msg);

    io.to('admins').emit('chat-message', msg);
    const workerState = states.get(targetWorkerId);
    if (workerState && workerState.socketId) {
      io.to(workerState.socketId).emit('chat-message', msg);
    }
  });

  socket.on('get-chat-history', ({ workerId } = {}) => {
    const targetWorkerId = user.role === 'worker' ? user.id : workerId;
    const history = chatMessages.filter(m => m.workerId === targetWorkerId);
    socket.emit('chat-history', { workerId: targetWorkerId, messages: history });
  });

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
