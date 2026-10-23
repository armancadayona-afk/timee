const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');

function harness() {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'worktimer-chat-test-'));
  const routes = new Map();
  const connections = [];
  const events = [];
  let io;

  const app = {
    disable() {},
    set() {},
    use() {},
    get(p, ...h) { routes.set('GET ' + p, h); },
    post(p, ...h) { routes.set('POST ' + p, h); },
    delete(p, ...h) { routes.set('DELETE ' + p, h); }
  };

  function express() { return app; }
  express.json = express.urlencoded = express.static = () => () => {};

  class Io {
    constructor() {
      io = this;
      this.engine = { use() {} };
      this.sockets = { sockets: new Map() };
    }
    on(e, h) { if (e === 'connection') connections.push(h); }
    to(room) {
      return { emit: (event, payload) => events.push({ room, event, payload }) };
    }
    emit(event, payload) {
      events.push({ room: 'broadcast', event, payload });
    }
  }

  function session() { return () => {}; }

  const sandbox = {
    require(name) {
      if (name === 'express') return express;
      if (name === 'express-session') return session;
      if (name === 'session-file-store') return () => class {};
      if (name === 'socket.io') return { Server: Io };
      if (name === 'dotenv') return { config() {} };
      if (name === 'http') return { createServer() { return { listen() {} }; } };
      if (name === './timer-state') return require('../timer-state');
      return require(name);
    },
    __dirname: temp,
    process: { env: {} },
    console: { log() {} },
    setTimeout: () => 1,
    clearTimeout() {}
  };

  vm.createContext(sandbox);
  vm.runInContext(
    fs.readFileSync(path.join(__dirname, '../server.js'), 'utf8') +
      '\nthis.testApi={conversations,messages,notifications,chatSettings,behaviorSettings};',
    sandbox
  );

  function request(method, p, user, body = {}) {
    const req = { body, ip: 'test-ip', session: { user }, params: {} };
    const res = {
      statusCode: 200,
      status(code) { this.statusCode = code; return this; },
      json(data) { this.data = data; }
    };

    let handlers = routes.get(method + ' ' + p);
    if (!handlers) {
      for (const [key, h] of routes.entries()) {
        const [m, routePath] = key.split(' ');
        if (m !== method) continue;
        const regexPath = routePath.replace(/:[a-zA-Z0-9_]+/g, '([^/]+)');
        const match = p.match(new RegExp('^' + regexPath + '$'));
        if (match) {
          handlers = h;
          const paramNames = (routePath.match(/:[a-zA-Z0-9_]+/g) || []).map(s => s.slice(1));
          paramNames.forEach((name, i) => { req.params[name] = match[i + 1]; });
          break;
        }
      }
    }

    if (!handlers) throw new Error(`Route not found: ${method} ${p}`);
    let i = 0;
    const next = () => handlers[i++]?.(req, res, next);
    next();
    return res;
  }

  return {
    temp,
    request,
    events,
    ...sandbox.testApi,
    cleanup: () => fs.rmSync(temp, { recursive: true, force: true })
  };
}

test('Admin and Worker real-time chat, message read receipts, and task reference', () => {
  const h = harness();
  try {
    const admin = { role: 'admin', id: 'admin', username: 'admin', name: 'Admin' };
    const worker1 = { role: 'worker', id: 'worker1', username: 'john', name: 'Worker 1' };

    // 1. Worker starts conversation with Admin
    const convRes = h.request('POST', '/api/chat/conversations/start', worker1, { targetUserId: 'admin', taskId: 'sample-task-1' });
    assert.equal(convRes.statusCode, 200);
    const convId = convRes.data.conversationId;

    // 2. Worker sends message with task reference
    const msgRes = h.request('POST', '/api/chat/messages', worker1, {
      conversationId: convId,
      text: 'I started working on sample task 1.',
      taskId: 'sample-task-1'
    });
    assert.equal(msgRes.statusCode, 200);
    assert.equal(msgRes.data.message.text, 'I started working on sample task 1.');

    // 3. Admin gets conversation list -> Unread count = 1
    const adminConvs = h.request('GET', '/api/chat/conversations', admin);
    assert.equal(adminConvs.statusCode, 200);
    assert.equal(adminConvs.data.conversations[0].unreadCount, 1);

    // 4. Admin reads messages -> auto marks read
    const readRes = h.request('GET', `/api/chat/conversations/${convId}/messages`, admin);
    assert.equal(readRes.statusCode, 200);

    // 5. Admin conversation list now has unread count = 0
    const adminConvs2 = h.request('GET', '/api/chat/conversations', admin);
    assert.equal(adminConvs2.data.conversations[0].unreadCount, 0);

    // 6. Admin replies to message
    const replyRes = h.request('POST', '/api/chat/messages', admin, {
      conversationId: convId,
      text: 'Great, let me know when submitted!',
      replyTo: { messageId: msgRes.data.message.id, textPreview: 'I started working...' }
    });
    assert.equal(replyRes.statusCode, 200);
  } finally {
    h.cleanup();
  }
});

test('Worker to Worker chat permission restriction when disabled vs enabled', () => {
  const h = harness();
  try {
    const worker1 = { role: 'worker', id: 'worker1', username: 'john', name: 'Worker 1' };
    const worker2 = { role: 'worker', id: 'worker2', username: 'mark', name: 'Worker 2' };
    const admin = { role: 'admin', id: 'admin', username: 'admin', name: 'Admin' };

    // 1. Worker 1 tries to chat with Worker 2 -> Disabled by default (403)
    const failRes = h.request('POST', '/api/chat/conversations/start', worker1, { targetUserId: 'worker2' });
    assert.equal(failRes.statusCode, 403);

    // 2. Admin enables worker-to-worker chat
    const setRes = h.request('POST', '/api/admin/chat-settings', admin, { allowWorkerToWorkerChat: true });
    assert.equal(setRes.statusCode, 200);

    // 3. Worker 1 tries to chat with Worker 2 -> Now allowed
    const passRes = h.request('POST', '/api/chat/conversations/start', worker1, { targetUserId: 'worker2' });
    assert.equal(passRes.statusCode, 200);
  } finally {
    h.cleanup();
  }
});

test('App Behavior settings save and retrieval', () => {
  const h = harness();
  try {
    const worker1 = { role: 'worker', id: 'worker1', username: 'john' };

    // Get default settings
    const getRes = h.request('GET', '/api/settings/behavior', worker1);
    assert.equal(getRes.statusCode, 200);
    assert.equal(getRes.data.settings.taskNotifications, true);

    // Update settings
    const postRes = h.request('POST', '/api/settings/behavior', worker1, {
      enableSound: false,
      enableVibration: false,
      launchOnStartup: true
    });
    assert.equal(postRes.statusCode, 200);
    assert.equal(postRes.data.settings.enableSound, false);
    assert.equal(postRes.data.settings.launchOnStartup, true);
  } finally {
    h.cleanup();
  }
});
