const { test } = require('node:test');
const assert = require('node:assert/strict');
const vm = require('node:vm');
const fs = require('node:fs');
const path = require('node:path');
const os = require('node:os');

function harness() {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), 'worktimer-task-test-'));
  const routes = new Map();
  const connections = [];
  const events = [];
  let io;

  const app = {
    disable() {},
    set() {},
    use() {},
    get(p, ...h) { routes.set('GET ' + p, h); },
    post(p, ...h) { routes.set('POST ' + p, h); }
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
      '\nthis.testApi={tasks,earnings,withdrawals,getWorkerEarnings};',
    sandbox
  );

  function request(method, p, user, body = {}) {
    const req = { body, ip: 'test-ip', session: { user }, params: {} };
    const res = {
      statusCode: 200,
      status(code) { this.statusCode = code; return this; },
      json(data) { this.data = data; }
    };

    // Match dynamic express routes like /api/tasks/:id/claim
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

test('Admin can create Direct and Open tasks; Direct task requires Accept/Deny', () => {
  const h = harness();
  try {
    const admin = { role: 'admin', username: 'admin' };
    const worker1 = { role: 'worker', id: 'worker1', username: 'john' };

    // 1. Create Direct Task
    const res1 = h.request('POST', '/api/tasks', admin, {
      title: 'Review System Logs',
      description: 'Check server logs for errors.',
      type: 'DIRECT',
      assignedWorkerId: 'worker1',
      rewardAmount: 50
    });
    assert.equal(res1.statusCode, 200);
    assert.equal(res1.data.task.status, 'ASSIGNED');

    const directTaskId = res1.data.task.id;

    // 2. Worker 1 accepts direct task
    const res2 = h.request('POST', `/api/tasks/${directTaskId}/respond`, worker1, { action: 'accept' });
    assert.equal(res2.statusCode, 200);
    assert.equal(res2.data.task.status, 'IN_PROGRESS');
  } finally {
    h.cleanup();
  }
});

test('Open task can be claimed atomically by only one worker', () => {
  const h = harness();
  try {
    const admin = { role: 'admin', username: 'admin' };
    const worker1 = { role: 'worker', id: 'worker1', username: 'john' };
    const worker2 = { role: 'worker', id: 'worker2', username: 'mark' };

    // Create Open Task
    const res1 = h.request('POST', '/api/tasks', admin, {
      title: 'Optimize Database Index',
      type: 'OPEN',
      rewardAmount: 100
    });
    const openTaskId = res1.data.task.id;

    // Worker 1 claims
    const claim1 = h.request('POST', `/api/tasks/${openTaskId}/claim`, worker1);
    assert.equal(claim1.statusCode, 200);
    assert.equal(claim1.data.task.assignedWorkerId, 'worker1');
    assert.equal(claim1.data.task.status, 'IN_PROGRESS');

    // Worker 2 tries to claim same task -> Conflict (409)
    const claim2 = h.request('POST', `/api/tasks/${openTaskId}/claim`, worker2);
    assert.equal(claim2.statusCode, 409);
  } finally {
    h.cleanup();
  }
});

test('Task submission, Admin approval, and 48-hour payment holding period', () => {
  const h = harness();
  try {
    const admin = { role: 'admin', username: 'admin' };
    const worker1 = { role: 'worker', id: 'worker1', username: 'john' };

    // Create & Accept Direct Task
    const t = h.request('POST', '/api/tasks', admin, {
      title: 'UI Redesign Review',
      type: 'DIRECT',
      assignedWorkerId: 'worker1',
      rewardAmount: 80
    }).data.task;

    h.request('POST', `/api/tasks/${t.id}/respond`, worker1, { action: 'accept' });

    // Submit Work
    const sub = h.request('POST', `/api/tasks/${t.id}/submit`, worker1, { note: 'Finished review' });
    assert.equal(sub.statusCode, 200);
    assert.equal(sub.data.task.status, 'PENDING_REVIEW');

    // Admin Approve Task with $80 payment
    const rev = h.request('POST', `/api/tasks/${t.id}/review`, admin, { action: 'approve', paymentAmount: 80 });
    assert.equal(rev.statusCode, 200);
    assert.equal(rev.data.task.status, 'COMPLETED');

    // Check Worker Earnings (Immediately after approval -> Pending Balance = $80, Available = $0)
    const earn1 = h.request('GET', '/api/earnings/my', worker1).data.summary;
    assert.equal(earn1.totalEarnings, 80);
    assert.equal(earn1.pendingBalance, 80);
    assert.equal(earn1.availableBalance, 0);

    // Worker cannot withdraw pending balance
    const wFail = h.request('POST', '/api/earnings/withdraw', worker1, { amount: 50 });
    assert.equal(wFail.statusCode, 400);

    // Fast-forward time past 48 hours for the ledger entry
    const ledger = h.earnings.find(e => e.taskId === t.id);
    ledger.availableAt = Date.now() - 1000; // 48h elapsed

    // Check Worker Earnings now -> Pending = $0, Available = $80
    const earn2 = h.request('GET', '/api/earnings/my', worker1).data.summary;
    assert.equal(earn2.pendingBalance, 0);
    assert.equal(earn2.availableBalance, 80);

    // Worker requests $50 withdrawal from Available Balance -> Success
    const wSuccess = h.request('POST', '/api/earnings/withdraw', worker1, { amount: 50, payoutMethod: 'PayPal' });
    assert.equal(wSuccess.statusCode, 200);
    assert.equal(wSuccess.data.withdrawal.status, 'PENDING');

    // Admin approves withdrawal
    const wId = wSuccess.data.withdrawal.id;
    const wReview = h.request('POST', '/api/admin/withdrawals/review', admin, { withdrawalId: wId, action: 'approve' });
    assert.equal(wReview.statusCode, 200);
    assert.equal(wReview.data.withdrawal.status, 'APPROVED');

    // Available balance is now $30
    const earn3 = h.request('GET', '/api/earnings/my', worker1).data.summary;
    assert.equal(earn3.availableBalance, 30);
  } finally {
    h.cleanup();
  }
});
