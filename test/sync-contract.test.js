const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(__dirname, '..');
const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
const admin = fs.readFileSync(path.join(root, 'public', 'admin.html'), 'utf8');
const worker = fs.readFileSync(path.join(root, 'public', 'worker-features.js'), 'utf8');

test('task updates are persisted and broadcast for admin and worker refresh', () => {
  assert.match(server, /app\.post\('\/api\/admin\/tasks'[\s\S]*?writeFileSync\(TASKS_FILE[\s\S]*?io\.emit\('tasks-updated'\)/);
  assert.match(server, /app\.post\('\/api\/worker\/tasks\/:id\/accept'[\s\S]*?writeFileSync\(TASKS_FILE[\s\S]*?io\.emit\('tasks-updated'\)/);
  assert.match(server, /app\.post\('\/api\/worker\/tasks\/:id\/submit'[\s\S]*?writeFileSync\(TASKS_FILE[\s\S]*?io\.emit\('tasks-updated'\)/);
  assert.match(admin, /socket\.on\('tasks-updated',[\s\S]*?showView\('tasks'\)/);
  assert.match(worker, /s\.on\('tasks-updated',loadTasks\)/);
});

test('earnings changes persist and refresh worker/admin earnings views', () => {
  assert.match(server, /function persistEarnings\(\)[\s\S]*?writeJsonAtomic\(EARNINGS_FILE, adminEarnings\)/);
  assert.match(server, /persistEarnings\(\)[\s\S]*?io\.emit\('earnings-updated'/);
  assert.match(admin, /socket\.on\('earnings-updated',[\s\S]*?showView\('reports'\)/);
  assert.match(worker, /s\.on\('earnings-updated',loadTasks\)/);
});

test('chat HTTP and Socket.IO paths both persist and broadcast; history is normalized', () => {
  assert.match(server, /app\.post\('\/api\/chat\/send'[\s\S]*?persistChat\(\)[\s\S]*?io\.to\('admins'\)\.emit\('chat-message'/);
  assert.match(server, /socket\.on\('send-chat-message'[\s\S]*?persistChat\(\)[\s\S]*?io\.to\('admins'\)\.emit\('chat-message'/);
  assert.match(server, /socket\.on\('get-chat-history'[\s\S]*?resolveWorkerId\(m\.workerId\) === targetWorkerId/);
  assert.match(admin, /socket\.on\('chat-message'/);
  assert.match(worker, /s\.on\('chat-message',showMessage\)/);
});

test('dashboard monitoring, worker state broadcasts, and device list retain live update wiring', () => {
  assert.match(server, /io\.to\('admins'\)\.emit\('workers-state'/);
  assert.match(admin, /socket\.on\('workers-state'/);
  assert.match(admin, /socket\.on\('rtc-offer'/);
  assert.match(admin, /socket\.on\('rtc-ice'/);
  assert.match(worker, /s\.on\('connect'/);
});
