const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(__dirname, '..');

test('admin keeps design-only Task, Chat, and Earning tabs plus monitor zoom/fullscreen and reset controls', () => {
  const html = fs.readFileSync(path.join(root, 'public/admin.html'), 'utf8');
  for (const tab of ['tasks', 'chat', 'reports']) assert.match(html, new RegExp(`data-view="${tab}"`));
  assert.match(html, /Visual design only/);
  assert.match(html, /data-action="zoom"/);
  assert.match(html, /data-action="fullscreen"/);
  assert.match(html, /data-action="timer-reset"/);
  assert.match(html, /id="btn-timer-reset"/);
  assert.doesNotMatch(html, /id="dashboard-create-task"|id="btn-send-chat"|id="earning-form"/);
});

test('task/chat/earning APIs and worker feature script are removed while worker navigation remains', () => {
  const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  const worker = fs.readFileSync(path.join(root, 'public/worker.html'), 'utf8');
  assert.doesNotMatch(server, /\/api\/(?:admin\/tasks|worker\/tasks|chat|admin\/earnings)/);
  assert.doesNotMatch(server, /send-chat-message|get-chat-history|tasks-updated|earnings-updated/);
  assert.doesNotMatch(worker, /worker-features\.js/);
  assert.match(worker, /worker-navigation\.js/);
  assert.match(worker, /screen-tasks/);
  assert.match(worker, /screen-chat/);
  assert.match(worker, /screen-earnings/);
});
