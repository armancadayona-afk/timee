const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const root = path.join(__dirname, '..');

test('admin has Task, Chat, and Earning views plus monitor zoom/fullscreen and reset controls', () => {
  const html = fs.readFileSync(path.join(root, 'public/admin.html'), 'utf8');
  for (const tab of ['tasks', 'chat', 'reports']) assert.match(html, new RegExp(`data-view="${tab}"`));
  assert.match(html, /data-action="zoom"/);
  assert.match(html, /data-action="fullscreen"/);
  assert.match(html, /data-action="timer-reset"/);
  assert.match(html, /id="btn-timer-reset"/);
});

test('task and earning APIs exist on server and worker navigation remains intact', () => {
  const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  const worker = fs.readFileSync(path.join(root, 'public/worker.html'), 'utf8');
  assert.match(server, /\/api\/tasks/);
  assert.match(server, /\/api\/earnings\/my/);
  assert.match(worker, /worker-navigation\.js/);
  assert.match(worker, /screen-tasks/);
  assert.match(worker, /screen-earnings/);
});
