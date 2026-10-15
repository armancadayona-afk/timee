'use strict';
// Run once when intentionally starting with a clean workspace: npm run reset-data
// This removes user-generated records and sessions, but keeps source code/configuration.
const fs = require('fs');
const path = require('path');
const dataDir = path.join(__dirname, 'data');
const files = [
  'state.json', 'sessions.json', 'tasks.json', 'chat.json',
  'earnings.json', 'users.json', 'device_requests.json'
];
for (const name of files) {
  const target = path.join(dataDir, name);
  if (fs.existsSync(target)) fs.rmSync(target, { force: true });
}
const sessionsDir = path.join(dataDir, 'http-sessions');
if (fs.existsSync(sessionsDir)) {
  for (const name of fs.readdirSync(sessionsDir)) {
    fs.rmSync(path.join(sessionsDir, name), { recursive: true, force: true });
  }
}
console.log('Application data cleared. Fixed account configuration and source files were preserved.');
