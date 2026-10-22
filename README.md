# Timee / Worker Timer — standalone web rebuild

A self-hosted browser application for worker timer sessions, task management, chat, earnings, device status, and authorized live camera monitoring. It runs as a Node.js web app and does not require an Android application.

## What changed

- Reworked the worker-side Task, Open Tasks, Task History, Chat, Earnings, and Profile screens as browser-native interfaces using the existing authenticated API.
- Added an administrator task review filter so submitted work is visible under **Needs Review** and can be approved. Task search and worker filtering include assigned workers.
- Fixed task submission authorization: workers can submit only a task assigned to them, and must accept it before submitting.
- Browser camera/microphone permission is requested when an authenticated administrator starts live monitoring, rather than on initial page load. Existing Socket.IO/WebRTC signaling events, RTC configuration endpoint, and admin monitoring authorization flow are retained. Monitoring stop releases camera/microphone tracks.
- Earnings display uses actual server-provided earning records and totals rather than presenting task titles as payments. Worker task/chat data refreshes on socket reconnect and relevant server events.
- Removed the duplicate backend file from the public static directory and excluded transient session files from the package.

## Run locally

Requirements: Node.js 18 or newer.

```sh
npm install
npm test
npm start
```

Open `http://localhost:3000` (or the port configured by the server). Use the credentials configured for your deployment. Set `SESSION_SECRET`, `ADMIN_USERNAME`, `ADMIN_PASSWORD`, and worker credentials using environment variables; do not deploy with development credentials.

## Browser camera requirements

Live camera/microphone monitoring requires the worker to be signed in to the worker page, the browser to grant camera/microphone permission, and the app to be served over HTTPS (localhost is permitted for local testing). The admin must be authenticated and use the existing Dashboard live-monitor controls. The browser prompts the worker for access when monitoring is requested.

## Verification performed

- `node --check server.js`, `node --check public/worker-features.js`, and `node --check timer-state.js` pass.
- Inline JavaScript in the admin, worker, and login pages passes `node --check`.
- `npm test`: 5 tests pass, covering worker authentication/route protection, camera request timing, and timer pause/resume accounting.

These automated checks do not replace a live two-browser WebRTC test on the final HTTPS deployment.

## Synchronization fixes and verification

This revision fixes two sync defects found during code review:

- Admin task/earnings panels now fetch fresh server data on Socket.IO update events instead of only redrawing stale in-memory arrays.
- Chat messages sent through the Socket.IO path are persisted to `data/chat-messages.json`; chat history compares normalized worker IDs and returns the original worker key used by the admin UI. This prevents messages from disappearing after restart and fixes worker ID/username mismatches in history.

### Checks run for this revision

- `node --check server.js` — passed.
- `node --check public/worker-features.js` — passed.
- Inline JavaScript syntax checks for `public/admin.html` and `public/worker.html` — passed.
- `node --test test/*.test.js` — 9/9 passed (5 existing auth/timer tests and 4 synchronization contract checks).

The synchronization contract tests verify persistence/broadcast wiring and the frontend listeners. A live two-browser end-to-end test was **not** completed in this environment because installing the app dependencies timed out. Run `npm install` and test with separate authenticated admin and worker browser sessions before production deployment.
