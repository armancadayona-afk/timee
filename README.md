# Timee / Worker Timer — rebuilt source

This rebuild keeps the existing dashboard, worker/device views, authentication model,
Socket.IO event names, timer-state implementation, WebRTC signaling and `/api/rtc-config`
flow intact. The visual HTML/CSS and core server were intentionally left unchanged.

## Focused fixes
- Worker Earnings now renders the server-provided earnings/statistics instead of treating
  task titles as earnings. Admin-added earning records and their totals update through the
  existing `tasks-updated` / `earnings-updated` events and refresh on reconnect.
- Worker task/chat views refresh when the authenticated Socket.IO connection reconnects.
- Removed the duplicate `public/server.js` copy from the public static directory. The real
  backend entry point remains the root `server.js`.
- Removed transient `data/http-sessions` cookie/session files from the source package.
  The server recreates this directory automatically at startup.

## Run and test
- Node.js 18+
- `npm install`
- `npm test`
- `npm start`

Configure `SESSION_SECRET`, `ADMIN_USERNAME`, `ADMIN_PASSWORD`, and worker credentials
through environment variables before deploying. Change the development default credentials.
Camera/microphone capture still requires browser permission and a secure context (HTTPS)
outside localhost. This source-level review does not replace testing on the actual worker
devices and deployment environment.
