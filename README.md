# Timee Worker Monitor

Standalone browser-based worker timer and monitoring app. It does not require an Android app; workers can sign in from a supported browser. Camera/microphone capture requires explicit browser permission and HTTPS (or localhost).

## Run locally

1. Install Node.js 18 or newer.
2. Run `npm install`.
3. Run `npm start`.
4. Open `http://localhost:3000` (or the port configured by the server).

## Preserved functionality

- Admin Dashboard, Workers, Devices, remote diagnostic commands, timer controls, and WebRTC camera/audio monitoring.
- Four independent monitor slots with Connect, Disconnect, camera switching, audio, recording, zoom, and fullscreen controls.
- Worker browser timer and WebRTC signaling.
- Admin timer reset closes the current timer session and resets elapsed time without intentionally disconnecting the live camera feed.

## Display names

The four worker display labels are Worker 1 through Worker 4. Login usernames/passwords are unchanged and remain configurable with `WORKER1_USERNAME`, `WORKER1_PASSWORD`, etc. Default local credentials are: admin `admin` / `admin123`; Worker 1 `john` / `john123`; Worker 2 `mark` / `mark123`; Worker 3 `anne` / `anne123`; Worker 4 `thing` / `thing123`. Change these credentials through environment variables before public deployment.

## Design-only sections

Tasks, Chat, and Earning tabs remain as visual layouts only. Their UI actions and server endpoints are removed from this build. These sections do not create tasks, send messages, or calculate/store earnings.

## Tests

Run `npm test` for the included automated authentication, timer, and WebRTC-state protocol tests. Run `npm run check` for a server syntax check. These tests do not replace a manual browser/device camera test.
