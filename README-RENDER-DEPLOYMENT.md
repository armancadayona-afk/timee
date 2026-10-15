# Kitty WorkTimer — Render Deployment

1. Deploy this project to your existing Render service.
2. Confirm `NODE_ENV=production` and set a strong `SESSION_SECRET`.
3. Change the admin credentials using `ADMIN_USERNAME` and `ADMIN_PASSWORD` before public use.
4. Worker accounts are fixed in `server.js` (or can be overridden with `WORKER1_*`, `WORKER2_*`, and `WORKER3_*` environment variables).
5. Device registration and approval flows have been removed. Workers sign in using their assigned username and password.

## Start with empty records

The app archive contains no seeded tasks, chat messages, work sessions, or earnings. If the existing Render service has a persistent disk from an earlier deployment, old records remain on that disk until explicitly cleared. Run `npm run reset-data` once in the deployed project's environment, or delete the app data JSON files and contents of `data/http-sessions/` on the mounted persistent disk. This action is destructive and should not be configured to run on every restart.
