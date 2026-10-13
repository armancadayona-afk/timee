# Kitty WorkTimer Device ID Update — TEST BUILD

This package modifies the uploaded Node.js server and HTML dashboards. It is **not** an Android APK.

## Install locally
1. Back up your current server and its `data/` directory.
2. Install Node.js 18+; run `npm install` in this directory.
3. Set `ADMIN_USERNAME`, `ADMIN_PASSWORD`, `SESSION_SECRET`, and `NODE_ENV` in your environment. Use strong unique secrets.
4. Run `npm start`. Visit `/` for admin login and `/worker.html` to request device approval.
5. In `/admin.html`, approve the device and assign a worker name. In worker page click Check Status.

## Important limitations
- Existing legacy worker states remain for historical compatibility; fixed worker credentials no longer work at `/api/login`.
- Device secrets and bearer tokens are kept in browser localStorage in this web build. A native Android APK should use Android Keystore and app-specific secure storage instead.
- The app requires explicit camera/mic permissions and displays active capture status. No camera preview appears on worker dashboard.
- The server persists `data/devices.json`, but Render's ephemeral filesystem may erase it on redeploy or restart. Configure a persistent disk or durable database before production.
- This is a development integration and has not been end-to-end tested against your live Render service.
- Server-side revocation is checked on new authenticated requests and socket connections; immediate termination of already connected sessions requires further hardening before production.
- Existing admin task/chat selectors still contain worker1/worker2/worker3 and need dynamic worker population for newly approved devices.
- Check Android WebView camera/microphone behavior separately; a webpage cannot guarantee background capture.


## Connectivity duration update
- Worker dashboard now displays a compact HH:MM:SS timer below the work timer.
- Admin cards display each worker's continuous Socket.IO connection duration.
- Server starts the timer on worker Socket.IO connection, clears it on disconnect, and resets on reconnect.
- The counter is **connection uptime**, not work hours. It does not imply camera or microphone streaming.
- Connection status is based on Socket.IO; Android can suspend WebView in the background. This package does not add a native Android foreground service or guarantee 24/7 uptime.
- The counter is reset after a server restart or socket reconnection.
- Run npm install, npm test, and test on a staging server before deployment.


## Dynamic approved workers update
- Removed fixed John/Mark/Anne accounts from active login and dashboard.
- Admin task and chat dropdowns now use approved Device ID workers dynamically.
- Revoked workers are removed from the live dashboard and their active socket is disconnected.
- Existing historical work sessions and chat records are not deleted. Back up your DATA_DIR before deployment.
- This update is for the web server/dashboard; it is not an Android APK.
- Change default ADMIN_USERNAME and ADMIN_PASSWORD via Render environment variables.
- Requires persistent DATA_DIR on Render for registration to survive restarts.
