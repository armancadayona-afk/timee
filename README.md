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
