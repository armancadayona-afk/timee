# Remote Device Terminal (web diagnostics phase)

This release adds a functioning admin-to-worker **diagnostic request** channel through Socket.IO. Choose a worker, choose one of four predefined diagnostics, and send. The Worker page shows an approval dialog for each request and returns a result. The server verifies admin/worker roles, tracks requests by UUID, enforces a 60-second expiry, caps output length, and maintains an in-memory audit list.

**Not an ADB shell:** This archive contains only Node.js and HTML/browser code, not an Android Studio project. Browser JavaScript cannot execute `adb shell` or Android privileged system commands. The four available diagnostic commands are browser-level: device-info, battery-status, network-status, and app-status. Battery information depends on browser API support.

To support actual ADB on managed Android phones, create a separately packaged native APK component with pairing/authorization on each device, a visible approval workflow, a strict allowlist of permitted operations, authentication, a revoke control, and durable audit logs. Do not execute arbitrary strings received from the server as shell commands.

Run `npm install`, `npm test`, then `npm start`. Admin opens `/admin.html`; worker opens `/worker.html` and must be signed in.
