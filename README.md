# Kitty WorkTimer — Device Registration & Administrator Approval System

A complete, secure, and persistent **Device Registration & Administrator Approval System** for the Kitty WorkTimer Android application and Node.js server.

---

## 🌸 Overview & Architecture

1. **Worker App Authentication**:
   - Replaces hardcoded worker accounts (`worker1`, `worker2`, `worker3`) with a unique **Device ID** generated persistently using `UUID.randomUUID()` in app-private storage.
   - Generates an asymmetric RSA 2048-bit key pair inside **Android Keystore**. Sends public key to the server upon registration and signs authentication tokens using the non-exportable private key.
   - **No Worker Passwords Required**: Approved devices automatically authenticate with the server using key-signed headers (`X-Device-Id`, `X-Device-Timestamp`, `X-Device-Signature`).

2. **Hello Kitty Registration Screen**:
   - Displays read-only **Device ID**, **Copy Device ID** button, **Register Device** button, and real-time **Status Badge**.
   - Handles states: `Not Registered`, `Pending Approval`, `Approved`, `Rejected`, `Revoked`, `Connection Error`.
   - Polls server status every ~10s and automatically unlocks the **Worker Dashboard** upon Administrator approval.

3. **Administrator Approval Console**:
   - Features a dedicated **Device Registration Approvals** section on the Admin Console (`admin.html`).
   - Displays Device ID, Device Name, Registration Date, Status, Assigned Worker Name.
   - Actions: **Approve**, **Reject**, **Rename Worker**, **Revoke Access**.
   - Admin access requires authenticated Administrator username and password (`/api/login`).

4. **Hello Kitty Worker Dashboard**:
   - Large work timer (`00:00:00`) with Start / Pause / Resume controls.
   - Select Task dropdown menu.
   - Messages & SMS Manager interface.
   - Connection Status & small connection duration timer (`HH:MM:SS`) measuring active server connection duration.
   - **No camera preview or controls** displayed on the Worker Dashboard.

5. **Native Foreground Service**:
   - `ConnectionService.kt` (`foregroundServiceType="dataSync"`) maintains server connection with a persistent notification (`"Kitty WorkTimer Active 💕"`).
   - Monitors device status and handles revocation immediately by stopping protected functions and returning to the registration screen.

---

## 📡 API Contract

- **Server Base URL**: `https://worker-timer-monitor-yqg3.onrender.com/`

### Device Endpoints
- `POST /api/devices/register`: Registers a new device request with `deviceId`, `deviceName`, `platform`, `appVersion`, and `publicKey`.
- `GET /api/devices/status`: Returns current status (`pending`, `approved`, `rejected`, `revoked`), assigned `workerId`, and `workerName`. Requires `X-Device-Id` header.

### Administrator Endpoints (Require Admin Authentication)
- `GET /api/admin/devices`: Lists all registered devices.
- `POST /api/admin/devices/:deviceId/approve`: Approves device and assigns worker display name.
- `POST /api/admin/devices/:deviceId/reject`: Rejects device registration.
- `POST /api/admin/devices/:deviceId/revoke`: Revokes device access immediately and disconnects sessions.
- `PATCH /api/admin/devices/:deviceId`: Renames worker or updates device metadata.

---

## 📦 Deliverables & Build Output

- **Worker APK**: `app/build/outputs/apk/worker/debug/app-worker-debug.apk`
  - Full Path: `C:\Users\kuyako\AndroidStudioProjects\timee\app\build\outputs\apk\worker\debug\app-worker-debug.apk`
- **Admin APK**: `app/build/outputs/apk/admin/debug/app-admin-debug.apk`
  - Full Path: `C:\Users\kuyako\AndroidStudioProjects\timee\app\build\outputs\apk\admin\debug\app-admin-debug.apk`

---

## 🔨 How to Build APKs

### Command Line (PowerShell):
```powershell
$env:JAVA_HOME = "C:\Program Files\Android\openjdk\jdk-21.0.8"
$env:ANDROID_HOME = "C:\Users\kuyako\AppData\Local\Android\Sdk"
.\gradlew.bat assembleDebug
```
