# Kitty WorkTimer Android apps

This project builds four separately installable Android WebView apps against the existing Kitty WorkTimer HTTPS server. They use its existing Express pages, REST APIs, session cookie, Socket.IO connection, and existing WebRTC signaling. No replacement signaling server is introduced.

## App IDs

- Admin: `com.kittyworktimer.admin`
- Worker 1: `com.kittyworktimer.worker1`
- Worker 2: `com.kittyworktimer.worker2`
- Worker 3: `com.kittyworktimer.worker3`

## Build

Requires JDK 17 and Android SDK 35. Configure the public HTTPS server URL using `SERVER_BASE_URL` or Gradle property `-PserverBaseUrl=https://your-host.example`.

```bash
cd android
gradle assembleAdminDebug assembleWorker1Debug assembleWorker2Debug assembleWorker3Debug -PserverBaseUrl=https://your-host.example
```

APKs are generated under `app/build/outputs/apk/<flavor>/debug/`. For release distribution, configure signing securely in your release pipeline. GitHub Actions publishes the debug APKs as downloadable workflow artifacts.

## Login and assigned workers

Each app has a fixed app ID and its assigned username is defined by flavor. The password is **not** embedded in the APK. Sign in once with the assigned server account; Android WebView retains the HTTPS session cookie for later launches. The existing server explicitly disables public device approval, so truly zero-touch first-launch authentication requires a server-side one-time device enrollment/revocable-token feature; hardcoding passwords into APKs would expose account credentials.

## Camera and WebRTC

The wrapper requests Android camera and microphone permissions when the existing site requests capture. It continues using the server's current Socket.IO/WebRTC flow and ICE configuration. Camera support depends on the target device's Android System WebView. Native Android screen sharing is not included; that requires Android MediaProjection and explicit system consent.

## Deployment

- Set a valid HTTPS URL before building; the placeholder URL is intentionally rejected at runtime.
- A valid TLS certificate and network access to the existing server are required.
- These apps wrap the existing dashboards and do not duplicate business logic.
