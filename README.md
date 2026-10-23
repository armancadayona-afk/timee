# Timee Worker Monitor & Task Management System

Standalone Node.js backend & web dashboard with real-time WebRTC camera monitoring, functional Task Management System, and Earnings System with 48-Hour Payment Holds.

## 🚀 Run locally

1. Install Node.js 18 or newer.
2. Run `npm install`.
3. Run `npm start`.
4. Open `http://localhost:3000` (or `http://10.0.2.2:3000` for Android emulator).

## 🌟 Task Management & Earnings System

- **Task Creation (Admin Only)**: Create Direct or Open Tasks with Title, Instructions, Deadline, Base Reward ($).
- **Direct Assignment**: Assigned to specific worker (`Worker 1-4`). Worker can Accept or Deny.
- **Open Available Tasks**: Published for any worker to Grab (claimed atomically).
- **Task Submission & Admin Review**: Workers submit completed work with notes; Admin approves with payment amount ($) or requests revision with feedback.
- **48-Hour Payment Hold**: Approved task earnings are held in Pending Balance for exactly 48 hours before automatically shifting to Available Balance for withdrawal.

## 🌐 Deploy to Render

To update your live server at `https://worker-timer-monitor-yqg3.onrender.com/`:
1. Push this `backend/` code to your GitHub repository connected to Render.com.
2. Render will run `npm install` and `npm start` automatically.
3. Once deployed, `https://worker-timer-monitor-yqg3.onrender.com/` will serve the active Task Management & Earnings system.

## 🧪 Tests

Run `npm test` for the included automated task creation, 48h hold earnings, authentication, timer, and WebRTC protocol tests.
