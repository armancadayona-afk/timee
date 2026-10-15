# Kitty WorkTimer

A Node.js/Express + Socket.IO worker timer, task management, monitoring, and chat application.

## Run locally

```sh
npm install
npm start
```

Open `http://localhost:3000`.

## Accounts

The three fixed worker accounts are configured in `server.js` and can be overridden with environment variables:

| Worker | Username | Password |
|---|---|---|
| Worker 1 | `worker1` | `bestworker1` |
| Worker 2 | `worker2` | `bestworker2` |
| Worker 3 | `worker3` | `bestworker3` |

Admin defaults are `admin` / `admin123`; set `ADMIN_USERNAME` and `ADMIN_PASSWORD` in the deployment environment before exposing the app publicly. Worker self-registration and device-registration/approval endpoints have been removed.

## Clear existing application data

Run `npm run reset-data` once to remove saved worker state, work sessions, tasks, chat, earnings, legacy user records, and stored login sessions. This does not remove source files or fixed account configuration. On a deployment with a persistent disk, run the command against that deployed app's data directory (or clear the same files on the mounted disk) to remove records from an earlier deployment. Do not run it on every restart because it intentionally deletes records.

## Checks

```sh
npm run check
npm test
```
