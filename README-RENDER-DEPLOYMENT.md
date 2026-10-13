# Kitty WorkTimer — Render Backend Deployment Guide

## How to Update the Existing Render Service
1. Connect to your Render dashboard at `https://dashboard.render.com`.
2. Locate service `worker-timer-monitor-yqg3`.
3. Push the files in this archive to your linked Git repository or deploy via manual deploy zip.
4. Verify environment variable `NODE_ENV=production`.
5. The server will initialize the persistent database `kitty-database.json` and handle dynamic Device ID registrations automatically.
