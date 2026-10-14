# Kitty Work Timer — Clean Start Edition

This is the updated responsive admin and worker project. Desktop and Android browsers use the same responsive pages (`public/admin.html`).

## Clean reset (requested)
On the **first server launch** with this release, previous worker accounts, device registrations, shifts, tasks, chats, persisted state, and HTTP sessions are removed. No Worker 1/2/3 preset accounts exist. Newly registered devices remain Pending until approved by the admin. The reset marker `data/.fresh-registration-reset-v1` prevents wiping newly created records on subsequent starts. **Back up old server data first if desired; this reset is destructive.**

The actual live Render deployment is NOT modified until you deploy this new project. If the host's persistent disk is mounted at a different directory, confirm `data/` is the persistent location used by the running app.

## Deployment
Copy the project contents into your repository. Set `SESSION_SECRET`, `ADMIN_USERNAME` and `ADMIN_PASSWORD` as Render environment variables. Run `npm install`, then `npm start`. Deploy the whole project, not just an HTML file. Verify the new page and device approval routes after deployment. Camera use requires HTTPS and user-granted permission.
