# Server source

Deploy contents of this directory to GitHub repository root. The worker APKs use prefilled accounts (`worker1`, `worker2`, `worker3`) with no first-run registration flow. Change all credentials in environment variables. Preserve `data/` across redeploys; otherwise the file-based tasks and earnings history may be lost. Server uses `npm start`.
