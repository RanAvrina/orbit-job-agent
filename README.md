# Orbit V6 — private job discovery prototype

## What's implemented
- Responsive Hebrew dashboard and job scoring for junior PMO, project coordination, information systems, ERP/CRM, data and AI roles.
- Job source adapters: Arbeitnow, configured Greenhouse boards, configured Lever sites.
- 30-minute scans when the server process is running, job deduplication, retention cap.
- API authorization on **all** endpoints, secure headers, basic rate limiting and safer token comparison.
- Manual application tracking only. **No automatic applications, no live cloud deployment, no guarantee of Israeli job coverage.**

## Run locally
1. Install Node.js 20+ and set `ORBIT_ADMIN_TOKEN` to a strong random secret.
2. Optionally set `GREENHOUSE_BOARDS` and `LEVER_SITES` to verified public career-board identifiers.
3. Run `node server.mjs` from this folder. Open `http://localhost:3000` and enter the secret in the dashboard.
4. Run `NODE_ENV=test node --test security.test.mjs` to test API access control.

For cloud deployment, use a private persistent disk for `ORBIT_DATA_FILE` and HTTPS. **Do not deploy without reading SECURITY.md.**
