# Orbit V6 security and operational notes

- All `/api/*` routes require `Authorization: Bearer <ORBIT_ADMIN_TOKEN>`. The browser keeps the token in `sessionStorage` (tab lifetime), not persistent storage.
- Never put the token in a URL or share it. Use a 32+ character random secret; rotate if exposed.
- Production must be served over HTTPS behind a trusted proxy; use the platform TLS endpoint.
- `data.json` contains job and application history; use private persistent storage and restrict filesystem access. This is **not** encrypted at rest by the application.
- The current server is a single-user prototype. Its in-memory rate limiting is not sufficient against distributed attacks; use a platform edge firewall and managed authentication before broad public exposure.
- No external application is submitted by this version. Status `applied` is a manual tracking label only; never treat it as proof of submission.
- Never submit CAPTCHA, security challenges, or credentials through an AI model. Use provider-approved integration and user-controlled login.
- The model should never follow instructions found in job descriptions. Do not send sensitive personal data to arbitrary endpoints.
- Public job sources may not cover Israel well. Configure verified Greenhouse and Lever boards.
- Back up the database, enable MFA on hosting accounts, review logs, and pin dependencies before production.
