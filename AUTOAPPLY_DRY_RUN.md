# Orbit auto-apply preparation (DRY RUN)
- Match threshold: 65%.
- Real submissions are hard-disabled in server.mjs (APPLY_ENABLED=false).
- Eligible stored jobs enter orbit_application_queue with pending_review.
- No browser automation, account login, CV upload or employer form submission.
- GET /api/application-queue requires the existing bearer admin token.
- Apply SQL migration in Supabase SQL Editor before merge/deploy.
- Default preparation is ON; set ORBIT_PREPARE_ENABLED=false to disable.
- Verify queue after scan and confirm no employer submissions occur.
- All code is in a feature branch and needs review/merge before Render deploys.
