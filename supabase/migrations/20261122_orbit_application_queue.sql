-- Orbit dry-run application queue. Run in Supabase SQL Editor before enabling cloud persistence.
create table if not exists public.orbit_application_queue (
  id bigint generated always as identity primary key,
  external_id text not null unique,
  source text not null,
  title text not null,
  company text,
  match_score integer not null check (match_score between 65 and 100),
  status text not null default 'pending_review' check (status in ('pending_review','approved_for_testing','rejected')),
  reason text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
alter table public.orbit_application_queue enable row level security;
-- No public policies. Server uses Supabase secret key only.
create index if not exists orbit_application_queue_status_idx on public.orbit_application_queue(status,created_at desc);
