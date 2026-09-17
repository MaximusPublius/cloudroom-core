-- Stores operational diagnostics separately from session history, using the existing PostgreSQL database.
-- Apply first to a disposable database; the core never migrates automatically:
--   psql "$CLOUDROOM_DATABASE_URL" -v ON_ERROR_STOP=1 -f docs/database/0002-diagnostics.sql
-- Only the owner may apply this to production. Use the same owner-isolated database/role as 0001.
-- A store label is NOT authorization: never give customer VMs a shared cross-customer database role.
-- RLS without browser policies denies Supabase anon/authenticated reads; use a protected server role.
-- Verify as the owner: SELECT store, record->>'kind', count(*) FROM cloudroom_diagnostics GROUP BY 1,2;
-- Verify browser roles cannot read these rows. See ../observability.md for investigation and retention.
-- Restrict Supabase's default browser grants atomically with table creation.
BEGIN;
CREATE TABLE public.cloudroom_diagnostics (
    store text NOT NULL,
    run_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    timestamp_ms bigint NOT NULL CHECK (timestamp_ms >= 0),
    record jsonb NOT NULL,
    PRIMARY KEY (store, run_id, sequence)
);
CREATE INDEX cloudroom_diagnostics_time ON public.cloudroom_diagnostics (store, timestamp_ms);
ALTER TABLE public.cloudroom_diagnostics ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.cloudroom_diagnostics FROM PUBLIC;
DO $$
DECLARE browser_role text;
BEGIN
    FOR browser_role IN SELECT rolname FROM pg_catalog.pg_roles
        WHERE rolname IN ('anon', 'authenticated')
    LOOP
        EXECUTE format('REVOKE ALL ON public.cloudroom_diagnostics FROM %I', browser_role);
    END LOOP;
END $$;
COMMIT;
