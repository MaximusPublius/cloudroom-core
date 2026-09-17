-- Stores ordered Cloudroom records, including receipts and original native text.
-- Apply explicitly to a designated nonproduction database first:
--   psql "$CLOUDROOM_DATABASE_URL" -v ON_ERROR_STOP=1 -f docs/database/0001-session-records.sql
-- Production application requires the database owner's approval; the service never migrates.
-- Verify: SELECT store, session_id, count(*) FROM cloudroom_records GROUP BY 1, 2;
-- The configured store identifies one owner's history. Give the service access only
-- to that owner's database; this first slice is not a shared multi-customer database API.
-- Supabase defaults can grant browser roles privileges that bypass RLS, such as TRUNCATE.
-- Create and restrict access in one transaction, before any core data is written.
BEGIN;
CREATE TABLE public.cloudroom_records (
    store text NOT NULL,
    session_id text NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    record text NOT NULL,
    PRIMARY KEY (store, session_id, sequence)
);
ALTER TABLE public.cloudroom_records ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON public.cloudroom_records FROM PUBLIC;
DO $$
DECLARE browser_role text;
BEGIN
    FOR browser_role IN SELECT rolname FROM pg_catalog.pg_roles
        WHERE rolname IN ('anon', 'authenticated')
    LOOP
        EXECUTE format('REVOKE ALL ON public.cloudroom_records FROM %I', browser_role);
    END LOOP;
END $$;
COMMIT;
