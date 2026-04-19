-- Migration: add_wf_transaction_matching (DOWN)
--
-- Reverses add_wf_transaction_matching.up.sql. Drops the three matcher
-- tables with CASCADE so FK references from wf_transaction_matches to
-- wf_vendor_aliases go with them.
--
-- Idempotent (IF EXISTS on every DROP).

BEGIN;

DROP TABLE IF EXISTS wf_transaction_matches CASCADE;
DROP TABLE IF EXISTS wf_matcher_runs        CASCADE;
DROP TABLE IF EXISTS wf_vendor_aliases         CASCADE;

COMMIT;
