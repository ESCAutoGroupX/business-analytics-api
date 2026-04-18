-- Migration: add_wickedfile_sync_tables (DOWN)
--
-- Reverses add_wickedfile_sync_tables.up.sql. Drops the four new WickedFile
-- sync tables and the 13 wf_* columns added to documents. Idempotent —
-- safe to run even if the UP migration was only partially applied.
--
-- Column drops target ONLY the columns this migration added. Any other
-- wf_* columns pre-existing on documents (e.g. wf_scan_id, wf_folder_path,
-- wf_keywords, wf_category, wf_id) are left intact.

BEGIN;

-- Drop tables in dependency order. IF EXISTS + CASCADE so a partial state
-- doesn't block rollback. wf_part_match_results FKs into wf_part_audits —
-- CASCADE covers that even without explicit ordering.

DROP TABLE IF EXISTS wf_part_match_results CASCADE;
DROP TABLE IF EXISTS wf_part_audits        CASCADE;
DROP TABLE IF EXISTS wf_match_results      CASCADE;
DROP TABLE IF EXISTS mongo_sync_state      CASCADE;

-- Reverse the documents table columns added in 2B. Indexes are dropped
-- automatically by DROP COLUMN.
ALTER TABLE documents DROP COLUMN IF EXISTS wf_scan_page_id;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_location_id;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_s3_key;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_ocr_agent_version;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_ocr_confidence;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_ml_parsed;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_ml_reviewed;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_invoice_number;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_po_number;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_subtotal;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_tax;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_line_item_count;
ALTER TABLE documents DROP COLUMN IF EXISTS wf_synced_at;

COMMIT;
