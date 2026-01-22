-- =============================================================================
-- Task 4: Domain Migration (Hive Streaming)
-- =============================================================================
-- Business context:
--   In 2024, the media holding migrated regional news sites from country-specific
--   domains (.ru, .ua, .by) to a unified .com domain for international audience.
--   Historical logs need URL transformation to match current domain structure
--   for consistent analytics and cross-period traffic comparison.
--
-- Transforms URLs: replaces TLDs with .com via external sed process
--
-- Regex: s/\.[a-z]{2,6}\//\.com\//
--   Matches: .ru/, .ua/, .com/, .info/ etc.
--   Replaces with: .com/
--
-- Stack: Apache Hive 2.1.1, Hive Streaming, GNU sed 4.2.2
-- =============================================================================

USE chupahinar;

SELECT TRANSFORM (ip, date_time, url, response_size, url_code, client_app)
USING 'sed -E "s/\.[a-z]{2,6}\//\.com\//"'
AS (ip STRING, date_time STRING, url STRING, response_size INT, url_code INT, client_app STRING)
FROM logs
LIMIT 10;
