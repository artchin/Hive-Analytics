-- =============================================================================
-- Task 2: Daily Traffic Report
-- =============================================================================
-- Aggregates visits by date for advertiser reporting
--
-- Stack: Apache Hive 2.1.1, HiveQL
-- =============================================================================

USE chupahinar;

SELECT date_time, COUNT(1) AS visits 
FROM logs
GROUP BY date_time 
ORDER BY visits DESC;
