-- =============================================================================
-- Task 3: Regional Demographics
-- =============================================================================
-- Gender breakdown by region (3-way JOIN + conditional aggregation)
--
-- Optimizations:
--   - hive.exec.parallel: parallel stage execution
--   - mapreduce.job.reduces=82: tuned for data volume
--   - auto.convert.join=False: tables too large for map-side join
--
-- Stack: Apache Hive 2.1.1, HiveQL, MapReduce 2.7
-- =============================================================================

USE chupahinar;

SET hive.exec.parallel=True;
SET hive.exec.parallel.thread.number=8;
SET hive.auto.convert.join=False;
SET mapreduce.job.reduces=82;

SELECT 
    i.region,
    SUM(IF(u.sex = 'male', 1, 0)) AS male,
    SUM(IF(u.sex = 'female', 1, 0)) AS female
FROM logs l
JOIN ipregions i ON i.ip = l.ip
JOIN users u ON u.ip = l.ip
GROUP BY i.region;
