-- =============================================================================
-- Task 5: Sampling Accuracy Study
-- =============================================================================
-- Compares TABLESAMPLE accuracy at 10/50/100 rows
--
-- Findings:
--   10 rows  — high variance, syntax check only
--   50 rows  — identifies top regions
--   100 rows — acceptable for demographics
--
-- Stack: Apache Hive 2.1.1, HiveQL, TABLESAMPLE
-- =============================================================================

USE chupahinar;

SET hive.exec.parallel=True;
SET hive.exec.parallel.thread.number=8;
SET mapreduce.job.reduces=30;

-- 10 ROWS (high variance)
SELECT 
    i.region,
    SUM(IF(u.sex = 'male', 1, 0)) AS male,
    SUM(IF(u.sex = 'female', 1, 0)) AS female
FROM logs TABLESAMPLE (10 ROWS) l
JOIN ipregions i ON i.ip = l.ip
JOIN users u ON u.ip = l.ip
GROUP BY i.region
ORDER BY male DESC
LIMIT 5;

-- 50 ROWS (medium variance)
SELECT 
    i.region,
    SUM(IF(u.sex = 'male', 1, 0)) AS male,
    SUM(IF(u.sex = 'female', 1, 0)) AS female
FROM logs TABLESAMPLE (50 ROWS) l
JOIN ipregions i ON i.ip = l.ip
JOIN users u ON u.ip = l.ip
GROUP BY i.region
ORDER BY male DESC
LIMIT 5;

-- 100 ROWS (lower variance)
SELECT 
    i.region,
    SUM(IF(u.sex = 'male', 1, 0)) AS male,
    SUM(IF(u.sex = 'female', 1, 0)) AS female
FROM logs TABLESAMPLE (100 ROWS) l
JOIN ipregions i ON i.ip = l.ip
JOIN users u ON u.ip = l.ip
GROUP BY i.region
ORDER BY male DESC
LIMIT 5;
