-- =============================================================================
-- Task 1: Data Schema Definition
-- =============================================================================
-- Creates 4-table schema for news portal log analytics
--
-- Tables:
--   staging_logs → logs (partitioned)
--   users, ip_regions, subnets
--
-- Stack: Apache Hive 2.1.1, HiveQL, RegexSerDe
-- =============================================================================

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=500;

USE chupahinar;

-- -----------------------------------------------------------------------------
-- staging_logs: Raw log parsing via RegexSerDe
-- -----------------------------------------------------------------------------
-- Regex groups:
--   1: IP (IPv4)
--   2: Date (YYYYMMDD)
--   3: URL
--   4: Response size
--   5: HTTP status
--   6: User agent

DROP TABLE IF EXISTS staging_logs;

CREATE EXTERNAL TABLE staging_logs (
    ip STRING,           
    date_time STRING,    
    url STRING,          
    response_size INT,
    url_code INT,
    client_app STRING 
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "^((?:\\d{1,3}\\.){3}\\d{1,3})\\s+(\\d{8})\\d+\\s+(http://[\\w./]+)\\s+(\\d+)\\s+(\\d+)\\s+(.*)$"
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_logs_M';

-- -----------------------------------------------------------------------------
-- Users: Demographics dimension
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS Users;

CREATE EXTERNAL TABLE Users (
    ip STRING,           
    browser STRING,    
    sex STRING,          
    age INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "^((?:\\d{1,3}\\.){3}\\d{1,3})\\s+([\\w/.]+)\\s+(\\w+)\\s+(\\d+)$"
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/user_data_M';

SELECT * FROM users LIMIT 10;

-- -----------------------------------------------------------------------------
-- logs: Partitioned fact table (116 partitions by date)
-- -----------------------------------------------------------------------------

SET hive.exec.dynamic.partition.mode=nonstrict;

DROP TABLE IF EXISTS logs;

CREATE EXTERNAL TABLE logs (
    ip STRING,
    url STRING,
    response_size INT,
    url_code INT,
    client_app STRING
)
PARTITIONED BY (date_time STRING)
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE logs PARTITION (date_time)
SELECT ip, url, response_size, url_code, client_app, date_time
FROM staging_logs;

SELECT * FROM logs LIMIT 10;

-- -----------------------------------------------------------------------------
-- Subnets: Network reference
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS Subnets;

CREATE EXTERNAL TABLE Subnets (
    ip STRING,
    mask STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/data/subnets/variant1';

SELECT * FROM Subnets LIMIT 10;

-- -----------------------------------------------------------------------------
-- IPRegions: IP-to-region mapping
-- -----------------------------------------------------------------------------

DROP TABLE IF EXISTS IPRegions;

CREATE EXTERNAL TABLE IPRegions (
    ip STRING,           
    region STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "^((?:\\d{1,3}\\.){3}\\d{1,3})\\s+(.*)$"
)
STORED AS TEXTFILE
LOCATION '/data/user_logs/ip_data_M';

SELECT * FROM users LIMIT 10;
