CREATE TABLE IF NOT EXISTS staging.job_info (
    id String, title String, company String, city String, url String, created_date String) 
    -- PARTITIONED BY (created_date STRING)
    USING hive;