CREATE TABLE IF NOT EXISTS staging.job_info (
    id String, title String, company String, city String, url String, created_date String) 
    USING hive;

CREATE TABLE IF NOT EXISTS staging.job_skill_info (
    id String, skill String) 
    USING hive;