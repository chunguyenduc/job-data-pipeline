CREATE TABLE IF NOT EXISTS public.job_info (
    id String, title String, company String, city String, url String, insert_at Timestamp) 
    USING hive
    PARTITIONED BY (created_date STRING);


INSERT INTO TABLE public.job_info PARTITION(created_date)
    SELECT id, title, company, city, insert_at, created_date
    FROM staging.job_info
