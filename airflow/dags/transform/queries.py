CREATE_STAGING_TABLE_JOB = "CREATE TABLE IF NOT EXISTS staging.job_info (\
        id String, title String, company String, city String, \
        url String, created_date String, insert_time Timestamp) \
        USING hive;"
CREATE_STAGING_TABLE_JOB_SKILL = "CREATE TABLE IF NOT EXISTS staging.job_skill_info ( \
        id String, skill String, insert_time Timestamp) \
        USING hive;"
CREATE_PUBLIC_TABLE_JOB = "CREATE TABLE IF NOT EXISTS public.job_info ( \
        id String, title String, company String, city String, url String, insert_time Timestamp) \
        USING hive \
        PARTITIONED BY (created_date STRING);"
INSERT_PUBLIC_TABLE_JOB = "INSERT INTO public.job_info \
        SELECT sub.id, sub.title, sub.company, sub.city, sub.url, sub.insert_time, sub.created_date FROM staging.job_info AS sub \
        LEFT OUTER JOIN public.job_info AS pub ON sub.id = pub.id \
	    WHERE pub.id is NULL;"
CREATE_PUBLIC_TABLE_JOB_SKILL = "CREATE TABLE IF NOT EXISTS public.job_skill_info ( \
        id String, skill String, insert_time Timestamp) \
        USING hive \
        PARTITIONED BY (created_date STRING);"

INSERT_PUBLIC_TABLE_JOB_SKILL = "INSERT INTO public.job_skill_info \
        SELECT sub.id, sub.skill, sub.insert_time, sub.created_date FROM staging.job_skill_info AS sub \
        LEFT OUTER JOIN public.job_skill_info AS pub ON sub.id = pub.id \
	    WHERE pub.id is NULL;"
