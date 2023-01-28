"""
"""
create_stg_schema = "CREATE SCHEMA IF NOT EXISTS staging"

drop_stg_job_table = "DROP TABLE IF EXISTS staging.job_info;"
drop_stg_job_skill_table = "DROP TABLE IF EXISTS staging.job_skill;"

create_stg_job_table = "CREATE TABLE IF NOT EXISTS staging.job_info (LIKE public.job_info INCLUDING DEFAULTS);"
create_stg_job_skill_table = "CREATE TABLE IF NOT EXISTS staging.job_skill(LIKE public.job_skill INCLUDING DEFAULTS);"

create_public_schema = "CREATE SCHEMA IF NOT EXISTS public"

insert_job = """
             COPY staging.job_info (id, title, company, city, url, created_date, created_time)
            FROM 's3://{{ params.bucket}}/job/job-{{ task_instance.xcom_pull(task_ids='extract_job_data', key='return_value') }}.csv'
            REGION '{{ params.region }}' IAM_ROLE '{{ params.iam_role }}' 
            DELIMITER ',' 
            REMOVEQUOTES
            IGNOREHEADER 1;
        """

insert_job_skill = """
            COPY staging.job_skill (id, skill, created_date, created_time)
        FROM 's3://{{ params.bucket }}/job_skill/job_skill-{{ task_instance.xcom_pull(task_ids='extract_job_data', key='return_value') }}.csv'
            REGION '{{ params.region }}' IAM_ROLE '{{ params.iam_role }}' 
            DELIMITER ',' 
            IGNOREHEADER 1;
        """


create_public_job_table = """
                CREATE TABLE IF NOT EXISTS public.job_info(
                 id varchar(200), 
                title varchar(300), 
                company varchar(200), 
                city varchar(100), 
                url varchar(500), 
                created_date varchar(30),
                created_time datetime default sysdate,
                insert_time datetime default sysdate
                );
        """

create_public_job_skill_table = """
                CREATE TABLE IF NOT EXISTS public.job_skill(
                id varchar(200), 
                skill varchar(100), 
                created_date varchar(30), 
                created_time datetime default sysdate,
                insert_time datetime default sysdate
                )
        """
insert_public_job = """
        INSERT INTO public.job_info (id, title, company, city, url, created_date, created_time, insert_time) 
        SELECT sub.id, sub.title, sub.company, sub.city, sub.url, sub.created_date, sub.created_time, sub.insert_time FROM staging.job_info AS sub 
        LEFT OUTER JOIN public.job_info AS pub ON sub.id = pub.id 
        WHERE pub.id is NULL;
"""

insert_public_job_skill = """
        INSERT INTO public.job_skill (id, skill, insert_time, created_time, created_date)
        SELECT sub.id, sub.skill, sub.insert_time, sub.created_time, sub.created_date FROM staging.job_skill AS sub 
        LEFT OUTER JOIN public.job_skill AS pub ON sub.id = pub.id 
        WHERE pub.id is NULL;
"""

sql_check_job = "SELECT count(id) from staging.job_info"
sql_check_skill = "SELECT count(id) from staging.job_skill"
