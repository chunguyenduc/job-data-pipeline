create_stg_schema = "CREATE SCHEMA IF NOT EXISTS staging"

drop_stg_job_table = "DROP TABLE IF EXISTS staging.job_info;"
drop_stg_job_skill_table = "DROP TABLE IF EXISTS staging.job_skill;"

create_stg_job_table = """
                CREATE TABLE IF NOT EXISTS staging.job_info(
                id varchar(200), 
                title varchar(300), 
                company varchar(200), 
                city varchar(100), 
                url varchar(400), 
                created_date varchar(30),
                created_time datetime default sysdate,
                insert_time datetime default sysdate
        );
        """

create_stg_job_skill_table = """
                CREATE TABLE IF NOT EXISTS staging.job_skill(
                id varchar(200), 
                skill varchar(100), 
                created_date varchar(30), 
                created_time datetime default sysdate,
                insert_time datetime default sysdate
        )
        """

create_public_schema = "CREATE SCHEMA IF NOT EXISTS public"

create_public_job_table = """
                CREATE TABLE IF NOT EXISTS public.job_info(
                 id varchar(200), 
                title varchar(300), 
                company varchar(200), 
                city varchar(100), 
                url varchar(400), 
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
