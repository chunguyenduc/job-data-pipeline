create_stg_schema = "CREATE SCHEMA IF NOT EXISTS staging"

drop_stg_job_table = "DROP TABLE IF EXISTS staging.job_info;"
create_stg_job_table = """
                CREATE TABLE IF NOT EXISTS staging.job_info(
                id varchar(150),
                title varchar(200),
                company varchar(50),
                city varchar(30),
                url varchar(200),
                created_date varchar(20),
                insert_time datetime default sysdate
                );
        """

create_stg_job_skill_table = """
                CREATE TABLE IF NOT EXISTS staging.job_skill(
                id varchar(150),
                skill varchar(50),
                created_date varchar(20),
                insert_time datetime default sysdate
                )
        """

create_public_schema = "CREATE SCHEMA IF NOT EXISTS public"

create_public_job_table = """
                CREATE TABLE IF NOT EXISTS public.job_info(
                id varchar(150),
                title varchar(200),
                company varchar(50),
                city varchar(30),
                url varchar(200),
                created_date varchar(20),
                insert_time datetime default sysdate
                );
        """

create_public_job_skill_table = """
                CREATE TABLE IF NOT EXISTS public.job_skill(
                id varchar(150),
                skill varchar(50),
                created_date varchar(20),
                insert_time datetime default sysdate
                )
        """
