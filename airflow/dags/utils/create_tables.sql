CREATE SCHEMA IF NOT EXISTS staging;


CREATE TABLE IF NOT EXISTS staging.job_info (
            id varchar(150),
            title varchar(200),
            company varchar(50),
            city varchar(30),
            url varchar(200),
            created_date varchar(20),
            insert_time datetime default sysdate
);

CREATE TABLE IF NOT EXISTS staging.job_skill (
            id varchar(150),
            skill varchar(50),
            created_date varchar(20),
            insert_time datetime default sysdate
);

