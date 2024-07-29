Assgn3

Secret key - vqOHf6RcWxUDyXXJg531Vr9xwbrv9DH0HJpDAO5d
Access key - AKIAYS2NVCWBA22MLUZE

USE database SNOW2;

--Create external stage
CREATE OR REPLACE STAGE assgn_stg
URL='s3://assgn3snowflake/Assgn/'
CREDENTIALS=(AWS_KEY_ID='AKIAYS2NVCWBA22MLUZE' AWS_SECRET_KEY='vqOHf6RcWxUDyXXJg531Vr9xwbrv9DH0HJpDAO5d');

--d. CREATE table in Snowflake with VARIANT column
CREATE OR REPLACE TABLE PERSON_NESTED (
    person VARIANT
);

--e. Create a Snowpipe with Auto Ingest Enabled
CREATE OR REPLACE PIPE person_pipe AUTO_INGEST = TRUE AS
COPY INTO PERSON_NESTED
FROM (
    SELECT 
    OBJECT_CONSTRUCT(
        'ID', $1,
        'Name', $2,
        'Age', $3,
        'Location', $4,
        'Zip', IFF($5 = '' OR $5 IS NULL, '00000', $5),
        'Filename', METADATA$FILENAME,
        'FileRowNumber', METADATA$FILE_ROW_NUMBER,
        'IngestedTimestamp', TO_TIMESTAMP_NTZ(CURRENT_TIMESTAMP)
    ) AS person
    FROM @assgn_stg
)
ON_ERROR = CONTINUE;

alter pipe person_pipe refresh;

show pipes;
 
--f. Subscribe the Snowflake SQS Queue in s3:
 
--g. Test Snowpipe by copying the sample JSON file and upload the file to s3 in path
 
select system$pipe_status('person_pipe');
 
show pipes;

SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    table_name => 'person_nested',
    start_time => DATEADD('hour', -24, CURRENT_TIMESTAMP())
));

select * from person_nested;

--Change Data Capture using Streams, Tasks and Merge
 
/* .Create Streams on PERSON_NESTED table to capture the 
change data on PERSON_NESTED table and use TASKS to Run 
SQL/Stored Procedure to Unnested the data from 
PERSON_NESTED and create PERSON_MASTER table. */
 
create or replace stream person_stream on table person_nested;
 
show streams;

--create master table


--create task
 
CREATE OR REPLACE TABLE PERSON_MASTER
 
CREATE OR REPLACE TASK PERSON_TASK
  WAREHOUSE = SNOW_WH
  SCHEDULE = '1 MINUTE'
AS
INSERT INTO EMPLOYEES(LOAD_TIME) VALUES(CURRENT_TIMESTAMP);

