-- Stop existing tasks
ALTER TASK FIND_DUPLICATES SUSPEND;
ALTER TASK DROP_DUPLCATED_DATA SUSPEND;
ALTER TASK INSERT_SINGLE_COPY SUSPEND;
ALTER TASK SERVER_INSTALL_EVENT_CHANGES SUSPEND;
ALTER TASK TRUNCATE_DUPLICATES_TABLE SUSPEND;

USE SCHEMA raw_data;


-- find all duplicates
CREATE OR REPLACE TRANSIENT TABLE raw_data.duplicates AS (
    SELECT RAW_FILE
    FROM raw_data.json_raw
    GROUP BY 1
    HAVING COUNT(*) > 1
);

DELETE FROM raw_data.json_raw AS a
USING raw_data.duplicates AS b
where (a.$1)=(b.$1);

-- insert single copy
INSERT INTO raw_data.json_raw
SELECT * 
FROM raw_data.duplicates;

TRUNCATE TABLE raw_data.duplicates;


-- Check for duplicates
SELECT * FROM ITRA_DEMO.raw_data.json_raw; -- 5,300,014
SELECT DISTINCT RAW_FILE FROM ITRA_DEMO.raw_data.json_raw; -- 5,300,014


-- Recreate stream object
CREATE OR REPLACE STREAM pipes.events_stream 
ON TABLE ITRA_DEMO.raw_data.json_raw
APPEND_ONLY=TRUE;

DROP STREAM raw_data.events_stream;