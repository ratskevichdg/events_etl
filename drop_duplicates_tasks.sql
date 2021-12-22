USE DATABASE ITRA_DEMO;
USE SCHEMA PIPES;

SELECT * FROM raw_data.duplicates;

-- Create task which find duplicates and put their into transient table
CREATE OR REPLACE TASK find_duplicates
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '3 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('EVENTS_STREAM')
    AS
INSERT INTO raw_data.duplicates
    (SELECT raw_file
    FROM raw_data.json_raw
    GROUP BY 1
    HAVING COUNT(*) > 1);
    

-- Create task which removes all data with duplicates from json_raw table
CREATE OR REPLACE TASK drop_duplcated_data
    WAREHOUSE = COMPUTE_WH
    AFTER find_duplicates
    AS
DELETE FROM raw_data.json_raw AS a
USING raw_data.duplicates AS b
WHERE (a.raw_file)=(b.raw_file);


-- Insert into json_raw table a single copy of duplicated data
CREATE OR REPLACE TASK insert_single_copy
    WAREHOUSE = COMPUTE_WH
    AFTER drop_duplcated_data
    AS
INSERT INTO raw_data.json_raw
SELECT * 
FROM raw_data.duplicates;


-- Truncate transient table
CREATE OR REPLACE TASK truncate_duplicates_table
    WAREHOUSE = COMPUTE_WH
    AFTER server_install_event_changes
    AS
TRUNCATE TABLE raw_data.duplicates;


ALTER TASK TRUNCATE_DUPLICATES_TABLE RESUME;
ALTER TASK SERVER_INSTALL_EVENT_CHANGES RESUME;
ALTER TASK INSERT_SINGLE_COPY RESUME;
ALTER TASK DROP_DUPLCATED_DATA RESUME;
ALTER TASK FIND_DUPLICATES RESUME;