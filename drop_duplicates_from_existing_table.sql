-- Stop existing tasks
ALTER TASK SERVER_INSTALL_EVENT_CHANGES SUSPEND;
ALTER TASK REFRESH_EVENTS_STREAM SUSPEND;
ALTER TASK DROP_DUPLICATES_FROM_JSON_RAW SUSPEND;

USE SCHEMA raw_data;


-- drop duplicates from json_raw table
INSERT OVERWRITE INTO ITRA_DEMO.raw_data.json_raw
(SELECT DISTINCT * FROM ITRA_DEMO.raw_data.json_raw);


-- Check for duplicates
SELECT * FROM ITRA_DEMO.raw_data.json_raw; -- 5,300,014
SELECT DISTINCT RAW_FILE FROM ITRA_DEMO.raw_data.json_raw; -- 5,300,014


-- Recreate stream object
CREATE OR REPLACE STREAM pipes.events_stream 
ON TABLE ITRA_DEMO.raw_data.json_raw
APPEND_ONLY=TRUE;