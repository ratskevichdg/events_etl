CREATE OR REPLACE DATABASE ITRA_DEMO;


-- Create events table
CREATE OR REPLACE SCHEMA events;
CREATE OR REPLACE TABLE events.server_install_events
(
    player_id INT,
    device_id INT,
    install_date TIMESTAMP,
    client_id STRING,
    app_name STRING,
    country STRING
);


-- Create integration object
CREATE OR REPLACE STORAGE INTEGRATION gcp_integration
    type = EXTERNAL_STAGE
    STORAGE_PROVIDER = GCS
    ENABLED = TRUE
    STORAGE_ALLOWED_LOCATIONS = ('gcs://<bucket_name>');
    
DESC STORAGE INTEGRATION gcp_integration;

-- Create file_format
CREATE OR REPLACE SCHEMA ITRA_DEMO.file_formats;
CREATE OR REPLACE file format file_formats.jsonformat_gcp
    TYPE = JSON;
    
-- Create stage object
CREATE OR REPLACE SCHEMA ITRA_DEMO.STAGES;

CREATE OR REPLACE stage ITRA_DEMO.STAGES.stage_gcp_json
    STORAGE_INTEGRATION = gcp_integration
    URL = 'gcs://<bucket_name>/<folder_name>'
    FILE_FORMAT = file_formats.jsonformat_gcp;
    
LIST @ITRA_DEMO.STAGES.stage_gcp_json;

CREATE OR REPLACE SCHEMA RAW_DATA;

CREATE OR REPLACE TABLE ITRA_DEMO.RAW_DATA.JSON_RAW (
    raw_file variant);
    
    
-- Copy data, which already exists in GCP

COPY INTO RAW_DATA.JSON_RAW
FROM @ITRA_DEMO.STAGES.stage_gcp_json;

-- Create pipe
CREATE OR REPLACE SCHEMA pipes;

CREATE OR REPLACE NOTIFICATION INTEGRATION gcp_notification
  TYPE = QUEUE
  NOTIFICATION_PROVIDER = GCP_PUBSUB
  ENABLED = true
  GCP_PUBSUB_SUBSCRIPTION_NAME = '<GCP_pubsub_subscription_name>';
  
DESC NOTIFICATION INTEGRATION gcp_notification;

CREATE OR REPLACE PIPE ITRA_DEMO.pipes.event_data_pipe 
    auto_ingest = TRUE
    INTEGRATION = gcp_notification
AS
    COPY INTO ITRA_DEMO.raw_data.json_raw
    FROM @ITRA_DEMO.stages.stage_gcp_json;



-- Create a stream object
CREATE OR REPLACE STREAM events_stream ON TABLE ITRA_DEMO.raw_data.json_raw;
SHOW STREAMS;


-- create task
CREATE OR REPLACE TASK server_install_event_changes
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '10 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('EVENTS_STREAM')
    AS
MERGE INTO ITRA_DEMO.events.server_install_events AS IE
USING (
        SELECT 
          $1:event_data.data.eventData.app_user_id::INT AS player_id,
          NULLIF($1:event_data.data.eventData.platformAccountId, 'unknown')::INT AS device_id,
          $1:event_data.timestampClient::TIMESTAMP AS install_date,
          $1:event_data.platform:: STRING AS client_id,
          $1:event_data.appName:: STRING AS app_name,
          NULLIF($1:event_data.countryCode, '')::STRING AS country,
          METADATA$ACTION,
          METADATA$ISUPDATE,
          METADATA$ROW_ID
        FROM events_stream
        WHERE $1:event_data.data.eventData.eventType = 'server_install'
    ) AS S
ON IE.player_id = S.player_id
WHEN MATCHED -- DELETE CONDITION
    AND S.METADATA$ACTION = 'DELETE'
    AND S.METADATA$ISUPDATE = 'FALSE'
    THEN DELETE
WHEN MATCHED -- UPDATE CONDITION
    AND S.METADATA$ACTION = 'INSERT'
    AND S.METADATA$ISUPDATE = 'TRUE'
    THEN UPDATE
    SET IE.player_id = S.player_id,
        IE.device_id = S.device_id,
        IE.install_date = S.install_date,
        IE.client_id = S.client_id,
        IE.app_name = S.app_name,
        IE.country = S.country
WHEN NOT MATCHED -- INSERT CONDITION
    AND S.METADATA$ACTION = 'INSERT'
    AND S.METADATA$ISUPDATE = 'FALSE'
    THEN INSERT
    (player_id, device_id, install_date, client_id, app_name, country)
    VALUES
    (S.player_id, S.device_id, S.install_date, S.client_id, S.app_name, S.country);
    
    
SHOW TASKS; 

ALTER TASK SERVER_INSTALL_EVENT_CHANGES RESUME;