CREATE OR REPLACE DATABASE ITRA_DEMO;


-- Create events schema
CREATE OR REPLACE SCHEMA events;


-- Create server_install_events table
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


-- Create shema for raw_data
CREATE OR REPLACE SCHEMA RAW_DATA;


-- Create table for raw json data
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