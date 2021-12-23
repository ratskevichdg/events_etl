ALTER TASK SERVER_INSTALL_EVENT_CHANGES SUSPEND;
ALTER TASK REFRESH_EVENTS_STREAM SUSPEND;
ALTER TASK DROP_DUPLICATES_FROM_JSON_RAW SUSPEND;


-- create task which inserts server_install events into server_install_events table
CREATE OR REPLACE TASK server_install_event_changes
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '60 MINUTE'
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
        AND events_stream.$1 NOT IN (
          SELECT RAW_FILE 
          FROM (
            SELECT 
              COUNT(*) AS NUM_ROWS, 
              RAW_FILE 
            FROM ITRA_DEMO.raw_data.json_raw 
            GROUP BY RAW_FILE 
            HAVING NUM_ROWS > 1
            )
        )
    ) AS S
ON IE.player_id = S.player_id
WHEN NOT MATCHED -- INSERT CONDITION
    AND S.METADATA$ACTION = 'INSERT'
    AND S.METADATA$ISUPDATE = 'FALSE'
    THEN INSERT
    (player_id, device_id, install_date, client_id, app_name, country)
    VALUES
    (S.player_id, S.device_id, S.install_date, S.client_id, S.app_name, S.country);
    

ALTER TASK DROP_DUPLICATES_FROM_JSON_RAW RESUME;
ALTER TASK REFRESH_EVENTS_STREAM RESUME;
ALTER TASK SERVER_INSTALL_EVENT_CHANGES RESUME;