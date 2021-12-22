-- create task
CREATE OR REPLACE TASK server_install_event_changes
    WAREHOUSE = COMPUTE_WH
    AFTER insert_single_copy
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
    
ALTER TASK TRUNCATE_DUPLICATES_TABLE RESUME;
ALTER TASK SERVER_INSTALL_EVENT_CHANGES RESUME;
ALTER TASK INSERT_SINGLE_COPY RESUME;
ALTER TASK DROP_DUPLCATED_DATA RESUME;
ALTER TASK FIND_DUPLICATES RESUME;