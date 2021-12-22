# EVENTS ETL
## An educational project on creating ETL event processingю
#### Content
##### preparing_steps.sql 
it is a SQL script for creating database with ==server_install== events.
Contains creating:
- Database
- Stage object
- Integration object
- Snowpipe
- Stream object
##### server_install_events_etl.sql
Creating task with ETL process for ==server_install== events
##### drop_duplicates_from_existing_table.sql
Delete duplicates from existing `json_raw` table
##### drop_duplicates_tasks.sql
Create tasks for checking and removing duplicates from new added data