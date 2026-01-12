-- download from: https://drive.google.com/drive/folders/1aaPtVrD2X9bC-oJKarETclTh76Uz6QVA

USE WAREHOUSE pipe_large
;

CREATE SCHEMA collab.muse_tech_task
;

USE SCHEMA collab.muse_tech_task
;
;

CREATE OR REPLACE TABLE collab.muse_tech_task.members
(
	id                 NUMBER NOT NULL PRIMARY KEY,
	member_id          VARCHAR(32),
	sign_up_date       TIMESTAMP,
	last_updated       TIMESTAMP,
	original_territory VARCHAR(2),
	current_territory  VARCHAR(2),
	schedule_tstamp    TIMESTAMP,
	extracted_at       TIMESTAMP
)
;


USE SCHEMA collab.muse_tech_task
;

PUT 'file:///Users/robin.patel/myrepos/sql_files/data_warehouse_engineer_tech_task/Feed A - Members.csv' @%members
;

COPY INTO collab.muse_tech_task.members
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.muse_tech_task.members
;
------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE collab.muse_tech_task.bookings
(
	id              NUMBER NOT NULL PRIMARY KEY,
	member_id       VARCHAR(32),
	booking_id      VARCHAR(32),
	booking_date    TIMESTAMP,
	last_updated    TIMESTAMP,
	booking_status  VARCHAR(16),
	schedule_tstamp TIMESTAMP,
	extracted_at    TIMESTAMP
)
;


USE SCHEMA collab.muse_tech_task
;

PUT 'file:///Users/robin.patel/myrepos/sql_files/data_warehouse_engineer_tech_task/Feed B - Bookings.csv' @%bookings
;

COPY INTO collab.muse_tech_task.bookings
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.muse_tech_task.bookings
;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE collab.muse_tech_task.events
(
	id              NUMBER NOT NULL PRIMARY KEY,
	territory       varchar(2),
	cookie_id       varchar(36),
	member_id       varchar(32),
	booking_id      varchar(32),
	event_name      varchar(11),
	event_tstamp    timestamp,
	page_urlpath    text,
	schedule_tstamp timestamp,
	extracted_at    timestamp
)
;


USE SCHEMA collab.muse_tech_task
;

PUT 'file:///Users/robin.patel/myrepos/sql_files/data_warehouse_engineer_tech_task/Feed C - Events.csv' @%events
;

COPY INTO collab.muse_tech_task.events
	FILE_FORMAT = (
		TYPE = CSV
			FIELD_DELIMITER = ','
			SKIP_HEADER = 1
			FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
			RECORD_DELIMITER = '\\n'
		)
;


SELECT *
FROM collab.muse_tech_task.events
;


GRANT USAGE ON SCHEMA collab.muse_tech_task TO ROLE data_team_basic
;

GRANT SELECT ON ALL TABLES IN SCHEMA collab.muse_tech_task TO ROLE data_team_basic
;


SELECT *
FROM collab.muse_tech_task.events e
INNER JOIN collab.muse_tech_task.members m ON e.member_id::VARCHAR = m.member_id::VARCHAR
INNER JOIN muse_tech_task.bookings b ON e.booking_id = b.booking_id