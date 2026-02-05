USE ROLE ai_admin
;

CREATE OR REPLACE cortex SEARCH service se.data.SE_EVENTS_CALENDAR
	ON EVENT_NAME
	attributes CALENDAR_SOURCE,START_DATE, TERRITORY, GSHEET_LINK
	WAREHOUSE='PIPE_MEDIUM'
	TARGET_LAG='1 day'
	AS (
SELECT
	event_name AS event_name__o,
	'Source:\n\n' || calendar_source ||
	'\n\n\nEvent Name:\n\n' || event_name ||
	'\n\n\nEvent Description:\n\n' || COALESCE(event_description, 'No Description') ||
	'\n\n\nEvent Date:\n\n' || start_date ||
	'\n\n\nEvent End Date:\n\n' || end_date ||
	'\n\n\nEvent Device:\n\n' || COALESCE(device, 'All Devices')
			   AS event_name,
	event_description,
	calendar_source,
	start_date,
	end_date,
	territory,
  	device,
	gsheet_link
FROM se.data.se_event_calendar
);

GRANT USAGE ON CORTEX SEARCH SERVICE SE.DATA.SE_EVENTS_CALENDAR TO ROLE se_basic
;


SELECT
	snowflake.cortex.search_preview(
			'SE.DATA.SE_EVENTS_CALENDAR',
			'{"query": "Are there any tests related to booking fees in 2025?", "limit": 3}'
	)
;

SELECT
	snowflake.cortex.search_preview(
			'SE.DATA.SE_EVENTS_CALENDAR',
			'{"query": "what product releases were there for mobile web?", "limit": 3}'
	)
;

SELECT *
FROM se.data.se_event_calendar sec
;

USE ROLE ai_admin
;



SELECT GET_DDL('table', 'COLLAB.DATA.MODULE_TOUCHED_SEARCHES')
;



SELECT
	event_name AS event_name__o,
	'Source:\n\n' || calendar_source ||
	'\n\n\nEvent Name:\n\n' || event_name ||
	'\n\n\nEvent Description:\n\n' || COALESCE(event_description, 'No Description') ||
	'\n\n\nEvent Date:\n\n' || start_date ||
	'\n\n\nEvent End Date:\n\n' || end_date ||
	'\n\n\nEvent Device:\n\n' || COALESCE(device, 'All Devices')
			   AS event_name,
	event_description,
	calendar_source,
	start_date,
	end_date,
	territory,
	gsheet_link
FROM se.data.se_event_calendar
;

SELECT * FROM  se.data.se_event_calendar


SELECT * FROM latest_vault.trading_gsheets.promo_calendar;
SELECT * FROM latest_vault.trading_gsheets.product_release_calendar;


USE ROLE ai_admin;
ALTER CORTEX SEARCH SERVICE se.data.se_events_calendar
SET QUERY = '
SELECT
	event_name AS event_name__o,
	''Source:\n\n'' || calendar_source ||
	''\n\n\nEvent Name:\n\n'' || event_name ||
	''\n\n\nEvent Description:\n\n'' || COALESCE(event_description, ''No Description'') ||
	''\n\n\nEvent Date:\n\n'' || start_date ||
	''\n\n\nEvent End Date:\n\n'' || end_date ||
	''\n\n\nEvent Device:\n\n'' || COALESCE(device, ''All Devices'')
			   AS event_name,
	event_description,
	calendar_source,
	start_date,
	end_date,
	territory,
	gsheet_link
FROM se.data.se_event_calendar
'
;

DESCRIBE CORTEX SEARCH SERVICE se.data.se_events_calendar;

GRANT USAGE ON CORTEX SEARCH SERVICE SCRATCH.ROBINPATEL.SE_TRADING_DECKS_DEMO TO ROLE se_basic
;

SELECT CURRENT_ROLE()


SELECT
	snowflake.cortex.search_preview(
			'SCRATCH.ROBINPATEL.SE_TRADING_DECKS_DEMO',
			'{"query": "What did the trading decks say about last week", "limit": 3}'
	)
;