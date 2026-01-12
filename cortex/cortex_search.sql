USE ROLE ai_admin
;

CREATE

OR

REPLACE
cortex SEARCH service se.data.SE_EVENTS_CALENDAR
	ON EVENT_NAME
	attributes CALENDAR_SOURCE,START_DATE, TERRITORY, GSHEET_LINK
	WAREHOUSE='PIPE_LARGE'
	TARGET_LAG='1 day'
	AS (
	SELECT
		EVENT_NAME AS EVENT_NAME__O,
	    'Source:\n\n' || CALENDAR_SOURCE ||
	  	'\n\n\nEvent Name:\n\n' || EVENT_NAME ||
	  	'\n\n\nEvent Description:\n\n' || COALESCE(EVENT_DESCRIPTION, 'No Description') ||
	  	'\n\n\nEvent Date:\n\n' ||  START_DATE ||
	  	'\n\n\nEvent End Date:\n\n' ||  END_DATE
                   AS EVENT_NAME,
	  	EVENT_DESCRIPTION,
		CALENDAR_SOURCE,
		START_DATE,
-- 		END_DATE,
		TERRITORY,
	  	GSHEET_LINK
	FROM SE.DATA.SE_EVENT_CALENDAR
);


SELECT
	snowflake.cortex.search_preview(
			'SE.DATA.SE_EVENTS_CALENDAR',
			'{"query": "Are there any tests related to booking fees in 2025?", "limit": 3}'
	)
;

SELECT *
FROM se.data.se_event_calendar sec
;

USE ROLE ai_admin;

GRANT USAGE ON CORTEX SEARCH SERVICE SE.DATA.SE_EVENTS_CALENDAR TO ROLE se_basic
;


SELECT GET_DDL('table', 'COLLAB.DATA.MODULE_TOUCHED_SEARCHES')
;

SELECT *
FROM se.data.scv_touched_searches sts
WHERE sts.event_tstamp::DATE = CURRENT_DATE - 1
AND sts.triggered_by = 'user'
