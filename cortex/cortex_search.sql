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



GRANT USAGE ON CORTEX SEARCH SERVICE  DBT.BI_PRODUCT_ANALYTICS__INTERMEDIATE.HOTJAR_CUSTOMER_REVIEWS TO ROLE dbt_analyst;
;

------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner;

CREATE OR REPLACE CORTEX SEARCH SERVICE SNOWMIND.SEARCH_SERVICES.PROMO_SEARCH_SERVICE
    ON SEARCH_INDEX_COL
    ATTRIBUTES
        PROMO_START_DATE,
        PROMO_END_DATE,
        CAMPAIGN_NAME,
        SE_SALE_ID,
        PROMO_TYPE,
        SEND_DATE,
        POSA  -- Add market/territory for filtering
    WAREHOUSE = 'ANALYST_MEDIUM'
    TARGET_LAG = '1 day'
    REFRESH_MODE = INCREMENTAL
    AS (
        SELECT
            -- Search column: enriched text for semantic search
			SEARCH_INDEX_COL,

            -- Attributes for filtering (ensure proper types)
            PROMO_START_DATE::DATE AS PROMO_START_DATE,
            PROMO_END_DATE::DATE AS PROMO_END_DATE,
            CAMPAIGN_NAME,
            SE_SALE_ID,
            PROMO_TYPE,
            SEND_DATE::DATE AS SEND_DATE,
            POSA,

            -- Additional columns for display
            SALESFORCE_OPPORTUNITY_ID,
            GSHEET_LINK
        FROM DATA_VAULT_MVP.DWH.PROMO_CALENDAR
    );

GRANT USAGE ON CORTEX SEARCH SERVICE snowmind.search_services.promo_search_service TO ROLE ai_developer
;

USE ROLE pipelinerunner;
GRANT USAGE ON CORTEX SEARCH SERVICE snowmind.search_services.promo_search_service TO ROLE ai_user;


CREATE OR REPLACE CORTEX SEARCH SERVICE SNOWMIND.SEARCH_SERVICES.TARGET_CLUSTER_SUB_REGION_SERVICE
    ON cluster_sub_region
    WAREHOUSE = 'ANALYST_SMALL'
    TARGET_LAG = '30 day'
    AS (

	SELECT DISTINCT
		dimension_6 AS cluster_sub_region
	FROM se.bi.targets t
	WHERE t.target_name = 'cluster_sub_region_target'
	);

GRANT USAGE ON CORTEX SEARCH SERVICE SNOWMIND.SEARCH_SERVICES.TARGET_CLUSTER_SUB_REGION_SERVICE TO ROLE ai_developer
;

