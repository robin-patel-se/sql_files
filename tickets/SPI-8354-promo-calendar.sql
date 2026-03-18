USE ROLE accountadmin
;


CREATE OR REPLACE VIEW snowmind.staging_unstructured.promotion_search_index AS
SELECT

	HASH(p.se_sale_id, p.campaign_name, p.promo_start_date, p.promo_end_date) AS promotion_hash,
	p.se_sale_id,
	p.posa,
	p.promo_type,
	p.campaign_name,
	p.promo_start_date,
	p.promo_end_date,
	p.send_date,
	ds.salesforce_opportunity_id,
	-- Concatenated Search Column
	ARRAY_TO_STRING(
			ARRAY_CONSTRUCT_COMPACT(
					'se_sale_id: ' || p.se_sale_id::TEXT,
					'salesforce_opportunity_id: ' || ds.salesforce_opportunity_id::TEXT,
					'posa: ' || p.posa::TEXT,
					'promo_type: ' || p.promo_type::TEXT,
					'campaign_name: ' || p.campaign_name::TEXT,
					'promo_start_date: ' || p.promo_start_date::TEXT,
					'promo_end_date: ' || p.promo_end_date::TEXT,
					'send_date: ' || p.send_date::TEXT
			),
			'\n'
	)                                                                         AS search_index_col
FROM latest_vault.fpa_gsheets.promotion p
INNER JOIN se.data.dim_sale ds
	ON p.se_sale_id = ds.se_sale_id
;


SELECT *
FROM latest_vault.fpa_gsheets.promotion p
;


USE ROLE accountadmin
;

USE DATABASE snowmind
;

USE SCHEMA search_services
;

SHOW cortex SEARCH SERVICES
;

GRANT USAGE ON CORTEX SEARCH SERVICE snowmind.search_services.promo_search_service TO ROLE ai_developer
;

GRANT USAGE ON CORTEX SEARCH SERVICE se.data.trading_decks TO ROLE ai_developer
;

DESCRIBE CORTEX SEARCH SERVICE snowmind.search_services.promo_search_service
;

ALTER cortex search SERVICE snowmind.search_services.promo_search_service REFRESH
;

SELECT
	COUNT(*)
FROM snowmind.staging_unstructured.promotion_search_index
;



SELECT *
FROM snowmind.staging_unstructured.promotion_search_index
;

DROP TABLE data_vault_mvp_dev_robin.dwh.promo_calendar
;


SELECT
	HASH(promotion.se_sale_id, promotion.campaign_name, promotion.promo_start_date,
		 promotion.promo_end_date) AS promotion_id,
	promotion.se_sale_id,
	promotion.posa,
	promotion.promo_type,
	promotion.campaign_name,
	promotion.promo_start_date,
	promotion.promo_end_date,
	promotion.send_date,
	dim_sale.salesforce_opportunity_id,
	-- used to check if anything has changed in the data
	HASH(
			promotion.se_sale_id,
			promotion.posa,
			promotion.promo_type,
			promotion.campaign_name,
			promotion.promo_start_date,
			promotion.promo_end_date,
			promotion.send_date,
			dim_sale.salesforce_opportunity_id,
	)                              AS promotion_hash,
	-- Concatenated Search Column
	ARRAY_TO_STRING(
			ARRAY_CONSTRUCT_COMPACT(
					'se_sale_id: ' || promotion.se_sale_id::TEXT,
					'salesforce_opportunity_id: ' || dim_sale.salesforce_opportunity_id::TEXT,
					'posa: ' || promotion.posa::TEXT,
					'promo_type: ' || promotion.promo_type::TEXT,
					'campaign_name: ' || promotion.campaign_name::TEXT,
					'promo_start_date: ' || promotion.promo_start_date::TEXT,
					'promo_end_date: ' || promotion.promo_end_date::TEXT,
					'send_date: ' || promotion.send_date::TEXT
			),
			'
'
	)                              AS search_index_col
FROM latest_vault_dev_robin.fpa_gsheets.promotion promotion
INNER JOIN data_vault_mvp_dev_robin.dwh.dim_sale dim_sale
	ON promotion.se_sale_id = dim_sale.se_sale_id
;

WITH
	dupes AS (
		SELECT
			promotion_id,
			se_sale_id,
			posa,
			promo_type,
			campaign_name,
			promo_start_date,
			promo_end_date,
			send_date,
			salesforce_opportunity_id,
			promotion_hash,
			search_index_col,
			gsheet_link,
			gsheet_row_number
		FROM data_vault_mvp_dev_robin.dwh.promo_calendar__model_data_test promo_calendar
		QUALIFY COUNT(*) OVER (PARTITION BY promotion_id) > 1
		ORDER BY promo_calendar.promotion_id
	)

SELECT
	dupes.promotion_id,
	dupes.se_sale_id,
	dupes.salesforce_opportunity_id,
	dupes.posa,
	ds.posa_territory,
	ds2.se_sale_id,
	dupes.promo_type,
	dupes.campaign_name,
	dupes.promo_start_date,
	dupes.promo_end_date,
	dupes.gsheet_link,
	dupes.send_date,
	dupes.promotion_hash,
	dupes.search_index_col,
	dupes.gsheet_row_number
FROM dupes
LEFT JOIN se.data.dim_sale ds
	ON dupes.se_sale_id = ds.se_sale_id
LEFT JOIN se.data.dim_sale ds2
	ON dupes.salesforce_opportunity_id = ds2.salesforce_opportunity_id
	AND dupes.posa = ds2.posa_territory
;



DROP TABLE data_vault_mvp_dev_robin.dwh.promo_calendar
;


SELECT
	ds.se_sale_id,
	ds.posa_territory
FROM se.data.dim_sale ds
WHERE ds.se_sale_id = 'A56248'
;



https://docs.google.com/spreadsheets/d/1LLyjEtylnTE4SQ1nCQ4FcbYyVgr35lVdk9o32UiyLFc/edit?gid=0
#gid=0&RANGE=13:13;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.promo_calendar
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.promo_calendar__model_data_test
AS
SELECT
	HASH(promotion.se_sale_id, promotion.campaign_name, promotion.promo_start_date,
		 promotion.promo_end_date) AS promotion_id,
	promotion.se_sale_id,
	promotion.posa,
	promotion.promo_type,
	promotion.campaign_name,
	promotion.promo_start_date,
	promotion.promo_end_date,
	promotion.send_date,
	dim_sale.salesforce_opportunity_id,
	-- used to check if anything has changed in the data
	HASH(
			promotion.se_sale_id,
			promotion.posa,
			promotion.promo_type,
			promotion.campaign_name,
			promotion.promo_start_date,
			promotion.promo_end_date,
			promotion.send_date,
			dim_sale.salesforce_opportunity_id
	)                              AS promotion_hash,
	-- Concatenated Search Column
	ARRAY_TO_STRING(
			ARRAY_CONSTRUCT_COMPACT(
					'se_sale_id: ' || promotion.se_sale_id::TEXT,
					'salesforce_opportunity_id: ' || dim_sale.salesforce_opportunity_id::TEXT,
					'posa: ' || promotion.posa::TEXT,
					'promo_type: ' || promotion.promo_type::TEXT,
					'campaign_name: ' || promotion.campaign_name::TEXT,
					'promo_start_date: ' || promotion.promo_start_date::TEXT,
					'promo_end_date: ' || promotion.promo_end_date::TEXT,
					'send_date: ' || promotion.send_date::TEXT
			),
			'\n'
	)                              AS search_index_col,
	'https://docs.google.com/spreadsheets/d/1LLyjEtylnTE4SQ1nCQ4FcbYyVgr35lVdk9o32UiyLFc/edit?gid=0#gid=0&range='
		|| (promotion.row_file_row_number + 2)::VARCHAR
		|| ':'
		|| (promotion.row_file_row_number + 2)::VARCHAR
								   AS gsheet_link,
	promotion.row_file_row_number  AS gsheet_row_number
FROM latest_vault_dev_robin.fpa_gsheets.promotion
INNER JOIN data_vault_mvp_dev_robin.dwh.dim_sale dim_sale
	ON promotion.se_sale_id = dim_sale.se_sale_id
;


CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.promotion CLONE latest_vault.fpa_gsheets.promotion

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale CLONE data_vault_mvp.dwh.dim_sale
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.promo_calendar
;

SELECT *
FROM snowflake.account_usage.query_history
WHERE query_id = '01c2fa04-0209-7c3b-0002-dd01506ef2d7'

USE ROLE pipelinerunner

GRANT USAGE ON CORTEX SEARCH SERVICE snowmind.search_services.promo_search_service TO ROLE ai_developer
;


DESCRIBE CORTEX SEARCH SERVICE snowmind.search_services.promo_search_service
;

USE ROLE pipelinerunner
;


SELECT GET_DDL('CORTEX_SEARCH_SERVICE', 'snowmind.search_services.promo_search_service')
;

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


/*CREATE OR REPLACE CORTEX SEARCH SERVICE PROMO_SEARCH_SERVICE
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
    );*/

GRANT USAGE ON CORTEX SEARCH SERVICE snowmind.search_services.promo_search_service TO ROLE ai_developer
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.promo_calendar pc

SELECT
           CONCAT_WS(' | ', -- Using | as separator for better readability
                CONCAT('campaign: ', IFNULL(promotion.campaign_name, '')),
                CONCAT('type: ', IFNULL(promotion.promo_type, '')),
                CONCAT('market: ', IFNULL(promotion.posa, '')),
                CONCAT('starts: ', IFNULL(TO_VARCHAR(promotion.promo_start_date, 'YYYY-MM-DD'), '')),
                CONCAT('ends: ', IFNULL(TO_VARCHAR(promotion.promo_end_date, 'YYYY-MM-DD'), '')),
                CONCAT('sale: ', IFNULL(promotion.se_sale_id, ''))
            ) AS search_index_col,
FROM data_vault_mvp.dwh.promo_calendar promotion
;


SELECT *
FROM data_vault_mvp.dwh.promo_calendar;

USE ROLE pipelinerunner;

DROP TABLE data_vault_mvp.dwh.promo_calendar;