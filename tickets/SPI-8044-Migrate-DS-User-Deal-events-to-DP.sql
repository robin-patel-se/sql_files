-- se calendar
-- row per sale per date since their go live date
-- asof join sale active
-- compare if active within start date of any day
-- check if its live on that date

-- query to find sample sales
SELECT *
FROM se.data.sale_active_snapshot sas
INNER JOIN se.data.dim_sale ds
	ON sas.se_sale_id = ds.se_sale_id
WHERE ds.sale_active = FALSE
  AND ds.sale_start_date >= '2025-01-01'

-- example of sale that got toggled on and off 3 times in 2025

-- historic data
WITH
	flattened_dim_sale AS (
		-- flattening old data model sales into multiple rows per territory
		SELECT
			dim_sale.se_sale_id,
			dim_sale.tb_offer_id,
			dim_sale.se_brand,
			dim_sale.sale_start_date,
			TRIM(territory.value) AS posa_territory
		FROM data_vault_mvp.dwh.dim_sale dim_sale,
			 LATERAL SPLIT_TO_TABLE(IFNULL(posa_territory, ''), '|') AS territory
		WHERE dim_sale.is_sale_hidden_from_page_search = FALSE
	),
	grain AS (
		SELECT
			dim_sale_territory.se_sale_id,
			dim_sale_territory.tb_offer_id,
			dim_sale_territory.posa_territory,
			dim_sale_territory.se_brand,
			dim_sale_territory.sale_start_date,
			se_territory.id AS posa_territory_id,
			se_calendar.date_value
		FROM flattened_dim_sale dim_sale_territory
		INNER JOIN data_vault_mvp.dwh.se_territory se_territory
			ON dim_sale_territory.posa_territory = se_territory.posa_territory
		LEFT JOIN data_vault_mvp.dwh.se_calendar se_calendar
			ON dim_sale_territory.sale_start_date::DATE <= se_calendar.date_value
			AND se_calendar.date_value < CURRENT_DATE
		WHERE se_territory.id IN (1, 2, 4, 8, 9, 10, 11, 12, 14, 15, 16, 25, 26, 27, 28)
		  AND dim_sale_territory.se_sale_id = 'A75823' -- TODO REMOVE
	),
	valid_deal_logic AS (
		SELECT
			grain.se_sale_id,
			grain.tb_offer_id,
			grain.posa_territory_id,
			grain.posa_territory,
			grain.se_brand,
			grain.sale_start_date,
			grain.date_value,
-- 	sale_active_snapshot.sale_active,
			sale_active_snapshot.view_date                                  AS last_active_date,
-- 			grain.date_value = last_active_date,
			DATEDIFF(DAY, sale_active_snapshot.view_date, grain.date_value) AS days_since_last_active,
			IFF(days_since_last_active <= 60, TRUE, FALSE)                  AS valid_deal
		FROM grain
		ASOF JOIN data_vault_mvp.dwh.sale_active_snapshot sale_active_snapshot
		MATCH_CONDITION ( grain.date_value >= sale_active_snapshot.view_date )
			ON grain.se_sale_id = sale_active_snapshot.se_sale_id
	)
SELECT
	date_value,
	se_sale_id,
	tb_offer_id,
	posa_territory,
	se_brand,
	sale_start_date,
	last_active_date,
	days_since_last_active,
FROM valid_deal_logic
WHERE valid_deal_logic.valid_deal
;

-- incremental version of the data to show results of valid deals today
WITH
	flattened_dim_sale AS (
		-- flattening old data model sales into multiple rows per territory
		SELECT
			dim_sale.se_sale_id,
			dim_sale.tb_offer_id,
			dim_sale.se_brand,
			dim_sale.sale_start_date,
			TRIM(territory.value) AS posa_territory
		FROM data_vault_mvp.dwh.dim_sale dim_sale,
			 LATERAL SPLIT_TO_TABLE(IFNULL(posa_territory, ''), '|') AS territory
		WHERE dim_sale.is_sale_hidden_from_page_search = FALSE
	),
	grain AS (
		SELECT
			dim_sale_territory.se_sale_id,
			dim_sale_territory.tb_offer_id,
			dim_sale_territory.posa_territory,
			dim_sale_territory.se_brand,
			dim_sale_territory.sale_start_date,
			se_territory.id AS posa_territory_id,
			-- replace
			CURRENT_DATE    AS date_value
		--  remove
		--	se_calendar.date_value
		FROM flattened_dim_sale dim_sale_territory
		INNER JOIN data_vault_mvp.dwh.se_territory se_territory
			ON dim_sale_territory.posa_territory = se_territory.posa_territory
		-- remove
-- 		LEFT JOIN data_vault_mvp.dwh.se_calendar se_calendar
-- 			ON dim_sale_territory.sale_start_date::DATE <= se_calendar.date_value
-- 			AND se_calendar.date_value < CURRENT_DATE
		WHERE se_territory.id IN (1, 2, 4, 8, 9, 10, 11, 12, 14, 15, 16, 25, 26, 27, 28)
		  AND dim_sale_territory.se_sale_id = 'A75823' -- TODO REMOVE
	),
	valid_deal_logic AS (
		SELECT
			grain.se_sale_id,
			grain.tb_offer_id,
			grain.posa_territory_id,
			grain.posa_territory,
			grain.se_brand,
			grain.sale_start_date,
			grain.date_value,
-- 	sale_active_snapshot.sale_active,
			sale_active_snapshot.view_date                                  AS last_active_date,
-- 			grain.date_value = last_active_date,
			DATEDIFF(DAY, sale_active_snapshot.view_date, grain.date_value) AS days_since_last_active,
			IFF(days_since_last_active <= 60, TRUE, FALSE)                  AS valid_deal
		FROM grain
		ASOF JOIN data_vault_mvp.dwh.sale_active_snapshot sale_active_snapshot
		MATCH_CONDITION ( grain.date_value >= sale_active_snapshot.view_date )
			ON grain.se_sale_id = sale_active_snapshot.se_sale_id
	)
SELECT
	date_value,
	se_sale_id,
	tb_offer_id,
	posa_territory,
	se_brand,
	sale_start_date,
	last_active_date,
	days_since_last_active,
FROM valid_deal_logic
WHERE valid_deal_logic.valid_deal
;

SELECT *
FROM se.data.dim_sale ds


SELECT *
FROM data_vault_mvp_dev_robin.ds_input_data_products.valid_deals_filter
WHERE view_date = CURRENT_DATE - 1
;



USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active_snapshot
	CLONE data_vault_mvp.dwh.sale_active_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_territory
	CLONE data_vault_mvp.dwh.se_territory
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.ds_input_data_products.valid_deals_filter
	CLONE data_vault_mvp.ds_input_data_products.valid_deals_filter
;

self_describing_task
\
    --include 'biapp.task_catalogue.ds.user_deal_events.valid_deals_filter.py' \
    --method 'run' \
    --start '2025-12-09 00:00:00' \
    --end '2025-12-09 00:00:00'



SELECT *
FROM data_vault_mvp_dev_robin.ds_input_data_products.valid_deals_filter
WHERE valid_deals_filter.view_date = CURRENT_DATE
;



------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/ds/user_deal_events/user_sale_page_views.

-- module=/biapp/task_catalogue/ds/user_deal_events/user_sale_page_views.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_user_attributes
	CLONE data_vault_mvp.dwh.tvl_user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/ds/user_deal_events/user_sale_page_views_today.py

-- module=/biapp/task_catalogue/ds/user_deal_events/user_sale_page_views_today.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
	CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_user_attributes
	CLONE data_vault_mvp.dwh.tvl_user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

CREATE SCHEMA IF NOT EXISTS se_dev_robin.data

;

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/ds/user_deal_events/user_booking_form_views.py

-- module=/biapp/task_catalogue/ds/user_deal_events/user_booking_form_views.py make clones


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_user_attributes
	CLONE data_vault_mvp.dwh.tvl_user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/ds/user_deal_events/user_booking_form_views_today.py

-- module=/biapp/task_catalogue/ds/user_deal_events/user_booking_form_views_today.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
	CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_user_attributes
	CLONE data_vault_mvp.dwh.tvl_user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/ds/user_deal_events/user_bookings.py

-- module=/biapp/task_catalogue/ds/user_deal_events/user_bookings.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_user_attributes
	CLONE data_vault_mvp.dwh.tvl_user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/ds/user_deal_events/user_bookings_today.py

-- module=/biapp/task_catalogue/ds/user_deal_events/user_bookings_today.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
	CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_user_attributes
	CLONE data_vault_mvp.dwh.tvl_user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

------------------------------------------------------------------------------------------------------------------------
-- biapp/task_catalogue/ds/user_deal_events/valid_deals_filter.py
-- module=/biapp/task_catalogue/ds/user_deal_events/valid_deals_filter.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active_snapshot
	CLONE data_vault_mvp.dwh.sale_active_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_territory
	CLONE data_vault_mvp.dwh.se_territory
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

------------------------------------------------------------------------------------------------------------------------
DROP TABLE data_vault_mvp_dev_robin.ds_input_data_products.user_booking_form_views
;

DROP TABLE data_vault_mvp_dev_robin.ds_input_data_products.user_sale_page_views
;

DROP TABLE data_vault_mvp_dev_robin.ds_input_data_products.user_bookings
;

------------------------------------------------------------------------------------------------------------------------
-- Consolidated Setup for Development Process
-- User: robinpatel

USE ROLE personal_role__robinpatel
;

--------------------------------------------------------------------------------
-- SCHEMA INITIALIZATION
--------------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE SCHEMA IF NOT EXISTS se_dev_robin.data
;

--------------------------------------------------------------------------------
-- DATA VAULT DWH CLONES & VIEWS
--------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_user_attributes
	CLONE data_vault_mvp.dwh.tvl_user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active_snapshot
	CLONE data_vault_mvp.dwh.sale_active_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_territory
	CLONE data_vault_mvp.dwh.se_territory
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

--------------------------------------------------------------------------------
-- SINGLE CUSTOMER VIEW STG CLONES
--------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_identity_stitching
	CLONE data_vault_mvp.single_customer_view_stg.module_identity_stitching
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

--------------------------------------------------------------------------------
-- SNOWPLOW CLONES
--------------------------------------------------------------------------------
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;


------------------------------------------------------------------------------------------------------------------------
self_describing_task --include 'biapp/task_catalogue/ds/user_deal_events/user_booking_form_views.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/ds/user_deal_events/user_bookings.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/ds/user_deal_events/user_sale_page_views.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/ds/user_deal_events/user_booking_form_views_today.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/ds/user_deal_events/user_bookings_today.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/ds/user_deal_events/user_sale_page_views_today.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'


self_describing_task --include 'biapp/task_catalogue/ds/user_deal_events/valid_deals_filter.py'  --method 'run' --start '1970-01-01 00:00:00' --end '1970-01-01 00:00:00'
self_describing_task --include 'biapp/task_catalogue/ds/user_deal_events/valid_deals_filter.py'  --method 'run' --start '2025-12-08 00:00:00' --end '2025-12-08 00:00:00'

self_describing_task --include 'biapp/task_catalogue/ds/user_deal_events/user_deal_events.py'  --method 'run' --start '2025-12-08 00:00:00' --end '2025-12-08 00:00:00'

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.ds_input_data_products.user_booking_form_views
;

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.ds_input_data_products.user_bookings
;

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.ds_input_data_products.user_sale_page_views
;

SELECT *
FROM data_vault_mvp_dev_robin.ds_input_data_products.user_deal_events
;



------------------------------------------------------------------------------------------------------------------------
-- validation


-- prod
SELECT
	user_deal_events.evt_date,
-- 	COUNT(*)                                                                                     AS user_sales,
	COUNT(DISTINCT user_deal_events.user_id)                                                     AS users,
-- 	COUNT_IF(user_deal_events.evt_name = 'deal-view')                                            AS deal_view__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'deal-view', user_deal_events.user_id, NULL)) AS deal_view__users,
-- 	COUNT_IF(user_deal_events.evt_name = 'book-form')                                            AS book_form__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'book-form', user_deal_events.user_id, NULL)) AS book_form__users,
-- 	COUNT_IF(user_deal_events.evt_name = 'order')                                                AS orders__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'order', user_deal_events.user_id, NULL))     AS orders__users,
FROM data_science.predictive_modeling.user_deal_events
WHERE user_deal_events.evt_date >= CURRENT_DATE - 10
GROUP BY ALL
;

-- dev
SELECT
	user_deal_events.evt_date,
-- 	COUNT(*)                                                                                     AS user_sales,
	COUNT(DISTINCT user_deal_events.user_id)                                                     AS users,
-- 	COUNT_IF(user_deal_events.evt_name = 'deal-view')                                            AS deal_view__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'deal-view', user_deal_events.user_id, NULL)) AS deal_view__users,
-- 	COUNT_IF(user_deal_events.evt_name = 'book-form')                                            AS book_form__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'book-form', user_deal_events.user_id, NULL)) AS book_form__users,
-- 	COUNT_IF(user_deal_events.evt_name = 'order')                                                AS orders__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'order', user_deal_events.user_id, NULL))     AS orders__users,
FROM data_vault_mvp_dev_robin.ds_input_data_products.user_deal_events
WHERE user_deal_events.evt_date >= CURRENT_DATE - 10
GROUP BY ALL
;

USE WAREHOUSE pipe_xlarge
;

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.user_deal_events_10_days AS
SELECT *
FROM data_vault_mvp_dev_robin.ds_input_data_products.user_deal_events
WHERE user_deal_events.evt_date >= CURRENT_DATE - 10
;

-- step change down for dev space on bookings on the 16th of December, investigating
-- prod
SELECT *
FROM data_science.predictive_modeling.user_deal_events user_deal_events
WHERE user_deal_events.evt_date = '2025-12-16'
  AND user_deal_events.evt_name = 'order'
GROUP BY ALL
;

-- dev
SELECT *
FROM scratch.robinpatel.user_deal_events_10_days user_deal_events
WHERE user_deal_events.evt_date = '2025-12-16'
  AND user_deal_events.evt_name = 'order'
GROUP BY ALL
;


WITH
	prod AS (
		SELECT *,
			   'prod' AS source,
		FROM data_science.predictive_modeling.user_deal_events user_deal_events
		WHERE user_deal_events.evt_date = '2025-12-16'
		  AND user_deal_events.evt_name = 'order'
		GROUP BY ALL
	),
	dev AS (

-- dev
		SELECT *,
			   'dev' AS source,
		FROM scratch.robinpatel.user_deal_events_10_days user_deal_events
		WHERE user_deal_events.evt_date = '2025-12-16'
		  AND user_deal_events.evt_name = 'order'
		GROUP BY ALL
	)

SELECT
	deal_id,
	user_id,
FROM prod
EXCEPT
SELECT
	deal_id,
	user_id,
FROM dev
;


SELECT *
FROM data_science.predictive_modeling.user_deal_events user_deal_events
WHERE user_deal_events.evt_date = '2025-12-16'
  AND user_deal_events.user_id = 79984780

SELECT *
FROM scratch.robinpatel.user_deal_events_10_days user_deal_events
WHERE user_deal_events.evt_date = '2025-12-16'
  AND user_deal_events.user_id = 79984780
;


SELECT *
FROM data_vault_mvp_dev_robin.ds_input_data_products.user_bookings ub
WHERE ub.user_id = '79984780'
;


SELECT
	bookings_history.hash_id,
	bookings_history.territory_id,
	bookings_history.deal_id,
	bookings_history.user_id,
	'order'                                                         AS evt_name,
	bookings_history.evt_date,
	bookings_history.se_brand,
	bookings_history.max_event_ts,
	bookings_history.min_event_ts,
	bookings_history.bookings                                       AS event_count,
	'data_vault_mvp_dev_robin.ds_input_data_products.user_bookings' AS data_source
FROM data_vault_mvp_dev_robin.ds_input_data_products.user_bookings bookings_history
WHERE (-- if queried between midnight and 4AM utc don't include previous day
		  CURRENT_TIME() >= '00:00:00'::TIME -- Check for 00:00 (start of day)
			  AND CURRENT_TIME() <= '04:00:00'::TIME -- Check for before 04:00
			  AND bookings_history.evt_date < CURRENT_DATE - 1
		  ) OR
	  (-- if queried after 4AM utc include previous day
		  CURRENT_TIME() > '04:00:00'::TIME -- Check after 04:00
		  )
		  AND bookings_history.user_id = 79984780
;


SELECT *
FROM data_vault_mvp_dev_robin.ds_input_data_products.valid_deals_filter vdf
WHERE vdf.deal_id = 'A79608'
;

-- need to investigate variance of valid deal logic. There are some sales that look like they should be in there. 'A79608' for example isn't in there at all.


SELECT *
FROM se.data.sale_active sa
WHERE sa.se_sale_id = 'A79608'
;


DROP SCHEMA data_vault_mvp_dev_robin.ds_input_data_products
;

SELECT *
FROM customer_insight.sandbox.crm_analysis
;


./
scripts/
mwaa-cli production "dags backfill --start-date '2025-12-27 00:00:00' --end-date '2025-12-28 00:00:00' --donot-pickle ds__user_deal_events__daily_at_04h00"



-- prod

SELECT
	user_deal_events.evt_date,
-- 	COUNT(*)                                                                                     AS user_sales,
	COUNT(DISTINCT user_deal_events.user_id)                                                     AS users,
-- 	COUNT_IF(user_deal_events.evt_name = 'deal-view')                                            AS deal_view__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'deal-view', user_deal_events.user_id, NULL)) AS deal_view__users,
-- 	COUNT_IF(user_deal_events.evt_name = 'book-form')                                            AS book_form__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'book-form', user_deal_events.user_id, NULL)) AS book_form__users,
-- 	COUNT_IF(user_deal_events.evt_name = 'order')                                                AS orders__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'order', user_deal_events.user_id, NULL))     AS orders__users,
FROM data_science.predictive_modeling.user_deal_events
WHERE user_deal_events.evt_date >= CURRENT_DATE - 10
GROUP BY ALL
;
-- dev
SELECT
	user_deal_events.evt_date,
-- 	COUNT(*)                                                                                     AS user_sales,
	COUNT(DISTINCT user_deal_events.user_id)                                                     AS users,
-- 	COUNT_IF(user_deal_events.evt_name = 'deal-view')                                            AS deal_view__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'deal-view', user_deal_events.user_id, NULL)) AS deal_view__users,
-- 	COUNT_IF(user_deal_events.evt_name = 'book-form')                                            AS book_form__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'book-form', user_deal_events.user_id, NULL)) AS book_form__users,
-- 	COUNT_IF(user_deal_events.evt_name = 'order')                                                AS orders__user_sales,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'order', user_deal_events.user_id, NULL))     AS orders__users,
FROM data_vault_mvp.ds_input_data_products.user_deal_events
WHERE user_deal_events.evt_date >= CURRENT_DATE - 10
GROUP BY ALL
;

SELECT
	se_brand,
	COUNT(*),
	SUM(event_count)
FROM data_vault_mvp.ds_input_data_products.user_deal_events
WHERE evt_date = CURRENT_DATE - 1
GROUP BY 1
;


SELECT CURRENT_ROLE()


SELECT *
FROM data_science.predictive_modeling.user_deal_events
WHERE user_deal_events.evt_date = CURRENT_DATE - 8
  AND user_deal_events.evt_name = 'deal-view'
  AND user_deal_events.user_id = 24033939


------------------------------------------------------------------------------------------------------------------------
-- https://gemini.google.com/app/5e2388af76f8e36f
-- CREATE OR REPLACE TASK daily_table_snapshot
--   WAREHOUSE = 'YOUR_WAREHOUSE_NAME'
--   SCHEDULE = 'USING CRON 0 0 * * * UTC' -- Runs at 12:00 AM UTC every day
-- AS
--   INSERT INTO my_table_snapshot
--   SELECT *, CURRENT_TIMESTAMP() AS snapshot_time
--   FROM original_table;

------------------------------------------------------------------------------------------------------------------------
-- snapshotting tables at a time of day

CREATE OR REPLACE TABLE scratch.robinpatel.user_deal_events_snapshots
(
	source           VARCHAR,
	snapshot_time    TIMESTAMP,
	evt_date         DATE,
	row_count        NUMBER,
	users            NUMBER,
	deal_view__users NUMBER,
	book_form__users NUMBER,
	orders__users    NUMBER
)
;

CREATE OR REPLACE TASK scratch.robinpatel.snapshot_data_science_user_deal_events
	WAREHOUSE = pipe_medium
	SCHEDULE = 'USING CRON 0 0 * * * UTC' -- Runs at 12:00 AM UTC every day
	AS
		INSERT INTO scratch.robinpatel.user_deal_events_snapshots
		SELECT
			'data_science.predictive_modeling.user_deal_events' AS source,
			CURRENT_TIMESTAMP                                   AS snapshot_time,
			user_deal_events.evt_date,
			COUNT(*)                                            AS row_count,
			COUNT(DISTINCT user_deal_events.user_id)            AS users,
			COUNT(DISTINCT IFF(user_deal_events.evt_name = 'deal-view', user_deal_events.user_id,
							   NULL))                           AS deal_view__users,
			COUNT(DISTINCT IFF(user_deal_events.evt_name = 'book-form', user_deal_events.user_id,
							   NULL))                           AS book_form__users,
			COUNT(DISTINCT IFF(user_deal_events.evt_name = 'order', user_deal_events.user_id,
							   NULL))                           AS orders__users
		FROM data_science.predictive_modeling.user_deal_events
		WHERE user_deal_events.evt_date >= CURRENT_DATE - 10
		GROUP BY ALL
;

CREATE OR REPLACE TASK scratch.robinpatel.snapshot_pipeline_user_deal_events
	WAREHOUSE = pipe_xlarge
	SCHEDULE = 'USING CRON 0 0 * * * UTC' -- Runs at 12:00 AM UTC every day
	AS
		INSERT INTO scratch.robinpatel.user_deal_events_snapshots
		SELECT
			'data_vault_mvp.ds_input_data_products.user_deal_events' AS source,
			CURRENT_TIMESTAMP                                        AS snapshot_time,
			user_deal_events.evt_date,
			COUNT(*)                                                 AS row_count,
			COUNT(DISTINCT user_deal_events.user_id)                 AS users,
			COUNT(DISTINCT IFF(user_deal_events.evt_name = 'deal-view', user_deal_events.user_id,
							   NULL))                                AS deal_view__users,
			COUNT(DISTINCT IFF(user_deal_events.evt_name = 'book-form', user_deal_events.user_id,
							   NULL))                                AS book_form__users,
			COUNT(DISTINCT IFF(user_deal_events.evt_name = 'order', user_deal_events.user_id,
							   NULL))                                AS orders__users
		FROM data_vault_mvp.ds_input_data_products.user_deal_events
		WHERE user_deal_events.evt_date >= CURRENT_DATE - 10
		GROUP BY ALL
;

ALTER TASK scratch.robinpatel.snapshot_data_science_user_deal_events RESUME
;

ALTER TASK scratch.robinpatel.snapshot_pipeline_user_deal_events RESUME
;


SELECT *
FROM scratch.robinpatel.user_deal_events_snapshots
;


SELECT
	'data_science.predictive_modeling.user_deal_events'                                          AS source,
	CURRENT_TIMESTAMP                                                                            AS snapshot_time,
	user_deal_events.evt_date,
	COUNT(*)                                                                                     AS row_count,
	COUNT(DISTINCT user_deal_events.user_id)                                                     AS users,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'deal-view', user_deal_events.user_id, NULL)) AS deal_view__users,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'book-form', user_deal_events.user_id, NULL)) AS book_form__users,
	COUNT(DISTINCT IFF(user_deal_events.evt_name = 'order', user_deal_events.user_id, NULL))     AS orders__users
FROM data_science.predictive_modeling.user_deal_events
WHERE user_deal_events.evt_date >= CURRENT_DATE - 10
GROUP BY ALL
;