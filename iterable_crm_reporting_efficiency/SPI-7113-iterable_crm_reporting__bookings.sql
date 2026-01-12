USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking
	CLONE data_vault_mvp.dwh.iterable_crm_reporting__booking
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.email.crm_reporting.iterable.iterable_crm_reporting__bookings.py' \
    --method 'run' \
    --start '2025-07-15 00:00:00' \
    --end '2025-07-15 00:00:00'\

SELECT
	attribution_model,
	campaign_id,
	message_id,
	touch_id,
	booking_id,
	booking_event_date,
	margin_gbp,
	gross_revenue_gbp,
	check_in_date,
	check_out_date,
	shiro_user_id,
	stitched_identity_type,
	app_push_open_context,
	travel_type,
	product_type,
	los,
	booking_domestic,
	margin_gbp_domestic,
	gross_revenue_gbp_domestic,
	los_domestic,
	booking_international,
	margin_gbp_international,
	gross_revenue_gbp_international,
	los_international,
	booking_hotel,
	margin_gbp_hotel,
	gross_revenue_gbp_hotel,
	los_hotel,
	booking_package,
	margin_gbp_package,
	gross_revenue_gbp_package,
	los_package
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking__step02__stack_booking_attribution_models
WHERE booking_id = 'A23926408'
;

SELECT DATEDIFF(DAY, CURRENT_DATE, CURRENT_DATE)



SELECT
	stacked_bookings.attribution_model,
	stacked_bookings.campaign_id,
	COALESCE(send_events1.message_id, send_events2.message_id)                       AS message_id,
	COALESCE(send_events1.message_id_email_hash, send_events2.message_id_email_hash) AS message_id_email_hash,
	stacked_bookings.shiro_user_id,
	stacked_bookings.booking_id,

	DATEDIFF(DAY, COALESCE(send_events1.send_start_date, send_events2.send_start_date),
			 stacked_bookings.booking_event_date)                                    AS days_from_send,
	LOWER(stacked_bookings.travel_type) IS NOT DISTINCT FROM 'domestic'              AS is_domestic,
	LOWER(stacked_bookings.travel_type) IS NOT DISTINCT FROM 'international'         AS is_international,
	LOWER(stacked_bookings.product_type) IS NOT DISTINCT FROM 'hotel'                AS is_hotel,
	LOWER(stacked_bookings.product_type) IS NOT DISTINCT FROM 'package'              AS is_package,

	-- bookings

	IFF(days_from_send = 0, 1, 0)                                                    AS bookings_same_day,
	IFF(days_from_send <= 1, 1, 0)                                                   AS bookings_1d,
	IFF(days_from_send <= 7, 1, 0)                                                   AS bookings_7d,
	IFF(days_from_send <= 28, 1, 0)                                                  AS bookings_28d,

	IFF(days_from_send = 0 AND is_domestic, 1, 0)                                    AS bookings_domestic_same_day,
	IFF(days_from_send <= 1 AND is_domestic, 1, 0)                                   AS bookings_domestic_1d,
	IFF(days_from_send <= 7 AND is_domestic, 1, 0)                                   AS bookings_domestic_7d,
	IFF(days_from_send <= 28 AND is_domestic, 1, 0)                                  AS bookings_domestic_28d,

	IFF(days_from_send = 0 AND is_international, 1, 0)                               AS bookings_international_same_day,
	IFF(days_from_send <= 1 AND is_international, 1, 0)                              AS bookings_international_1d,
	IFF(days_from_send <= 7 AND is_international, 1, 0)                              AS bookings_international_7d,
	IFF(days_from_send <= 28 AND is_international, 1, 0)                             AS bookings_international_28d,

	IFF(days_from_send = 0 AND is_hotel, 1, 0)                                       AS bookings_hotel_same_day,
	IFF(days_from_send <= 1 AND is_hotel, 1, 0)                                      AS bookings_hotel_1d,
	IFF(days_from_send <= 7 AND is_hotel, 1, 0)                                      AS bookings_hotel_7d,
	IFF(days_from_send <= 28 AND is_hotel, 1, 0)                                     AS bookings_hotel_28d,

	IFF(days_from_send = 0 AND is_package, 1, 0)                                     AS bookings_package_same_day,
	IFF(days_from_send <= 1 AND is_package, 1, 0)                                    AS bookings_package_1d,
	IFF(days_from_send <= 7 AND is_package, 1, 0)                                    AS bookings_package_7d,
	IFF(days_from_send <= 28 AND is_package, 1, 0)                                   AS bookings_package_28d,

	-- margin

	IFF(days_from_send = 0, stacked_bookings.margin_gbp, 0)                          AS margin_gbp_same_day,
	IFF(days_from_send <= 1, stacked_bookings.margin_gbp, 0)                         AS margin_gbp_1d,
	IFF(days_from_send <= 7, stacked_bookings.margin_gbp, 0)                         AS margin_gbp_7d,
	IFF(days_from_send <= 28, stacked_bookings.margin_gbp, 0)                        AS margin_gbp_28d,

	IFF(days_from_send = 0 AND is_domestic, stacked_bookings.margin_gbp, 0)          AS margin_gbp_domestic_same_day,
	IFF(days_from_send <= 1 AND is_domestic, stacked_bookings.margin_gbp, 0)         AS margin_gbp_domestic_1d,
	IFF(days_from_send <= 7 AND is_domestic, stacked_bookings.margin_gbp, 0)         AS margin_gbp_domestic_7d,
	IFF(days_from_send <= 28 AND is_domestic, stacked_bookings.margin_gbp, 0)        AS margin_gbp_domestic_28d,

	IFF(days_from_send = 0 AND is_international, stacked_bookings.margin_gbp,
		0)                                                                           AS margin_gbp_international_same_day,
	IFF(days_from_send <= 1 AND is_international, stacked_bookings.margin_gbp,
		0)                                                                           AS margin_gbp_international_1d,
	IFF(days_from_send <= 7 AND is_international, stacked_bookings.margin_gbp,
		0)                                                                           AS margin_gbp_international_7d,
	IFF(days_from_send <= 28 AND is_international, stacked_bookings.margin_gbp,
		0)                                                                           AS margin_gbp_international_28d,

	IFF(days_from_send = 0 AND is_hotel, stacked_bookings.margin_gbp, 0)             AS margin_gbp_hotel_same_day,
	IFF(days_from_send <= 1 AND is_hotel, stacked_bookings.margin_gbp, 0)            AS margin_gbp_hotel_1d,
	IFF(days_from_send <= 7 AND is_hotel, stacked_bookings.margin_gbp, 0)            AS margin_gbp_hotel_7d,
	IFF(days_from_send <= 28 AND is_hotel, stacked_bookings.margin_gbp, 0)           AS margin_gbp_hotel_28d,

	IFF(days_from_send = 0 AND is_package, stacked_bookings.margin_gbp, 0)           AS margin_gbp_package_same_day,
	IFF(days_from_send <= 1 AND is_package, stacked_bookings.margin_gbp, 0)          AS margin_gbp_package_1d,
	IFF(days_from_send <= 7 AND is_package, stacked_bookings.margin_gbp, 0)          AS margin_gbp_package_7d,
	IFF(days_from_send <= 28 AND is_package, stacked_bookings.margin_gbp, 0)         AS margin_gbp_package_28d,

	-- gross revenue

	IFF(days_from_send = 0, stacked_bookings.gross_revenue_gbp, 0)                   AS gross_revenue_gbp_same_day,
	IFF(days_from_send <= 1, stacked_bookings.gross_revenue_gbp, 0)                  AS gross_revenue_gbp_1d,
	IFF(days_from_send <= 7, stacked_bookings.gross_revenue_gbp, 0)                  AS gross_revenue_gbp_7d,
	IFF(days_from_send <= 28, stacked_bookings.gross_revenue_gbp, 0)                 AS gross_revenue_gbp_28d,

	IFF(days_from_send = 0 AND is_domestic, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_domestic_same_day,
	IFF(days_from_send <= 1 AND is_domestic, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_domestic_1d,
	IFF(days_from_send <= 7 AND is_domestic, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_domestic_7d,
	IFF(days_from_send <= 28 AND is_domestic, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_domestic_28d,

	IFF(days_from_send = 0 AND is_international, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_international_same_day,
	IFF(days_from_send <= 1 AND is_international, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_international_1d,
	IFF(days_from_send <= 7 AND is_international, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_international_7d,
	IFF(days_from_send <= 28 AND is_international, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_international_28d,

	IFF(days_from_send = 0 AND is_hotel, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_hotel_same_day,
	IFF(days_from_send <= 1 AND is_hotel, stacked_bookings.gross_revenue_gbp, 0)     AS gross_revenue_gbp_hotel_1d,
	IFF(days_from_send <= 7 AND is_hotel, stacked_bookings.gross_revenue_gbp, 0)     AS gross_revenue_gbp_hotel_7d,
	IFF(days_from_send <= 28 AND is_hotel, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_hotel_28d,

	IFF(days_from_send = 0 AND is_package, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_package_same_day,
	IFF(days_from_send <= 1 AND is_package, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_package_1d,
	IFF(days_from_send <= 7 AND is_package, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_package_7d,
	IFF(days_from_send <= 28 AND is_package, stacked_bookings.gross_revenue_gbp,
		0)                                                                           AS gross_revenue_gbp_package_28d,

	-- length of stay (los) -- all are within 28 days of send

	IFF(days_from_send <= 28, stacked_bookings.los, 0)                               AS los,
	IFF(days_from_send <= 28 AND is_domestic, stacked_bookings.los, 0)               AS los_domestic,
	IFF(days_from_send <= 28 AND is_international, stacked_bookings.los, 0)          AS los_international,
	IFF(days_from_send <= 28 AND is_hotel, stacked_bookings.los, 0)                  AS los_hotel,
	IFF(days_from_send <= 28 AND is_package, stacked_bookings.los, 0)                AS los_package,

FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking__step02__stack_booking_attribution_models stacked_bookings
	-- with message id
	LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends send_events1
			  ON stacked_bookings.message_id = send_events1.message_id
				  AND stacked_bookings.campaign_id = send_events1.campaign_id::VARCHAR
				  AND stacked_bookings.shiro_user_id = send_events1.shiro_user_id
				  AND send_events1.send_event_date::DATE >= DATEADD('day', -28, '2025-07-13 04:30:00'::DATE)
				  -- without messsage id
	ASOF JOIN (
				  SELECT *
				  FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
				  WHERE send_event_date::DATE >= DATEADD('day', -28, '2025-07-13 04:30:00'::DATE)
			  ) send_events2
			  MATCH_CONDITION (stacked_bookings.booking_event_date >= send_events2.send_start_date)
			  ON stacked_bookings.campaign_id = send_events2.campaign_id::VARCHAR
				  AND stacked_bookings.shiro_user_id = send_events2.shiro_user_id
--                 AND send_events2.send_event_date::DATE >= DATEADD('day', -28, '2025-07-13 04:30:00'::DATE)
-- Additionl efficiency filter for when we need to run historic backfills
WHERE DATEDIFF(DAY, COALESCE(send_events1.send_start_date, send_events2.send_start_date),
			   stacked_bookings.booking_event_date) <= 28

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.asof_join_test AS
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking__step02__stack_booking_attribution_models stacked_bookings
WHERE stacked_bookings.message_id IS NULL
  AND stacked_bookings.attribution_model = 'last click'
;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.asof_join_test_recent_sends AS
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__sends
WHERE send_event_date::DATE >= DATEADD('day', -28, '2025-07-13 04:30:00'::DATE)
;

USE WAREHOUSE pipe_xlarge
;

SELECT
	stacked_bookings.campaign_id,
	stacked_bookings.shiro_user_id,
	stacked_bookings.booking_id,
	stacked_bookings.message_id,
	stacked_bookings.booking_event_date,
	stacked_bookings.margin_gbp,
	stacked_bookings.gross_revenue_gbp,
	send_events2.message_id

FROM scratch.robinpatel.asof_join_test stacked_bookings
	ASOF JOIN scratch.robinpatel.asof_join_test_recent_sends send_events2
			  MATCH_CONDITION (stacked_bookings.booking_event_date >= send_events2.send_start_date)
			  ON stacked_bookings.campaign_id = send_events2.campaign_id::VARCHAR
				  AND stacked_bookings.shiro_user_id = send_events2.shiro_user_id
;

WITH
	model_data AS (
		SELECT
			stacked_bookings.campaign_id,
			stacked_bookings.shiro_user_id,
			stacked_bookings.booking_id,
			stacked_bookings.message_id,
			stacked_bookings.booking_event_date,
			stacked_bookings.margin_gbp,
			stacked_bookings.gross_revenue_gbp,
			send_events2.message_id

		FROM scratch.robinpatel.asof_join_test stacked_bookings
			INNER JOIN scratch.robinpatel.asof_join_test_recent_sends send_events2
					   ON stacked_bookings.campaign_id = send_events2.campaign_id::VARCHAR
						   AND stacked_bookings.shiro_user_id = send_events2.shiro_user_id
	)
SELECT
	model_data.campaign_id,
	model_data.shiro_user_id,
	COUNT(*)
FROM model_data
GROUP BY model_data.campaign_id,
		 model_data.shiro_user_id
HAVING COUNT(*) > 1
;

-- example of a null message id booking that has a auto campaign, and joining to the most appropriate campaign
-- CAMPAIGN_ID	SHIRO_USER_ID
-- 13309850	20128313.00000

SELECT
	campaign_id,
	shiro_user_id,
	send_event_date,
	message_id
FROM scratch.robinpatel.asof_join_test_recent_sends
WHERE asof_join_test_recent_sends.campaign_id = 13309850 AND asof_join_test_recent_sends.shiro_user_id = '20128313'



SELECT
	stacked_bookings.campaign_id,
	stacked_bookings.shiro_user_id,
	stacked_bookings.booking_id,
	stacked_bookings.message_id,
	stacked_bookings.booking_event_date,
	stacked_bookings.margin_gbp,
	stacked_bookings.gross_revenue_gbp,
	send_events2.message_id

FROM scratch.robinpatel.asof_join_test stacked_bookings
	ASOF JOIN scratch.robinpatel.asof_join_test_recent_sends send_events2
			  MATCH_CONDITION (stacked_bookings.booking_event_date >= send_events2.send_start_date)
			  ON stacked_bookings.campaign_id = send_events2.campaign_id::VARCHAR
				  AND stacked_bookings.shiro_user_id = send_events2.shiro_user_id
WHERE stacked_bookings.shiro_user_id = '20128313'
  AND stacked_bookings.campaign_id = '13309850'

-- need to make a send filter table as no disjunction conditions allowed

USE WAREHOUSE pipe_xlarge
;


CAMPAIGN_ID	SHIRO_USER_ID
13583413	5427803.00000


SELECT
	campaign_id,
	shiro_user_id,
	send_event_date,
	message_id
FROM scratch.robinpatel.asof_join_test_recent_sends
WHERE asof_join_test_recent_sends.campaign_id = 13583413 AND asof_join_test_recent_sends.shiro_user_id = '5427803'
;



SELECT
	stacked_bookings.campaign_id,
	stacked_bookings.shiro_user_id,
	stacked_bookings.booking_id,
	stacked_bookings.message_id,
	stacked_bookings.booking_event_date,
	stacked_bookings.margin_gbp,
	stacked_bookings.gross_revenue_gbp,
	send_events2.message_id

FROM scratch.robinpatel.asof_join_test stacked_bookings
	ASOF JOIN scratch.robinpatel.asof_join_test_recent_sends send_events2
			  MATCH_CONDITION (stacked_bookings.booking_event_date >= send_events2.send_start_date)
			  ON stacked_bookings.campaign_id = send_events2.campaign_id::VARCHAR
				  AND stacked_bookings.shiro_user_id = send_events2.shiro_user_id
WHERE stacked_bookings.shiro_user_id = '5427803'
  AND stacked_bookings.campaign_id = '13583413'
;

SELECT
	attribution_model,
	campaign_id,
	message_id,
	message_id_email_hash,
	shiro_user_id,
	booking_id,
	days_from_send,
	is_domestic,
	is_international,
	is_hotel,
	is_package,
	bookings_same_day,
	bookings_1d,
	bookings_7d,
	bookings_28d,
	bookings_domestic_same_day,
	bookings_domestic_1d,
	bookings_domestic_7d,
	bookings_domestic_28d,
	bookings_international_same_day,
	bookings_international_1d,
	bookings_international_7d,
	bookings_international_28d,
	bookings_hotel_same_day,
	bookings_hotel_1d,
	bookings_hotel_7d,
	bookings_hotel_28d,
	bookings_package_same_day,
	bookings_package_1d,
	bookings_package_7d,
	bookings_package_28d,
	margin_gbp_same_day,
	margin_gbp_1d,
	margin_gbp_7d,
	margin_gbp_28d,
	margin_gbp_domestic_same_day,
	margin_gbp_domestic_1d,
	margin_gbp_domestic_7d,
	margin_gbp_domestic_28d,
	margin_gbp_international_same_day,
	margin_gbp_international_1d,
	margin_gbp_international_7d,
	margin_gbp_international_28d,
	margin_gbp_hotel_same_day,
	margin_gbp_hotel_1d,
	margin_gbp_hotel_7d,
	margin_gbp_hotel_28d,
	margin_gbp_package_same_day,
	margin_gbp_package_1d,
	margin_gbp_package_7d,
	margin_gbp_package_28d,
	gross_revenue_gbp_same_day,
	gross_revenue_gbp_1d,
	gross_revenue_gbp_7d,
	gross_revenue_gbp_28d,
	gross_revenue_gbp_domestic_same_day,
	gross_revenue_gbp_domestic_1d,
	gross_revenue_gbp_domestic_7d,
	gross_revenue_gbp_domestic_28d,
	gross_revenue_gbp_international_same_day,
	gross_revenue_gbp_international_1d,
	gross_revenue_gbp_international_7d,
	gross_revenue_gbp_international_28d,
	gross_revenue_gbp_hotel_same_day,
	gross_revenue_gbp_hotel_1d,
	gross_revenue_gbp_hotel_7d,
	gross_revenue_gbp_hotel_28d,
	gross_revenue_gbp_package_same_day,
	gross_revenue_gbp_package_1d,
	gross_revenue_gbp_package_7d,
	gross_revenue_gbp_package_28d,
	los,
	los_domestic,
	los_international,
	los_hotel,
	los_package
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking__step03__model_data
WHERE send_event_date >


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking__step02__stack_booking_attribution_models
WHERE iterable_crm_reporting__booking__step02__stack_booking_attribution_models.booking_id = 'A23939123'


SELECT
	iterable_crm_reporting__booking__step02__stack_booking_attribution_models.attribution_model,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking__step02__stack_booking_attribution_models
GROUP BY 1

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking__step04__attach_send_data
WHERE iterable_crm_reporting__booking__step04__attach_send_data.bookings_7d_lc !=
	  iterable_crm_reporting__booking__step04__attach_send_data.bookings_7d_lnd
;



SELECT
	sta.*,
	stmc.touch_mkt_channel,
	stmc2.touch_mkt_channel AS lc_touch_mkt_channel,
FROM se.data.scv_touch_attribution sta
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON sta.attributed_touch_id = stmc.touch_id
	INNER JOIN se.data.scv_touch_marketing_channel stmc2 ON sta.touch_id = stmc2.touch_id
WHERE sta.touch_id = '3637a73f76243e4eee2b32ef3a67d12057c34affe31d04945290d6ba32bbe5ed'
  AND sta.touch_start_tstamp >= CURRENT_DATE - 30


SELECT
	stmc.touch_mkt_channel AS last_click_channel,
	stba.app_push_open_context,

FROM se.data_pii.scv_touch_basic_attributes stba
	INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
	AND stmc.touch_start_tstamp >= CURRENT_DATE - 30
WHERE stba.touch_start_tstamp >= CURRENT_DATE - 30
  AND stmc.touch_mkt_channel = 'Direct'
  AND stba.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL
;


SELECT
	message_id
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking__step05__aggregate_bookings icrbs04asd
WHERE send_event_date = '2025-07-06'
  AND campaign_id = 14094689
EXCEPT
SELECT
	icr.message_id
FROM data_vault_mvp.dwh.iterable_crm_reporting icr
WHERE icr.send_event_date = '2025-07-06'
  AND icr.campaign_id = 14094689
  AND (icr.bookings_lc > 0 OR icr.bookings_lnd > 0)


;

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking__step05__aggregate_bookings')
;

CREATE OR REPLACE TRANSIENT TABLE iterable_crm_reporting__booking__step05__aggregate_bookings
(
	message_id_email_hash VARCHAR NOT NULL PRIMARY KEY,
	message_id VARCHAR,
	campaign_id VARCHAR,
	send_event_date DATE,
	shiro_user_id NUMBER,
	booking_id_array_lc ARRAY,
	bookings_same_day_lc NUMBER,
	bookings_1d_lc NUMBER,
	bookings_7d_lc NUMBER,
	bookings_28d_lc NUMBER,
	bookings_domestic_same_day_lc NUMBER,
	bookings_domestic_1d_lc NUMBER,
	bookings_domestic_7d_lc NUMBER,
	bookings_domestic_28d_lc NUMBER,
	bookings_international_same_day_lc NUMBER,
	bookings_international_1d_lc NUMBER,
	bookings_international_7d_lc NUMBER,
	bookings_international_28d_lc NUMBER,
	bookings_hotel_same_day_lc NUMBER,
	bookings_hotel_1d_lc NUMBER,
	bookings_hotel_7d_lc NUMBER,
	bookings_hotel_28d_lc NUMBER,
	bookings_package_same_day_lc NUMBER,
	bookings_package_1d_lc NUMBER,
	bookings_package_7d_lc NUMBER,
	bookings_package_28d_lc NUMBER,
	margin_gbp_same_day_lc NUMBER,
	margin_gbp_1d_lc NUMBER,
	margin_gbp_7d_lc NUMBER,
	margin_gbp_28d_lc NUMBER,
	margin_gbp_domestic_same_day_lc NUMBER,
	margin_gbp_domestic_1d_lc NUMBER,
	margin_gbp_domestic_7d_lc NUMBER,
	margin_gbp_domestic_28d_lc NUMBER,
	margin_gbp_international_same_day_lc NUMBER,
	margin_gbp_international_1d_lc NUMBER,
	margin_gbp_international_7d_lc NUMBER,
	margin_gbp_international_28d_lc NUMBER,
	margin_gbp_hotel_same_day_lc NUMBER,
	margin_gbp_hotel_1d_lc NUMBER,
	margin_gbp_hotel_7d_lc NUMBER,
	margin_gbp_hotel_28d_lc NUMBER,
	margin_gbp_package_same_day_lc NUMBER,
	margin_gbp_package_1d_lc NUMBER,
	margin_gbp_package_7d_lc NUMBER,
	margin_gbp_package_28d_lc NUMBER,
	gross_revenue_gbp_same_day_lc NUMBER,
	gross_revenue_gbp_1d_lc NUMBER,
	gross_revenue_gbp_7d_lc NUMBER,
	gross_revenue_gbp_28d_lc NUMBER,
	gross_revenue_gbp_domestic_same_day_lc NUMBER,
	gross_revenue_gbp_domestic_1d_lc NUMBER,
	gross_revenue_gbp_domestic_7d_lc NUMBER,
	gross_revenue_gbp_domestic_28d_lc NUMBER,
	gross_revenue_gbp_international_same_day_lc NUMBER,
	gross_revenue_gbp_international_1d_lc NUMBER,
	gross_revenue_gbp_international_7d_lc NUMBER,
	gross_revenue_gbp_international_28d_lc NUMBER,
	gross_revenue_gbp_hotel_same_day_lc NUMBER,
	gross_revenue_gbp_hotel_1d_lc NUMBER,
	gross_revenue_gbp_hotel_7d_lc NUMBER,
	gross_revenue_gbp_hotel_28d_lc NUMBER,
	gross_revenue_gbp_package_same_day_lc NUMBER,
	gross_revenue_gbp_package_1d_lc NUMBER,
	gross_revenue_gbp_package_7d_lc NUMBER,
	gross_revenue_gbp_package_28d_lc NUMBER,
	booking_id_array_lnd ARRAY,
	bookings_same_day_lnd NUMBER,
	bookings_1d_lnd NUMBER,
	bookings_7d_lnd NUMBER,
	bookings_28d_lnd NUMBER,
	bookings_domestic_same_day_lnd NUMBER,
	bookings_domestic_1d_lnd NUMBER,
	bookings_domestic_7d_lnd NUMBER,
	bookings_domestic_28d_lnd NUMBER,
	bookings_international_same_day_lnd NUMBER,
	bookings_international_1d_lnd NUMBER,
	bookings_international_7d_lnd NUMBER,
	bookings_international_28d_lnd NUMBER,
	bookings_hotel_same_day_lnd NUMBER,
	bookings_hotel_1d_lnd NUMBER,
	bookings_hotel_7d_lnd NUMBER,
	bookings_hotel_28d_lnd NUMBER,
	bookings_package_same_day_lnd NUMBER,
	bookings_package_1d_lnd NUMBER,
	bookings_package_7d_lnd NUMBER,
	bookings_package_28d_lnd NUMBER,
	margin_gbp_same_day_lnd NUMBER,
	margin_gbp_1d_lnd NUMBER,
	margin_gbp_7d_lnd NUMBER,
	margin_gbp_28d_lnd NUMBER,
	margin_gbp_domestic_same_day_lnd NUMBER,
	margin_gbp_domestic_1d_lnd NUMBER,
	margin_gbp_domestic_7d_lnd NUMBER,
	margin_gbp_domestic_28d_lnd NUMBER,
	margin_gbp_international_same_day_lnd NUMBER,
	margin_gbp_international_1d_lnd NUMBER,
	margin_gbp_international_7d_lnd NUMBER,
	margin_gbp_international_28d_lnd NUMBER,
	margin_gbp_hotel_same_day_lnd NUMBER,
	margin_gbp_hotel_1d_lnd NUMBER,
	margin_gbp_hotel_7d_lnd NUMBER,
	margin_gbp_hotel_28d_lnd NUMBER,
	margin_gbp_package_same_day_lnd NUMBER,
	margin_gbp_package_1d_lnd NUMBER,
	margin_gbp_package_7d_lnd NUMBER,
	margin_gbp_package_28d_lnd NUMBER,
	gross_revenue_gbp_same_day_lnd NUMBER,
	gross_revenue_gbp_1d_lnd NUMBER,
	gross_revenue_gbp_7d_lnd NUMBER,
	gross_revenue_gbp_28d_lnd NUMBER,
	gross_revenue_gbp_domestic_same_day_lnd NUMBER,
	gross_revenue_gbp_domestic_1d_lnd NUMBER,
	gross_revenue_gbp_domestic_7d_lnd NUMBER,
	gross_revenue_gbp_domestic_28d_lnd NUMBER,
	gross_revenue_gbp_international_same_day_lnd NUMBER,
	gross_revenue_gbp_international_1d_lnd NUMBER,
	gross_revenue_gbp_international_7d_lnd NUMBER,
	gross_revenue_gbp_international_28d_lnd NUMBER,
	gross_revenue_gbp_hotel_same_day_lnd NUMBER,
	gross_revenue_gbp_hotel_1d_lnd NUMBER,
	gross_revenue_gbp_hotel_7d_lnd NUMBER,
	gross_revenue_gbp_hotel_28d_lnd NUMBER,
	gross_revenue_gbp_package_same_day_lnd NUMBER,
	gross_revenue_gbp_package_1d_lnd NUMBER,
	gross_revenue_gbp_package_7d_lnd NUMBER,
	gross_revenue_gbp_package_28d_lnd NUMBER,
	los NUMBER,
	los_domestic NUMBER,
	los_international NUMBER,
	los_hotel NUMBER,
	los_package NUMBER,
)
;


SELECT * FROm data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking;
DROP TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking;



self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__bookings.py'  --method 'run' --start '2021-11-03 00:00:00' --end '2021-11-03 00:00:00'
self_describing_task --include 'biapp/task_catalogue/dv/dwh/email/crm_reporting/iterable/iterable_crm_reporting__bookings.py'  --method 'run' --start '2025-07-15 00:00:00' --end '2025-07-15 00:00:00'

ALTER TABLE data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__booking RENAME TO data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__bookings;