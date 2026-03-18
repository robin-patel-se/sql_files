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

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
	CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product_reservation
	CLONE latest_vault.cms_mysql.product_reservation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.reservation
	CLONE latest_vault.cms_mysql.reservation
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest
	CLONE data_vault_mvp.bi.session_metrics__events_of_interest
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.scv.session_metrics.session_metrics__events_of_interest.py' \
    --method 'run' \
    --start '2026-03-05 00:00:00' \
    --end '2026-03-05 00:00:00'
------------------------------------------------------------------------------------------------------------------------

USE WAREHOUSE pipe_xlarge
;

SELECT
	sm.touch_id,
	sm.touch_start_tstamp,
	sm.touch_affiliate_territory,
	em.*
FROM data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest__step03__agg_event_metrics em
INNER JOIN se.bi.session_metrics sm
	ON em.touch_id = sm.touch_id
	AND sm.touch_se_brand = 'SE Brand'
	AND sm.touch_start_tstamp >= '2026-01-01'
	AND sm.touch_affiliate_territory = 'UK'
WHERE em.booking_form_views_hotel_plus IS NOT NULL
;

SELECT *
FROM se.data.scv_touched_booking_form_views stbfv
INNER JOIN se.data.dim_sale ds
	ON stbfv.se_sale_id = ds.se_sale_id
	AND ds.product_configuration = 'Hotel Plus'
WHERE stbfv.event_tstamp >= '2026-01-01'
;

SELECT
	stbfv.booking_id IS NULL,
	COUNT(*)
FROM se.data.scv_touched_booking_form_views stbfv
INNER JOIN se.data.dim_sale ds
	ON stbfv.se_sale_id = ds.se_sale_id
	AND ds.product_configuration = 'Hotel Plus'
WHERE stbfv.event_tstamp >= '2026-01-01'
GROUP BY ALL
;

/*
BOOKING_ID_IS_NULL	COUNT(*)
true				342987
false				43008
  */


-- found that there aren't many hotel plus booking form views with a booking id


SELECT
	stmeoi.num_results
FROM se.data.scv_touched_module_events_of_interest stmeoi
WHERE stmeoi.event_tstamp >= '2026-01-01'
  AND stmeoi.event_subcategory = 'search'



CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest COPY GRANTS
	(
		-- (lineage) metadata for the current job
	 schedule_tstamp TIMESTAMP,
	 run_tstamp TIMESTAMP,
	 operation_id VARCHAR,
	 created_at TIMESTAMP,
	 updated_at TIMESTAMP,

		--touch metrics
	 touch_id VARCHAR,
	 spvs NUMBER,
	 unique_spvs NUMBER,
	 spvs_hotel NUMBER,
	 unique_spvs_hotel NUMBER,
	 spvs_hotel_plus NUMBER,
	 unique_spvs_hotel_plus NUMBER,
	 spvs_catalogue NUMBER,
	 unique_spvs_catalogue NUMBER,
	 spvs_package NUMBER,
	 unique_spvs_package NUMBER,
	 spvs_domestic NUMBER,
	 unique_spvs_domestic NUMBER,
	 spvs_international NUMBER,
	 unique_spvs_international NUMBER,
	 booking_form_views NUMBER,
	 booking_form_views_hotel NUMBER,
	 booking_form_views_hotel_plus NUMBER,
	 booking_form_views_catalogue NUMBER,
	 booking_form_views_package NUMBER,
	 booking_form_views_domestic NUMBER,
	 booking_form_views_international NUMBER,
	 bookings NUMBER,
	 bookings_hotel NUMBER,
	 bookings_hotel_plus NUMBER,
	 bookings_catalogue NUMBER,
	 bookings_package NUMBER,
	 bookings_domestic NUMBER,
	 bookings_international NUMBER,
	 booking_id_list VARCHAR,
	 booking_id_array ARRAY,
	 margin_gbp NUMBER,
	 margin_gbp_hotel NUMBER,
	 margin_gbp_hotel_plus NUMBER,
	 margin_gbp_catalogue NUMBER,
	 margin_gbp_package NUMBER,
	 margin_gbp_domestic NUMBER,
	 margin_gbp_international NUMBER,
	 gross_revenue_gbp NUMBER,
	 gross_revenue_gbp_hotel NUMBER,
	 gross_revenue_gbp_hotel_plus NUMBER,
	 gross_revenue_gbp_catalogue NUMBER,
	 gross_revenue_gbp_package NUMBER,
	 gross_revenue_gbp_domestic NUMBER,
	 gross_revenue_gbp_international NUMBER,
	 searches NUMBER,
	 searches_with_zero_results NUMBER,
	 avg_search_results NUMBER,
	 user_searches NUMBER,
	 user_searches_with_zero_results NUMBER,
	 avg_user_search_results NUMBER,
	 page_load_searches NUMBER,
	 page_load_searches_with_zero_results NUMBER,
	 avg_page_load_search_results NUMBER,
	 min_price_filter_searches NUMBER,
	 max_price_filter_searches NUMBER,
	 sort_by_searches NUMBER,
	 pay_button_clicks NUMBER,
	 pay_button_clicks_non_voucher NUMBER,
	 pay_button_clicks_voucher NUMBER,
		CONSTRAINT pk_session_metrics__events_of_interest
			PRIMARY KEY (
						 touch_id
				)
		)
AS
SELECT
	'2026-03-03 03:30:00',
	'2026-03-06 09:18:25',
	'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/bi/scv/session_metrics/session_metrics__events_of_interest.py__20260303T033000__daily_at_03h30',
	CURRENT_TIMESTAMP()::TIMESTAMP,
	CURRENT_TIMESTAMP()::TIMESTAMP,
	batch.touch_id,
	batch.spvs,
	batch.unique_spvs,
	batch.spvs_hotel,
	batch.unique_spvs_hotel,
	batch.spvs_hotel_plus,
	batch.unique_spvs_hotel_plus,
	batch.spvs_catalogue,
	batch.spvs_package,
	batch.unique_spvs_package,
	batch.unique_spvs_catalogue,
	batch.booking_form_views,
	batch.booking_form_views_hotel,
	batch.booking_form_views_hotel_plus,
	batch.booking_form_views_catalogue,
	batch.booking_form_views_package,
	batch.bookings,
	batch.bookings_hotel,
	batch.bookings_hotel_plus,
	batch.bookings_catalogue,
	batch.bookings_package,
	batch.booking_id_list,
	batch.booking_id_array,
	batch.margin_gbp,
	batch.gross_revenue_gbp,
	batch.searches,
	batch.user_searches,
	batch.page_load_searches,
	batch.min_price_filter_searches,
	batch.max_price_filter_searches,
	batch.sort_by_searches,
	batch.pay_button_clicks,
	batch.pay_button_clicks_non_voucher,
	batch.pay_button_clicks_voucher
FROM data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest__model_data batch
;


SELECT *
FROM data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest
;

SELECT
	stmeoi.search_context,
	stmeoi.triggered_by,
	stmeoi.search_context['page_number'],
FROM se.data.scv_touched_module_events_of_interest stmeoi
WHERE stmeoi.event_subcategory = 'search'
  AND stmeoi.event_tstamp >= CURRENT_DATE - 1
  AND stmeoi.triggered_by IS NOT NULL


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.bi
;

-- CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest
-- 	CLONE data_vault_mvp.bi.session_metrics__events_of_interest
-- ;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__feature_flags
	CLONE data_vault_mvp.bi.session_metrics__feature_flags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__login_types
	CLONE data_vault_mvp.bi.session_metrics__login_types
;

CREATE SCHEMA IF NOT EXISTS se_dev_robin.data
;

CREATE OR REPLACE VIEW se_dev_robin.data.page_url_categorisation
AS
SELECT *
FROM se.data.page_url_categorisation
;

CREATE OR REPLACE VIEW se_dev_robin.data.screen_view_classification
AS
SELECT *
FROM se.data.screen_view_classification
;

self_describing_task --include 'biapp/task_catalogue/dv/bi/scv/session_metrics/session_metrics.py'  --method 'run' --start '2022-12-01 00:00:00' --end '2022-12-01 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
;

SELECT *
FROM se_dev_robin.bi.session_metrics
;

------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;
-- DROP TABLE data_vault_mvp.bi.session_metrics_20260122;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.session_metrics_20260309 CLONE data_vault_mvp.bi.session_metrics
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.session_metrics__events_of_interest_20260309 CLONE data_vault_mvp.bi.session_metrics__events_of_interest
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.session_metrics__feature_flags_20260309 CLONE data_vault_mvp.bi.session_metrics__feature_flags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.session_metrics__login_types_20260309 CLONE data_vault_mvp.bi.session_metrics__login_types
;



DROP TABLE data_vault_mvp.bi.session_metrics
;

DROP TABLE data_vault_mvp.bi.session_metrics__events_of_interest
;

DROP TABLE data_vault_mvp.bi.session_metrics__feature_flags
;

DROP TABLE data_vault_mvp.bi.session_metrics__login_types
;

./scripts/mwaa-cli production "dags backfill --start-date '2022-11-30 00:00:00' --end-date '2022-12-01 00:00:00' --donot-pickle bi__session_metrics__daily_at_03h30"



SELECT *
FROM se.bi.session_metrics sm
WHERE sm.spvs > 0
;

SELECT *
FROM data_vault_mvp.bi.session_metrics sm

SELECT *
FROM data_vault_mvp.bi.session_metrics__events_of_interest smeoi
;


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.bi
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest
	CLONE data_vault_mvp.bi.session_metrics__events_of_interest
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__feature_flags
	CLONE data_vault_mvp.bi.session_metrics__feature_flags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.session_metrics__login_types
	CLONE data_vault_mvp.bi.session_metrics__login_types
;

CREATE SCHEMA IF NOT EXISTS se_dev_robin.data
;

self_describing_task --include 'biapp/task_catalogue/dv/bi/scv/session_metrics/session_metrics.py'  --method 'run' --start '2022-12-01 00:00:00' --end '2022-12-01 00:00:00'


SELECT
	COUNT(*)
FROM se.bi.session_metrics sm
;

SELECT
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
;

SELECT
	sm.touch_se_brand,
	sm.touch_experience,
	COUNT(*)
FROM se.bi.session_metrics sm
GROUP BY ALL
;

SELECT
	stba.touch_se_brand,
	stba.touch_experience,
	COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
GROUP BY ALL
;

SELECT *
FROM data_vault_mvp_dev_robin.bi.session_metrics
WHERE spvs > 0
;

DROP TABLE data_vault_mvp_dev_robin.bi.session_metrics
;

SELECT *
FROM se_dev_robin.bi.session_metrics
;

SELECT
	se_sale_id,
	posa,
	promo_type,
	campaign_name,
	promo_start_date,
	promo_end_date,
	send_date,

FROM latest_vault.fpa_gsheets.promotion
;


SELECT *
FROM data_vault_mvp_dev_robin.bi.session_metrics


SELECT
	SUM(sm.spvs)
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
WHERE sm.touch_affiliate_territory = 'IT' AND sm.touch_start_tstamp >= '2026-01-01';


SELECT
	SUM(sm.spvs)
FROM data_vault_mvp_dev_robin.bi.session_metrics sm
WHERE sm.touch_affiliate_territory = 'IT' AND sm.touch_start_tstamp >= '2026-01-01';

SELECT SUM(spvs) FROM data_vault_mvp.bi.session_metrics_20260309;

SELECT SUM(mtba.num_spvs) FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba;

40,030,131
880,695,309,
880,389,782

30,078,367


SELECT SUM(spvs) FROM data_vault_mvp.bi.session_metrics__events_of_interest_20260309 s;
SELECT SUM(spvs) FROM data_vault_mvp.bi.session_metrics__events_of_interest s;
SELECT SUM(spvs) FROM data_vault_mvp_dev_robin.bi.session_metrics__events_of_interest_20260309 s;


dwh__transactional__fact_booking__daily_at_01h00

./scripts/mwaa-cli production "dags backfill --start-date '2026-02-28 00:00:00' --end-date '2026-02-28 23:59:59' --donot-pickle dwh__transactional__fact_booking__daily_at_01h00"