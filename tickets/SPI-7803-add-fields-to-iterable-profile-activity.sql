-- module=/biapp/task_catalogue/dv/dwh/iterable/user_profile_activity.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS dbt_dev_robin.bi_customer_insight
;

CREATE OR REPLACE TRANSIENT TABLE dbt_dev_robin.bi_customer_insight.ci_ab_test_log_union
	CLONE dbt.bi_customer_insight.ci_ab_test_log_union
;

CREATE OR REPLACE TRANSIENT TABLE dbt_dev_robin.bi_customer_insight.ci_ab_test_member_history_table_union
	CLONE dbt.bi_customer_insight.ci_ab_test_member_history_table_union
;

CREATE OR REPLACE TRANSIENT TABLE dbt_dev_robin.bi_customer_insight.ci_rfv_segments
	CLONE dbt.bi_customer_insight.ci_rfv_segments
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.city_translation
	CLONE latest_vault.cms_mysql.city_translation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.country_translation
	CLONE latest_vault.cms_mysql.country_translation
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

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.favorite
	CLONE latest_vault.cms_mysql.favorite
;

CREATE SCHEMA IF NOT EXISTS data_science_dev_robin.nextoken_prod
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.nextoken_prod.kronos_refined_deals
	CLONE data_science.nextoken_prod.kronos_refined_deals
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.mention_me__profile_activity
	CLONE data_vault_mvp.dwh.mention_me__profile_activity
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates
	CLONE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit
	CLONE data_vault_mvp.dwh.se_credit
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags
	CLONE data_vault_mvp.dwh.se_sale_tags
;

CREATE SCHEMA IF NOT EXISTS data_science_dev_robin.test
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.test.sunday_deals_reordered
	CLONE data_science.test.sunday_deals_reordered
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review
	CLONE data_vault_mvp.dwh.user_booking_review
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities
	CLONE data_vault_mvp.dwh.user_recent_activities
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_subscription_event
	CLONE data_vault_mvp.dwh.user_subscription_event
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.wish_list
	CLONE latest_vault.cms_mysql.wish_list
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.wish_list_item
	CLONE latest_vault.cms_mysql.wish_list_item
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
	CLONE data_vault_mvp.dwh.iterable__user_profile_activity
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.iterable.user_profile_activity.py' \
    --method 'run' \
    --start '2025-10-09 00:00:00' \
    --end '2025-10-09 00:00:00'

SELECT *
FROM data_vault_mvp.dwh.user_last_pageview ulp
;


SELECT TO_TIMESTAMP('2025-09-23 03:30:25.000000000') <> NULL
;


SELECT *
FROM data_vault_mvp.dwh.user_last_spv uls
QUALIFY COUNT(*) OVER (PARTITION BY uls.shiro_user_id) > 1



SELECT *
FROM data_vault_mvp.dwh.user_subscription_event u
	use role pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities_20251009 CLONE data_vault_mvp.dwh.user_recent_activities
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_last_spv_20251009 CLONE data_vault_mvp.dwh.user_last_spv
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.user_last_pageview_20251009 CLONE data_vault_mvp.dwh.user_last_pageview
;


-- prod
SELECT
	YEAR(last_activity_tstamp),
	COUNT(*)
FROM data_vault_mvp.dwh.user_recent_activities
GROUP BY ALL
;

-- backup
SELECT
	YEAR(last_activity_tstamp),
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.user_recent_activities_20251009
GROUP BY ALL
;



WITH
	last_opt_out_event AS (
		SELECT
			u.user_id           AS shiro_user_id,
			MAX(u.event_tstamp) AS last_opt_out_event
		FROM data_vault_mvp.dwh.user_subscription_event u
		WHERE u.subscription_type = 0
		GROUP BY u.user_id
	)
		,
	last_opt_in_event AS (
		SELECT
			u.user_id           AS shiro_user_id,
			MAX(u.event_tstamp) AS last_opt_in_event
		FROM data_vault_mvp.dwh.user_subscription_event u
		WHERE u.subscription_type IS DISTINCT FROM 0
		GROUP BY u.user_id
	)
		,
	model_data AS (
		SELECT
			ua.shiro_user_id,
			ua.email_opt_in,
			ua.email_opt_in_status,
			looe.last_opt_out_event,
			loie.last_opt_in_event,
			ua.membership_account_status
		FROM data_vault_mvp.dwh.user_attributes ua
		LEFT JOIN last_opt_out_event looe
			ON ua.shiro_user_id = looe.shiro_user_id
		LEFT JOIN last_opt_in_event loie
			ON ua.shiro_user_id = loie.shiro_user_id
		WHERE ua.email_opt_in = 0
	)
SELECT
	last_opt_out_event IS NOT NULL,
	COUNT(*)
FROM model_data
WHERE model_data.membership_account_status IS DISTINCT FROM 'DELETED'
GROUP BY ALL
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step26__explicitly_opted_out_users


------------------------------------------------------------------------------------------------------------------------
SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	shiro_user_id,
	last_sale_pageview_tstamp
FROM data_vault_mvp.dwh.user_last_spv
;


-- 19,503,378 -- via using opt out events (which is 99.9% of all deleted users) - so can't be accurate
-- 15,115,043 -- via using opt in events

-- using logic of if a currently opted out user was ever opted in then they must have actively unsubscribed


/*
LAST_OPT_IN_EVENT IS NOT NULL	COUNT(*)
TRUE	15115043
FALSE	4392331
*/

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_subscription_event CLONE data_vault_mvp.dwh.user_subscription_event
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step25__mention_me
;

SELECT
	iterable__user_profile_activity.is_legitimate_interest_eligible,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
GROUP BY 1
;


SELECT
	iterable__user_profile_activity.last_app_session_end_tstamp IS NOT NULL,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
GROUP BY 1

SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_activity
;


SELECT
	record['dataFields']['userActivity']['isLegitimateInterestEligible'],
	COUNT(*)
FROM unload_vault_mvp_dev_robin.iterable.user_profile_activity_01__20251011t040000__daily_at_04h00
GROUP BY 1

USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable__user_profile_activity_20251013 CLONE data_vault_mvp.dwh.iterable__user_profile_activity
;


CREATE OR REPLACE TABLE data_vault_mvp.dwh.iterable__user_profile_activity
(
	-- (lineage) metadata for the current job
	schedule_tstamp                           TIMESTAMP,
	run_tstamp                                TIMESTAMP,
	operation_id                              VARCHAR,
	created_at                                TIMESTAMP,
	updated_at                                TIMESTAMP,

	shiro_user_id                             INT,
	row_hash                                  VARCHAR,
	current_affiliate_territory               VARCHAR,
	membership_account_status                 VARCHAR,
	daily_opt_in                              BOOLEAN,
	weekly_opt_in                             BOOLEAN,
	user_has_ranks                            BOOLEAN,
	last_email_open_tstamp                    TIMESTAMP,
	last_email_click_tstamp                   TIMESTAMP,
	last_sale_pageview_tstamp                 TIMESTAMP,
	last_purchase_tstamp                      TIMESTAMP,
	penultimate_spv_tstamp                    TIMESTAMP,
	days_between_penultimate_and_current_spv  NUMBER,
	most_recent_non_purchase_activity         TIMESTAMP,
	daily_spv_deals                           ARRAY,
	weekly_spv_deals                          ARRAY,
	monthly_spv_deals                         ARRAY,
	sales_page_views_last_7_days              ARRAY,
	sales_page_views_last_14_days             ARRAY,
	sales_page_views_last_30_days             ARRAY,
	sales_page_views_last_60_days             ARRAY,
	segment_name                              VARCHAR,
	athena_segment_name                       VARCHAR,
	signup_tstamp                             TIMESTAMP,
	last_session_end_tstamp                   TIMESTAMP,
	last_app_session_end_tstamp               TIMESTAMP,
	user_last_updated_tstamp                  TIMESTAMP,
	profile_last_updated_tstamp               TIMESTAMP,
	last_voucher_purchase_tstamp              TIMESTAMP,
	latest_cash_credit_expiration_tstamp      TIMESTAMP,
	user_inactivity_flag                      BOOLEAN,
	user_to_be_deleted_on                     DATE,
	sale_tags_last_7_days                     ARRAY,
	sale_tags_last_30_days                    ARRAY,
	sale_tags_last_60_days                    ARRAY,
	last_booking_review_object                OBJECT,
	number_of_reviews                         NUMBER,
	nps_score                                 DECIMAL(13, 4),
	user_search_one_to_nine_results           OBJECT,
	user_search_greater_than_nine_results     OBJECT,
	is_user_search_equal                      BOOLEAN,
	sessions_last_1_days                      NUMBER,
	sessions_last_7_days                      NUMBER,
	sessions_last_14_days                     NUMBER,
	avg_spvs_per_session_last_1_7_14_days     ARRAY,
	num_sale_page_views_1_days                NUMBER,
	num_sale_page_views_7_days                NUMBER,
	num_sale_page_views_14_days               NUMBER,
	num_sale_page_views_30_days               NUMBER,
	num_sales_page_views_last_60_days         NUMBER,
	num_intl_sale_page_views_7_days           NUMBER,
	num_intl_sale_page_views_14_days          NUMBER,
	num_intl_sale_page_views_30_days          NUMBER,
	num_domestic_sale_page_views_7_days       NUMBER,
	num_domestic_sale_page_views_14_days      NUMBER,
	num_domestic_sale_page_views_30_days      NUMBER,
	top_5_sale_page_views_last_1_days         ARRAY,
	top_5_sale_page_views_last_7_days         ARRAY,
	top_5_sale_page_views_last_14_days        ARRAY,
	top_2_most_popular_tag_by_sale_page_views ARRAY,
	top_2_tag_spvs_last_7_days                ARRAY,
	top_2_tag_spvs_last_14_days               ARRAY,
	top_2_tag_spvs_last_30_days               ARRAY,
	top_2_tag_spvs_last_60_days               ARRAY,
	user_favorites_array                      ARRAY,
	user_wishlist_array                       ARRAY,
	user_has_credit                           BOOLEAN,
	user_credit_total                         OBJECT,
	user_credit_array                         ARRAY,
	recency_frequency_value                   OBJECT,
	ab_test_member_history_array              ARRAY,
	sunday_deals                              OBJECT,
	mention_me_object                         OBJECT,
	is_legitimate_interest_eligible           BOOLEAN,
	CONSTRAINT pk_iterable__user_profile_activity
		PRIMARY KEY (
					 shiro_user_id
			)
)
;

USE WAREHOUSE pipe_2xlarge
;

MERGE INTO data_vault_mvp.dwh.iterable__user_profile_activity AS target
	USING data_vault_mvp.dwh.iterable__user_profile_activity__model_data AS batch
	ON target.shiro_user_id = batch.shiro_user_id
	WHEN MATCHED AND batch.membership_account_status = 'DELETED'
		THEN DELETE
	WHEN MATCHED AND batch.current_affiliate_territory = 'US'
		THEN DELETE
	WHEN MATCHED AND batch.is_test_user = TRUE
		THEN DELETE
	WHEN MATCHED
		AND target.row_hash != batch.row_hash
			AND batch.membership_account_status IS DISTINCT FROM 'DELETED'
			AND batch.current_affiliate_territory IS DISTINCT FROM 'US'
			AND batch.is_test_user IS DISTINCT FROM TRUE
		THEN UPDATE SET
		target.schedule_tstamp = '2025-10-12 03:30:00',
		target.run_tstamp = '2025-10-13 17:38:30',
		target.operation_id =
				'ScriptOperator__/usr/local/airflow/dags/biapp/task_catalogue/dv/dwh/iterable/user_profile_activity.py__20251012T033000__daily_at_03h30',
		target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

		target.row_hash = batch.row_hash,
		target.current_affiliate_territory = batch.current_affiliate_territory,
		target.membership_account_status = batch.membership_account_status,
		target.daily_opt_in = batch.daily_opt_in,
		target.weekly_opt_in = batch.weekly_opt_in,
		target.user_has_ranks = batch.user_has_ranks,
		target.last_email_open_tstamp = batch.last_email_open_tstamp,
		target.last_email_click_tstamp = batch.last_email_click_tstamp,
		target.last_sale_pageview_tstamp = batch.last_sale_pageview_tstamp,
		target.last_purchase_tstamp = batch.last_purchase_tstamp,
		target.penultimate_spv_tstamp = batch.penultimate_spv_tstamp,
		target.days_between_penultimate_and_current_spv = batch.days_between_penultimate_and_current_spv,
		target.most_recent_non_purchase_activity = batch.most_recent_non_purchase_activity,
		target.daily_spv_deals = batch.daily_spv_deals,
		target.weekly_spv_deals = batch.weekly_spv_deals,
		target.monthly_spv_deals = batch.monthly_spv_deals,
		target.sales_page_views_last_7_days = batch.sales_page_views_last_7_days,
		target.sales_page_views_last_14_days = batch.sales_page_views_last_14_days,
		target.sales_page_views_last_30_days = batch.sales_page_views_last_30_days,
		target.sales_page_views_last_60_days = batch.sales_page_views_last_60_days,
		target.segment_name = batch.segment_name,
		target.athena_segment_name = batch.athena_segment_name,
		target.signup_tstamp = batch.signup_tstamp,
		target.last_session_end_tstamp = batch.last_session_end_tstamp,
		target.last_app_session_end_tstamp = batch.last_app_session_end_tstamp,
		target.user_last_updated_tstamp = batch.user_last_updated_tstamp,
		target.profile_last_updated_tstamp = batch.profile_last_updated_tstamp,
		target.last_voucher_purchase_tstamp = batch.last_voucher_purchase_tstamp,
		target.latest_cash_credit_expiration_tstamp = batch.latest_cash_credit_expiration_tstamp,
		target.user_inactivity_flag = batch.user_inactivity_flag,
		target.user_to_be_deleted_on = batch.user_to_be_deleted_on,
		target.sale_tags_last_7_days = batch.sale_tags_last_7_days,
		target.sale_tags_last_30_days = batch.sale_tags_last_30_days,
		target.sale_tags_last_60_days = batch.sale_tags_last_60_days,
		target.last_booking_review_object = batch.last_booking_review_object,
		target.number_of_reviews = batch.number_of_reviews,
		target.nps_score = batch.nps_score,
		target.user_search_one_to_nine_results = batch.user_search_one_to_nine_results,
		target.user_search_greater_than_nine_results = batch.user_search_greater_than_nine_results,
		target.is_user_search_equal = batch.is_user_search_equal,
		target.sessions_last_1_days = batch.sessions_last_1_days,
		target.sessions_last_7_days = batch.sessions_last_7_days,
		target.sessions_last_14_days = batch.sessions_last_14_days,
		target.avg_spvs_per_session_last_1_7_14_days = batch.avg_spvs_per_session_last_1_7_14_days,
		target.num_sale_page_views_1_days = batch.num_sale_page_views_1_days,
		target.num_sale_page_views_7_days = batch.num_sale_page_views_7_days,
		target.num_sale_page_views_14_days = batch.num_sale_page_views_14_days,
		target.num_sale_page_views_30_days = batch.num_sale_page_views_30_days,
		target.num_sales_page_views_last_60_days = batch.num_sales_page_views_last_60_days,
		target.num_intl_sale_page_views_7_days = batch.num_intl_sale_page_views_7_days,
		target.num_intl_sale_page_views_14_days = batch.num_intl_sale_page_views_14_days,
		target.num_intl_sale_page_views_30_days = batch.num_intl_sale_page_views_30_days,
		target.num_domestic_sale_page_views_7_days = batch.num_domestic_sale_page_views_7_days,
		target.num_domestic_sale_page_views_14_days = batch.num_domestic_sale_page_views_14_days,
		target.num_domestic_sale_page_views_30_days = batch.num_domestic_sale_page_views_30_days,
		target.top_5_sale_page_views_last_1_days = batch.top_5_sale_page_views_last_1_days,
		target.top_5_sale_page_views_last_7_days = batch.top_5_sale_page_views_last_7_days,
		target.top_5_sale_page_views_last_14_days = batch.top_5_sale_page_views_last_14_days,
		target.top_2_most_popular_tag_by_sale_page_views = batch.top_2_most_popular_tag_by_sale_page_views,
		target.top_2_tag_spvs_last_7_days = batch.top_2_tag_spvs_last_7_days,
		target.top_2_tag_spvs_last_14_days = batch.top_2_tag_spvs_last_14_days,
		target.top_2_tag_spvs_last_30_days = batch.top_2_tag_spvs_last_30_days,
		target.top_2_tag_spvs_last_60_days = batch.top_2_tag_spvs_last_60_days,
		target.user_favorites_array = batch.user_favorites_array,
		target.user_wishlist_array = batch.user_wishlist_array,
		target.user_has_credit = batch.user_has_credit,
		target.user_credit_total = batch.user_credit_total,
		target.user_credit_array = batch.user_credit_array,
		target.recency_frequency_value = batch.recency_frequency_value,
		target.ab_test_member_history_array = batch.ab_test_member_history_array,
		target.sunday_deals = batch.sunday_deals,
		target.mention_me_object = batch.mention_me_object,
		target.is_legitimate_interest_eligible = batch.is_legitimate_interest_eligible
	WHEN NOT MATCHED
		AND batch.membership_account_status IS DISTINCT FROM 'DELETED'
			AND batch.current_affiliate_territory IS DISTINCT FROM 'US'
			AND batch.is_test_user IS DISTINCT FROM TRUE
		THEN INSERT VALUES ('2025-10-12 03:30:00',
							'2025-10-13 17:38:30',
							'ScriptOperator__/usr/local/airflow/dags/biapp/task_catalogue/dv/dwh/iterable/user_profile_activity.py__20251012T033000__daily_at_03h30',
							CURRENT_TIMESTAMP()::TIMESTAMP,
							CURRENT_TIMESTAMP()::TIMESTAMP,
							batch.shiro_user_id,
							batch.row_hash,
							batch.current_affiliate_territory,
							batch.membership_account_status,
							batch.daily_opt_in,
							batch.weekly_opt_in,
							batch.user_has_ranks,
							batch.last_email_open_tstamp,
							batch.last_email_click_tstamp,
							batch.last_sale_pageview_tstamp,
							batch.last_purchase_tstamp,
							batch.penultimate_spv_tstamp,
							batch.days_between_penultimate_and_current_spv,
							batch.most_recent_non_purchase_activity,
							batch.daily_spv_deals,
							batch.weekly_spv_deals,
							batch.monthly_spv_deals,
							batch.sales_page_views_last_7_days,
							batch.sales_page_views_last_14_days,
							batch.sales_page_views_last_30_days,
							batch.sales_page_views_last_60_days,
							batch.segment_name,
							batch.athena_segment_name,
							batch.signup_tstamp,
							batch.last_session_end_tstamp,
							batch.last_app_session_end_tstamp,
							batch.user_last_updated_tstamp,
							batch.profile_last_updated_tstamp,
							batch.last_voucher_purchase_tstamp,
							batch.latest_cash_credit_expiration_tstamp,
							batch.user_inactivity_flag,
							batch.user_to_be_deleted_on,
							batch.sale_tags_last_7_days,
							batch.sale_tags_last_30_days,
							batch.sale_tags_last_60_days,
							batch.last_booking_review_object,
							batch.number_of_reviews,
							batch.nps_score,
							batch.user_search_one_to_nine_results,
							batch.user_search_greater_than_nine_results,
							batch.is_user_search_equal,
							batch.sessions_last_1_days,
							batch.sessions_last_7_days,
							batch.sessions_last_14_days,
							batch.avg_spvs_per_session_last_1_7_14_days,
							batch.num_sale_page_views_1_days,
							batch.num_sale_page_views_7_days,
							batch.num_sale_page_views_14_days,
							batch.num_sale_page_views_30_days,
							batch.num_sales_page_views_last_60_days,
							batch.num_intl_sale_page_views_7_days,
							batch.num_intl_sale_page_views_14_days,
							batch.num_intl_sale_page_views_30_days,
							batch.num_domestic_sale_page_views_7_days,
							batch.num_domestic_sale_page_views_14_days,
							batch.num_domestic_sale_page_views_30_days,
							batch.top_5_sale_page_views_last_1_days,
							batch.top_5_sale_page_views_last_7_days,
							batch.top_5_sale_page_views_last_14_days,
							batch.top_2_most_popular_tag_by_sale_page_views,
							batch.top_2_tag_spvs_last_7_days,
							batch.top_2_tag_spvs_last_14_days,
							batch.top_2_tag_spvs_last_30_days,
							batch.top_2_tag_spvs_last_60_days,
							batch.user_favorites_array,
							batch.user_wishlist_array,
							batch.user_has_credit,
							batch.user_credit_total,
							batch.user_credit_array,
							batch.recency_frequency_value,
							batch.ab_test_member_history_array,
							batch.sunday_deals,
							batch.mention_me_object,
							batch.is_legitimate_interest_eligible)
;

SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_activity;
