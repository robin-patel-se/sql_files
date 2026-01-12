DROP SCHEMA data_vault_mvp_dev_robin.dwh
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS dbt_dev_robin.bi_customer_insight
;

CREATE OR REPLACE TRANSIENT TABLE dbt_dev_robin.bi_customer_insight.ci_ab_test_member_history_table_union
	CLONE dbt.bi_customer_insight.ci_ab_test_member_history_table_union
;

CREATE OR REPLACE TRANSIENT TABLE dbt_dev_robin.bi_customer_insight.ci_ab_test_log_union
	CLONE dbt.bi_customer_insight.ci_ab_test_log_union
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

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.mention_me__profile_activity
	CLONE data_vault_mvp.dwh.mention_me__profile_activity
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates
	CLONE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
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

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review
	CLONE data_vault_mvp.dwh.user_booking_review
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities
	CLONE data_vault_mvp.dwh.user_recent_activities
;

CREATE SCHEMA IF NOT EXISTS data_science_dev_robin.nextoken_prod
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.nextoken_prod.kronos_refined_deals
	CLONE data_science.nextoken_prod.kronos_refined_deals
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.wish_list
	CLONE latest_vault.cms_mysql.wish_list
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.wish_list_item
	CLONE latest_vault.cms_mysql.wish_list_item
;

CREATE SCHEMA IF NOT EXISTS data_science_dev_robin.test
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.test.sunday_deals_reordered
	CLONE data_science.test.sunday_deals_reordered
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
	CLONE data_vault_mvp.dwh.iterable__user_profile_activity
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.iterable.user_profile_activity.py' \
    --method 'run' \
    --start '2025-09-22 00:00:00' \
    --end '2025-09-22 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity_20250922 CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
;
-- prod
SELECT
	segment_name,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity_20250922
GROUP BY ALL
;

-- dev
SELECT
	segment_name,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
GROUP BY ALL
;



WITH
	athena_segments AS
		(
			SELECT
				sua.shiro_user_id,
				sua.current_affiliate_territory,
				sua.main_affiliate_brand,
				DATE(sua.signup_tstamp)                 AS signup_date,
				COALESCE(up.segment_name, 'no segment') AS segment_name,
				rfv.rfv_segment,
				up.last_email_open_tstamp,
				up.last_email_click_tstamp,
				up.last_session_end_tstamp
			FROM se.data.se_user_attributes sua
			LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity up
				USING (shiro_user_id)
			LEFT JOIN dbt.bi_customer_insight.ci_rfv_segments rfv
				ON sua.shiro_user_id = rfv.shiro_user_id
			WHERE 1 = 1
			  AND email_opt_in_status IN ('daily', 'weekly')
			  AND membership_account_status = 'FULL_ACCOUNT'
			  AND sua.current_affiliate_territory <> 'IE'
			  AND rfv_segment NOT IN ('Dormant', 'Sunset')
		)

SELECT *
FROM athena_segments a
WHERE 1 = 1
  AND (segment_name LIKE '%ACT_DEAD%' OR segment_name = 'no segment')
  AND signup_date > CURRENT_DATE - 735
  AND last_session_end_tstamp IS NULL
;


SELECT *
FROM se.data.se_user_attributes sua
WHERE sua.current_affiliate_territory = 'IE'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity iupa
;


-- prod
SELECT
	segment_name,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity_master
GROUP BY ALL
;

-- dev
SELECT
	segment_name,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
GROUP BY ALL
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step03__agg_obj_spvs
;

SELECT *
FROM data_vault_mvp_dev_robin.information_schema.tables
WHERE table_schema = 'DWH' AND table_name LIKE 'ITERABLE__USER_PROFILE_ACTIVITY%'
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step01__model_spv_tags_subset_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step01__model_spv_tags_subset
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step02__model_spvs_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step02__model_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step03__agg_obj_spvs_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step03__agg_obj_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step04__agg_spvs_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step04__agg_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step05__model_top_five_deals_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step05__model_top_five_deals
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step07__penultimate_spvs_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step07__penultimate_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step08__users_with_ranks_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step08__users_with_ranks
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step09__model_tags_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step09__model_tags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step10__agg_tags_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step10__agg_tags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step11__spv_tags_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step11__spv_tags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step12__agg_popular_tags_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step12__agg_popular_tags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step13__agg_past_days_popular_tags_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step13__agg_past_days_popular_tags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step14__user_booking_reviews_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step14__user_booking_reviews
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step15__locale_country_lookup_helper_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step15__locale_country_lookup_helper
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step16__locale_city_lookup_helper_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step16__locale_city_lookup_helper
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step17__month_replace_helper_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step17__month_replace_helper
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step18__user_search_results_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step18__user_search_results
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step19__user_sessions_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step19__user_sessions
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step20__user_favorite_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step20__user_favorite
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step21__user_wishlist_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step21__user_wishlist
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step22__user_voucher_credit_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step22__user_voucher_credit
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step23__recency_frequency_value_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step23__recency_frequency_value
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step24__ab_test_member_history_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step24__ab_test_member_history
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step25__sunday_deals_ordered_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step25__sunday_deals_ordered
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step26__mention_me_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step26__mention_me
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity__step27__model_data_master CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step27__model_data
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
;



WITH
	master AS (
		SELECT
			shiro_user_id,
			HASH(*) AS master_hash
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step05__model_top_five_deals_master
	),
	dev AS (
		SELECT
			shiro_user_id,
			HASH(*) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step05__model_top_five_deals
	)
SELECT
	COALESCE(master.shiro_user_id, dev.shiro_user_id) AS shiro_user_id,
	master_hash,
	dev_hash,
FROM master
FULL OUTER JOIN dev
	ON master.shiro_user_id = dev.shiro_user_id
WHERE master_hash != dev_hash
;
-- master
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step05__model_top_five_deals_master
WHERE shiro_user_id = '64872384'
;
-- dev
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step05__model_top_five_deals
WHERE shiro_user_id = '64872384'
;


[
  "A13625",
  "A14387",
  "A62444",
  "A63692",
  "A10773"
]

[
  "A13625",
  "A10773",
  "A53974",
  "A14387",
  "A62444"
];


WITH
	agg_deals_into_buckets AS (
		SELECT
			spv_tags.shiro_user_id,
			spv_tags.se_sale_id,
			SUM(IFF(spv_tags.event_tstamp::DATE = CURRENT_DATE() - 1, 1, 0))   AS sales_page_views_last_1_days,
			SUM(IFF(spv_tags.event_tstamp::DATE >= CURRENT_DATE() - 8, 1, 0))  AS sales_page_views_last_7_days,
			SUM(IFF(spv_tags.event_tstamp::DATE >= CURRENT_DATE() - 15, 1, 0)) AS sales_page_views_last_14_days
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step01__model_spv_tags_subset spv_tags
		WHERE event_tstamp::DATE >= CURRENT_DATE() - 15
		  AND spv_tags.shiro_user_id = '64872384'
		GROUP BY spv_tags.shiro_user_id,
				 spv_tags.se_sale_id
	),
	rank_deals_within_buckets AS (
		SELECT
			deal_buckets.shiro_user_id,
			deal_buckets.se_sale_id,
			ROW_NUMBER() OVER (PARTITION BY deal_buckets.shiro_user_id ORDER BY deal_buckets.sales_page_views_last_1_days DESC)  AS rank_sales_page_views_last_1_days,
			ROW_NUMBER() OVER (PARTITION BY deal_buckets.shiro_user_id ORDER BY deal_buckets.sales_page_views_last_7_days DESC)  AS rank_sales_page_views_last_7_days,
			ROW_NUMBER() OVER (PARTITION BY deal_buckets.shiro_user_id ORDER BY deal_buckets.sales_page_views_last_14_days DESC) AS rank_sales_page_views_last_14_days
		FROM agg_deals_into_buckets deal_buckets
	)
SELECT
	shiro_user_id,
	ARRAY_AGG(
			CASE rank_sales_page_views_last_1_days
				WHEN 1 THEN se_sale_id
				WHEN 2 THEN se_sale_id
				WHEN 3 THEN se_sale_id
				WHEN 4 THEN se_sale_id
				WHEN 5 THEN se_sale_id
			END) WITHIN GROUP (ORDER BY rank_sales_page_views_last_1_days ASC)  AS top_5_sale_page_views_last_1_days,
	ARRAY_AGG(
			CASE rank_sales_page_views_last_7_days
				WHEN 1 THEN se_sale_id
				WHEN 2 THEN se_sale_id
				WHEN 3 THEN se_sale_id
				WHEN 4 THEN se_sale_id
				WHEN 5 THEN se_sale_id
			END) WITHIN GROUP (ORDER BY rank_sales_page_views_last_7_days ASC)  AS top_5_sale_page_views_last_7_days,
	ARRAY_AGG(
			CASE rank_sales_page_views_last_14_days
				WHEN 1 THEN se_sale_id
				WHEN 2 THEN se_sale_id
				WHEN 3 THEN se_sale_id
				WHEN 4 THEN se_sale_id
				WHEN 5 THEN se_sale_id
			END) WITHIN GROUP (ORDER BY rank_sales_page_views_last_14_days ASC) AS top_5_sale_page_views_last_14_days
FROM rank_deals_within_buckets
GROUP BY shiro_user_id


WITH
	agg_deals_into_buckets AS (
		SELECT
			spv_tags.shiro_user_id,
			spv_tags.se_sale_id,
			SUM(IFF(spv_tags.event_tstamp::DATE = CURRENT_DATE() - 1, 1, 0))   AS sales_page_views_last_1_days,
			SUM(IFF(spv_tags.event_tstamp::DATE >= CURRENT_DATE() - 8, 1, 0))  AS sales_page_views_last_7_days,
			SUM(IFF(spv_tags.event_tstamp::DATE >= CURRENT_DATE() - 15, 1, 0)) AS sales_page_views_last_14_days
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step01__model_spv_tags_subset spv_tags
		WHERE event_tstamp::DATE >= CURRENT_DATE() - 15
		  AND spv_tags.shiro_user_id = '64872384'
		GROUP BY spv_tags.shiro_user_id,
				 spv_tags.se_sale_id
	),
	rank_deals_within_buckets AS (
		SELECT
			deal_buckets.shiro_user_id,
			deal_buckets.se_sale_id,
			IFF(deal_buckets.sales_page_views_last_1_days > 0,
				ROW_NUMBER() OVER (PARTITION BY deal_buckets.shiro_user_id ORDER BY deal_buckets.sales_page_views_last_1_days DESC),
				NULL) AS rank_sales_page_views_last_1_days,
			IFF(deal_buckets.sales_page_views_last_7_days > 0,
				ROW_NUMBER() OVER (PARTITION BY deal_buckets.shiro_user_id ORDER BY deal_buckets.sales_page_views_last_7_days DESC),
				NULL) AS rank_sales_page_views_last_7_days,
			IFF(deal_buckets.sales_page_views_last_14_days > 0,
				ROW_NUMBER() OVER (PARTITION BY deal_buckets.shiro_user_id ORDER BY deal_buckets.sales_page_views_last_14_days DESC),
				NULL) AS rank_sales_page_views_last_14_days
		FROM agg_deals_into_buckets deal_buckets
	)
SELECT
	shiro_user_id,
	ARRAY_AGG(
			CASE rank_sales_page_views_last_1_days
				WHEN 1 THEN se_sale_id
				WHEN 2 THEN se_sale_id
				WHEN 3 THEN se_sale_id
				WHEN 4 THEN se_sale_id
				WHEN 5 THEN se_sale_id
			END) WITHIN GROUP (ORDER BY rank_sales_page_views_last_1_days ASC)  AS top_5_sale_page_views_last_1_days,
	ARRAY_AGG(
			CASE rank_sales_page_views_last_7_days
				WHEN 1 THEN se_sale_id
				WHEN 2 THEN se_sale_id
				WHEN 3 THEN se_sale_id
				WHEN 4 THEN se_sale_id
				WHEN 5 THEN se_sale_id
			END) WITHIN GROUP (ORDER BY rank_sales_page_views_last_7_days ASC)  AS top_5_sale_page_views_last_7_days,
	ARRAY_AGG(
			CASE rank_sales_page_views_last_14_days
				WHEN 1 THEN se_sale_id
				WHEN 2 THEN se_sale_id
				WHEN 3 THEN se_sale_id
				WHEN 4 THEN se_sale_id
				WHEN 5 THEN se_sale_id
			END) WITHIN GROUP (ORDER BY rank_sales_page_views_last_14_days ASC) AS top_5_sale_page_views_last_14_days
FROM rank_deals_within_buckets
GROUP BY shiro_user_id

SELECT
	record['userId'],
	u.record

FROM unload_vault_mvp.iterable.user_profile_activity_01__20250716t040000__daily_at_04h00 u
;


SELECT
	iupa.shiro_user_id,
	iupa.top_5_sale_page_views_last_1_days,
	ARRAY_SIZE(iupa.top_5_sale_page_views_last_1_days)  AS top_5_sale_page_views_last_1_days_array_size,
	iupa.top_5_sale_page_views_last_7_days,
	ARRAY_SIZE(iupa.top_5_sale_page_views_last_7_days)  AS top_5_sale_page_views_last_7_days_array_size,
	iupa.top_5_sale_page_views_last_14_days,
	ARRAY_SIZE(iupa.top_5_sale_page_views_last_14_days) AS top_5_sale_page_views_last_14_days_array_size,
	top_5_sale_page_views_last_1_days_array_size = top_5_sale_page_views_last_14_days_array_size
FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa
WHERE (top_5_sale_page_views_last_1_days_array_size +
	   top_5_sale_page_views_last_7_days_array_size +
	   top_5_sale_page_views_last_14_days_array_size > 0)
  AND top_5_sale_page_views_last_1_days_array_size = top_5_sale_page_views_last_14_days_array_size

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step05__model_top_five_deals
WHERE iterable__user_profile_activity__step05__model_top_five_deals.shiro_user_id = '64872384'
;


WITH
	master AS (
		SELECT
			shiro_user_id,
			HASH(
					ARRAY_SIZE(top_5_sale_page_views_last_1_days),
					ARRAY_SIZE(top_5_sale_page_views_last_7_days),
					ARRAY_SIZE(top_5_sale_page_views_last_14_days)) AS master_hash
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step05__model_top_five_deals_master
	),
	dev AS (
		SELECT
			shiro_user_id,
			HASH(
					ARRAY_SIZE(top_5_sale_page_views_last_1_days),
					ARRAY_SIZE(top_5_sale_page_views_last_7_days),
					ARRAY_SIZE(top_5_sale_page_views_last_14_days)) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step05__model_top_five_deals
	)
SELECT
	COALESCE(master.shiro_user_id, dev.shiro_user_id) AS shiro_user_id,
	master_hash,
	dev_hash,
FROM master
FULL OUTER JOIN dev
	ON master.shiro_user_id = dev.shiro_user_id
WHERE master_hash != dev_hash
;



WITH
	master AS (
		SELECT
			shiro_user_id,
			HASH(*) AS master_hash
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step21__user_wishlist_master
	),
	dev AS (
		SELECT
			shiro_user_id,
			HASH(*) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step20__user_wishlist
	)
SELECT
	COALESCE(master.shiro_user_id, dev.shiro_user_id) AS shiro_user_id,
	master_hash,
	dev_hash,
FROM master
FULL OUTER JOIN dev
	ON master.shiro_user_id = dev.shiro_user_id
WHERE master_hash != dev_hash
;

-- master
SELECT *,
	   HASH(*) AS master_hash
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step21__user_wishlist_master
WHERE shiro_user_id = '55163177'
;

-- dev
SELECT *,
	   HASH(*) AS dev_hash,
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step20__user_wishlist
WHERE shiro_user_id = '55163177'
;



WITH
	master AS (
		SELECT
			shiro_user_id,
			HASH(*) AS master_hash
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step22__user_voucher_credit_master
	),
	dev AS (
		SELECT
			shiro_user_id,
			HASH(*) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step21__user_voucher_credit
	)
SELECT
	COALESCE(master.shiro_user_id, dev.shiro_user_id) AS shiro_user_id,
	master_hash,
	dev_hash,
FROM master
FULL OUTER JOIN dev
	ON master.shiro_user_id = dev.shiro_user_id
WHERE master_hash != dev_hash
;

-- master
SELECT *,
	   HASH(*) AS master_hash
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step22__user_voucher_credit_master
WHERE shiro_user_id = '704713'
;

-- dev
SELECT *,
	   HASH(*) AS dev_hash,
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step21__user_voucher_credit
WHERE shiro_user_id = '704713'
;


WITH
	master AS (
		SELECT
			shiro_user_id,
			HASH(*) AS master_hash
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step24__ab_test_member_history_master
	),
	dev AS (
		SELECT
			shiro_user_id,
			HASH(*) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step23__ab_test_member_history
	)
SELECT
	COALESCE(master.shiro_user_id, dev.shiro_user_id) AS shiro_user_id,
	master_hash,
	dev_hash,
FROM master
FULL OUTER JOIN dev
	ON master.shiro_user_id = dev.shiro_user_id
WHERE master_hash != dev_hash
;

-- master
SELECT *,
	   HASH(*) AS master_hash
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step24__ab_test_member_history_master
WHERE shiro_user_id = '51381060'
;

-- dev
SELECT *,
	   HASH(*) AS dev_hash,
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step23__ab_test_member_history
WHERE shiro_user_id = '51381060'
;

-- master
SELECT
	segment_name,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity
GROUP BY segment_name

-- dev
SELECT
	segment_name,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
GROUP BY segment_name
;


WITH
	master AS (
		SELECT
			shiro_user_id,
			HASH(* exclude(schedule_tstamp, run_tstamp, operation_id, created_at, updated_at, row_hash)) AS master_hash
		FROM data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity
	),
	dev AS (
		SELECT
			shiro_user_id,
			HASH(* exclude(schedule_tstamp, run_tstamp, operation_id, created_at, updated_at, row_hash)) AS dev_hash
		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
	)
SELECT
	COALESCE(master.shiro_user_id, dev.shiro_user_id) AS shiro_user_id,
	master_hash,
	dev_hash,
FROM master
FULL OUTER JOIN dev
	ON master.shiro_user_id = dev.shiro_user_id
WHERE master_hash != dev_hash
;

-- master
SELECT * EXCLUDE (schedule_tstamp, run_tstamp, operation_id, created_at, updated_at, row_hash), HASH(*) AS master_hash
FROM data_vault_mvp_dev_robin.dwh__iterable__user_profile_activity.iterable__user_profile_activity
WHERE shiro_user_id = '79697936'
;

-- dev
SELECT * EXCLUDE (schedule_tstamp, run_tstamp, operation_id, created_at, updated_at, row_hash), HASH(*) AS dev_hash,
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
WHERE shiro_user_id = '79697936'
;

SELECT *,
	   ua.original_affiliate_territory
FROM data_vault_mvp.dwh.user_attributes ua
;

