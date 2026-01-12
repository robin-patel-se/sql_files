SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_activity__step25__model_data
QUALIFY COUNT(*) OVER (PARTITION BY shiro_user_id) > 1


WITH
	step24 AS (
		SELECT
			ura.*
		FROM data_vault_mvp.dwh.user_recent_activities ura
			INNER JOIN data_vault_mvp.dwh.user_attributes ua
					   ON ura.shiro_user_id = ua.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step03__agg_obj_spvs s
					   ON ura.shiro_user_id = s.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step04__agg_spvs AS agg_spvs
					   ON ura.shiro_user_id = agg_spvs.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step05__model_top_five_deals AS top
					   ON ura.shiro_user_id = top.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step07__penultimate_spvs AS pen
					   ON pen.shiro_user_id = ura.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step08__users_with_ranks uhr
					   ON ura.shiro_user_id = uhr.user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step10__agg_tags AS at
					   ON ura.shiro_user_id = at.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step12__agg_popular_tags AS pop_tags
					   ON ura.shiro_user_id = pop_tags.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step13__agg_past_days_popular_tags AS pop_tags_past_days
					   ON ura.shiro_user_id = pop_tags_past_days.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step14__user_booking_reviews ubr
					   ON ura.shiro_user_id = ubr.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step18__user_search_results usrn
					   ON usrn.shiro_user_id = ura.shiro_user_id
						   AND usrn.search_results_category = '1-9'

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step18__user_search_results usro
					   ON usro.shiro_user_id = ura.shiro_user_id
						   AND usro.search_results_category = '9+'

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step19__user_sessions ses
					   ON ses.shiro_user_id = ura.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step20__user_favorite fav
					   ON fav.shiro_user_id = ura.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step21__user_wishlist wish
					   ON wish.shiro_user_id = ura.shiro_user_id

			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step22__user_voucher_credit user_voucher_credit
					   ON user_voucher_credit.shiro_user_id = ura.shiro_user_id
--
-- 			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step23__recency_frequency_value recency_frequency_value
-- 					   ON recency_frequency_value.shiro_user_id = ura.shiro_user_id
--
-- 			LEFT JOIN  data_vault_mvp.dwh.iterable__user_profile_activity__step24__ab_test_member_history ab_test_member_history
-- 					   ON ab_test_member_history.shiro_user_id = ura.shiro_user_id
	)
SELECT *
FROM step24 s
WHERE s.shiro_user_id = 78184439
;

-- checked step 25 to find dupes, found they were being introduced with the voucher credit join


SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_activity__step22__user_voucher_credit
WHERE shiro_user_id = 78184439
;



WITH
	user_credit_balance AS (

		-- step i) model currently active credits
		SELECT
			se_credit.shiro_user_id,
			TRUE                                                                  AS user_has_credit,
			se_credit.credit_amount_gbp::DECIMAL(13, 2)                           AS credit_amount_gbp,
			se_credit.credit_currency                                             AS credit_currency,
			se_credit.credit_amount::DECIMAL(13, 2)                               AS credit_amount_in_credit_currency,
			TO_VARCHAR(se_credit.credit_date_created, 'YYYY-MM-DD HH:MI:SS')      AS credit_start_date,
			TO_VARCHAR(se_credit.credit_expiration_tstamp, 'YYYY-MM-DD HH:MI:SS') AS credit_expiry_date,
			se_credit.credit_type                                                 AS credit_type

		FROM data_vault_mvp.dwh.se_credit se_credit
		WHERE
		  -- some credits can be both 'ACTIVE' and past their expiry date, exclude those here
			se_credit.credit_expiration_date > CURRENT_DATE()
		  AND se_credit.credit_status = 'ACTIVE'
		  AND se_credit.shiro_user_id IS NOT NULL

		UNION ALL

		-- step ii) model the update of non-active credits
		-- this step is necessary because previously active credits
		-- ..need to be re-sent with user_has_credit: FALSE in order to zero-out
		-- ..prior credit history in Iterable
		-- ..i.e. if you post nothing, the old array will not update
		-- note: that if a user also has an active credit, the active credit takes precedence (i.e. no entry here)
		-- note: sent a single record here to zero out all prior records
		-- note: you cannot reliably Delta of field: credit_last_updated as the credit status can change and this field is often not updated
		-- note: the credit snapshot is weekly, so lacks sufficient precision to report daily
		SELECT
			se_credit.shiro_user_id,
			FALSE                             AS user_has_credit,
			0.00::DECIMAL(13, 2)              AS credit_amount_gbp,
			MAX(se_credit.credit_currency)    AS credit_currency,
			0.00                              AS credit_amount_in_credit_currency,
			TO_VARCHAR(MAX(se_credit.credit_date_created),
					   'YYYY-MM-DD HH:MI:SS') AS credit_start_date,
			TO_VARCHAR(IFF(MAX(se_credit.credit_expiration_tstamp::DATE) > CURRENT_DATE(),
						   CURRENT_TIMESTAMP(), MAX(credit_expiration_tstamp)),
					   'YYYY-MM-DD HH:MI:SS') AS credit_expiry_date,
			'INACTIVE'                        AS credit_type

		FROM data_vault_mvp.dwh.se_credit se_credit
		WHERE se_credit.credit_status != 'ACTIVE'
		  AND se_credit.shiro_user_id NOT IN (
			-- user should not have an active credit
			SELECT
				COALESCE(shiro_user_id, '-1') AS shiro_user_id
			FROM data_vault_mvp.dwh.se_credit
			WHERE se_credit.credit_expiration_date > CURRENT_DATE()
			  AND se_credit.credit_status = 'ACTIVE'
			  AND se_credit.shiro_user_id IS NOT NULL
		)
		  AND se_credit.shiro_user_id IS NOT NULL
		GROUP BY se_credit.shiro_user_id
	),
	model AS (
		SELECT
			user_credit_balance.shiro_user_id,
			user_credit_balance.user_has_credit,
			MAX(user_credit_balance.credit_start_date)                              AS most_recent_start,
			OBJECT_CONSTRUCT('creditTotalGBP', SUM(credit_amount_gbp),
				-- at the time of writing, 1x credit_currency per user (i.e. users cannot have multiple credit currencies)
				-- ..should this not hold true in future, the assertion not to duplicate shiro_user_id will fail
				-- ..i.e. don't belt-and-braces AGG or MAX currency here, let the assertion fail
							 'creditTotalinCreditCurrency', SUM(user_credit_balance.credit_amount_in_credit_currency),
							 'creditCurrency', user_credit_balance.credit_currency) AS user_credit_total,
			ARRAY_AGG(OBJECT_CONSTRUCT(
							  'creditAmountGBP', user_credit_balance.credit_amount_gbp,
							  'creditCurrency', user_credit_balance.credit_currency,
							  'creditAmountInCreditCurrency', user_credit_balance.credit_amount_in_credit_currency,
							  'creditStartDate', user_credit_balance.credit_start_date,
							  'creditExpiryDate', user_credit_balance.credit_expiry_date,
							  'creditType', user_credit_balance.credit_type
						  )) WITHIN GROUP (
						  ORDER BY
						  user_credit_balance.credit_start_date
						  DESC)                                                     AS user_credit_array
		FROM user_credit_balance AS user_credit_balance
		GROUP BY user_credit_balance.shiro_user_id,
				 user_credit_balance.user_has_credit,
				 user_credit_balance.credit_currency
	)
SELECT *
FROM model
QUALIFY ROW_NUMBER() OVER (PARTITION BY shiro_user_id ORDER BY most_recent_start) = 1


SELECT *
FROM se.data.se_credit sc
WHERE sc.shiro_user_id = 78184439
;

CREATE DATABASE customer_insight_dev_robin
;

CREATE SCHEMA customer_insight_dev_robin.data_marts
;


CREATE DATABASE dbt_dev_robin
;

CREATE SCHEMA dbt_dev_robin.bi_customer_insight
;

CREATE OR REPLACE TRANSIENT TABLE customer_insight_dev_robin.data_marts.ab_test_member_history_table CLONE customer_insight.data_marts.ab_test_member_history_table
;

CREATE OR REPLACE TRANSIENT TABLE customer_insight_dev_robin.data_marts.ab_test_log CLONE customer_insight.data_marts.ab_test_log
;

CREATE OR REPLACE TRANSIENT TABLE dbt_dev_robin.bi_customer_insight.ci_rfv_segments CLONE dbt.bi_customer_insight.ci_rfv_segments
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.city_translation CLONE latest_vault.cms_mysql.city_translation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.country_translation CLONE latest_vault.cms_mysql.country_translation
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.fact_booking CLONE data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.favorite CLONE latest_vault.cms_mysql.favorite
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates CLONE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit CLONE data_vault_mvp.dwh.se_credit
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags CLONE data_vault_mvp.dwh.se_sale_tags
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review CLONE data_vault_mvp.dwh.user_booking_review
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_recent_activities CLONE data_vault_mvp.dwh.user_recent_activities
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.operational_output.vw_recommended_deals_augmented CLONE data_science.operational_output.vw_recommended_deals_augmented
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.wish_list CLONE latest_vault.cms_mysql.wish_list
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.wish_list_item CLONE latest_vault.cms_mysql.wish_list_item
;


DROP TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity__step22__user_voucher_credit
WHERE shiro_user_id = 78184439
;


------------------------------------------------------------------------------------------------------------------------
WITH
	dupe_users AS (
		SELECT *
		FROM se.data.se_credit sc
		WHERE sc.credit_status = 'ACTIVE'
		QUALIFY COUNT(*) OVER (PARTITION BY sc.shiro_user_id, sc.credit_currency) > 1
	)
SELECT
	du.shiro_user_id,
	LISTAGG(du.credit_currency,)
FROM dupe_users du
GROUP BY 1
;

SELECT
	sc.shiro_user_id,
	LISTAGG(DISTINCT sc.credit_currency, ', ') AS currencies
FROM se.data.se_credit sc
WHERE sc.credit_status = 'ACTIVE'
GROUP BY 1
HAVING COUNT(DISTINCT sc.credit_currency) > 1


SELECT *
FROM se.data.harmonised_sale_calendar_view hscv
;


SELECT
	sc.shiro_user_id,
	LISTAGG(DISTINCT sc.credit_currency, ', ') AS currencies
FROM se.data.se_credit sc
WHERE sc.credit_status = 'ACTIVE'
GROUP BY 1
HAVING COUNT(DISTINCT sc.credit_currency) > 1;


