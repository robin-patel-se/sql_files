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

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale
AS
SELECT *
FROM data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.favorite
	CLONE latest_vault.cms_mysql.favorite
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

CREATE SCHEMA IF NOT EXISTS data_science_dev_robin.operational_output
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.operational_output.vw_recommended_deals_augmented
	CLONE data_science.operational_output.vw_recommended_deals_augmented
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

self_describing_task \
    --include 'biapp.task_catalogue.dv.dwh.iterable.user_profile_activity.py' \
    --method 'run' \
    --start '2025-01-24 00:00:00' \
    --end '2025-01-24 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity upa
	INNER JOIN se.data.se_user_attributes sua ON upa.shiro_user_id = sua.shiro_user_id AND sua.is_test_user
;


SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_activity upa
	INNER JOIN se.data.se_user_attributes sua ON upa.shiro_user_id = sua.shiro_user_id AND sua.is_test_user
;