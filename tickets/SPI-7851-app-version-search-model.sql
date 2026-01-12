USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
	CLONE data_vault_mvp.single_customer_view_stg.page_screen_enrichment
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_searches
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_searches
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_booking_form_views
	CLONE data_vault_mvp.single_customer_view_stg.module_touched_booking_form_views
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

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.search_model
	CLONE data_vault_mvp.bi.search_model
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.bi.search.search_model.py' \
    --method 'run' \
    --start '2025-10-07 00:00:00' \
    --end '2025-10-07 00:00:00'

SELECT
	COUNT(*)
FROM data_vault_mvp.bi.search_model
;

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.bi.search_model
;


SELECT *
FROM data_vault_mvp_dev_robin.bi.search_model