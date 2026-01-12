USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
CLONE data_vault_mvp.dwh.se_sale;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.hotel_sale_offer
CLONE latest_vault.cms_mysql.hotel_sale_offer;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.cms_allocation_link
CLONE data_vault_mvp.dwh.cms_allocation_link;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_offer
CLONE latest_vault.cms_mysql.base_offer;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.mari_hotel_offer_rooms_and_rates_snapshot
CLONE data_vault_mvp.dwh.mari_hotel_offer_rooms_and_rates_snapshot;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.fpa_gsheets;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.constant_currency
CLONE latest_vault.fpa_gsheets.constant_currency;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS SELECT * FROM data_vault_mvp.dwh.fact_booking;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.global_deal_potential_pit
CLONE data_vault_mvp.bi.global_deal_potential_pit;

self_describing_task \
    --include 'biapp.task_catalogue.dv.bi.tableau.deal_model.global_deal_potential_pit.py' \
    --method 'run' \
    --start '2024-12-04 00:00:00' \
    --end '2024-12-04 00:00:00'


------------------------------------------------------------------------------------------------------------------------

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.dim_sale
AS SELECT * FROM data_vault_mvp.dwh.dim_sale;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS SELECT * FROM data_vault_mvp.dwh.fact_booking;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.email_reporting
CLONE data_vault_mvp.dwh.email_reporting;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review
CLONE data_vault_mvp.dwh.user_booking_review;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active
CLONE data_vault_mvp.dwh.sale_active;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.bi;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.sale_date_spvs
CLONE data_vault_mvp.bi.sale_date_spvs;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_territory
CLONE latest_vault.cms_mysql.sale_territory;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory
CLONE latest_vault.cms_mysql.territory;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.fpa_gsheets;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.constant_currency
CLONE latest_vault.fpa_gsheets.constant_currency;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.promotion
CLONE latest_vault.fpa_gsheets.promotion;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking
CLONE data_vault_mvp.dwh.se_booking;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.wrd_booking
CLONE data_vault_mvp.dwh.wrd_booking;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS SELECT * FROM data_vault_mvp.dwh.se_calendar;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags_snapshot
CLONE data_vault_mvp.dwh.se_sale_tags_snapshot;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking
CLONE data_vault_mvp.dwh.tb_booking;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.fact_sale_metrics
CLONE data_vault_mvp.bi.fact_sale_metrics;

self_describing_task \
    --include 'biapp.task_catalogue.dv.bi.tableau.deal_model.fact_sale_metrics.py' \
    --method 'run' \
    --start '2024-12-04 00:00:00' \
    --end '2024-12-04 00:00:00'

------------------------------------------------------------------------------------------------------------------------

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active
CLONE data_vault_mvp.dwh.sale_active;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale
CLONE data_vault_mvp.dwh.se_sale;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS SELECT * FROM data_vault_mvp.dwh.fact_booking;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS SELECT * FROM data_vault_mvp.dwh.se_calendar;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.spv_thresholds
CLONE data_vault_mvp.bi.spv_thresholds;

self_describing_task \
    --include 'biapp.task_catalogue.dv.bi.tableau.deal_model.spv_thresholds.py' \
    --method 'run' \
    --start '2024-12-04 00:00:00' \
    --end '2024-12-04 00:00:00'