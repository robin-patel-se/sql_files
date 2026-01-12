SELECT
    sb.booking_completed_timestamp,
    IFF(DATE_PART('hour', sb.booking_completed_timestamp) < 16, 'Pre 4PM', 'Post 4PM') AS pre_post_4_pm,
    sb.check_in_date
FROM se.data.se_booking sb
WHERE sb.booking_status IN ('COMPLETE', 'REFUNDED')
  AND sb.booking_completed_date = sb.check_in_date;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.chiasma_sale CLONE data_vault_mvp.dwh.chiasma_sale;
CREATE SCHEMA latest_vault_dev_robin.fpa_gsheets;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.posu_categorisation CLONE latest_vault.fpa_gsheets.posu_categorisation;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.reactivated_sale_active CLONE data_vault_mvp.dwh.reactivated_sale_active;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer CLONE data_vault_mvp.dwh.tb_offer;
CREATE OR REPLACE TRANSIENT TABLE se_dev_robin.data.se_sale_travel_type CLONE se.data.se_sale_travel_type;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_sale CLONE data_vault_mvp.dwh.tvl_sale;