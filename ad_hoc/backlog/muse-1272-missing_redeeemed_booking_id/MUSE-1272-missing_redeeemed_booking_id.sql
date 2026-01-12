SELECT * FROM se.data.se_credit sc WHERE sc.redeemed_se_booking_id IS NOT NULL;


CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.credit CLONE hygiene_snapshot_vault_mvp.cms_mysql.credit;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking_credit CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking_credit;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation_credit CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation_credit;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.billing CLONE hygiene_snapshot_vault_mvp.cms_mysql.billing;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.shiro_user CLONE hygiene_snapshot_vault_mvp.cms_mysql.shiro_user;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.time_limited_credit CLONE hygiene_snapshot_vault_mvp.cms_mysql.time_limited_credit;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.external_booking CLONE hygiene_snapshot_vault_mvp.cms_mysql.external_booking;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking CLONE data_vault_mvp.dwh.tb_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking CLONE data_vault_mvp.dwh.se_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates CLONE data_vault_mvp.fx.rates;

self_describing_task --include 'dv/dwh/transactional/se_credit.py'  --method 'run' --start '2021-11-01 00:00:00' --end '2021-11-01 00:00:00'

SELECT * FROM data_vault_mvp_dev_robin.dwh.se_credit sc WHERE sc.redeemed_se_booking_id IS NOT NULL;

