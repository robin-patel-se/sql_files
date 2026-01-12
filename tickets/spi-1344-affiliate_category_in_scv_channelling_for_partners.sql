SELECT *
FROM se.data.se_affiliate sa
WHERE sa.category = 'PARTNER'


SELECT
    a.url_string
FROM latest_vault.cms_mysql.affiliate a
WHERE a.category = 'PARTNER'
    self_describing_task --include 'biapp/task_catalogue/se/data/udfs/udf_functions.py'  --method 'run' --start '2022-06-13 00:00:00' --end '2022-06-13 00:00:00'

DROP FUNCTION se_dev_robin.data.airline_name_from_iata_code(VARCHAR
                                                           )

SELECT se_dev_robin.data.partner_affiliate_param('urlaubsguru410');


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_mkt_channel = 'Other'
  AND se_dev_robin.data.partner_affiliate_param(mtmc.affiliate);


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_20220614 CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;


USE WAREHOUSE pipe_xlarge;
UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_mkt_channel = 'Partner'
WHERE target.touch_mkt_channel = 'Other'
  AND se.data.partner_affiliate_param(target.affiliate);


SELECT
    mtmc.touch_mkt_channel,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_mkt_channel IN ('Partner', 'Other')
GROUP BY 1;

SELECT
    mtmc.touch_mkt_channel,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_mkt_channel IN ('Partner', 'Other')
GROUP BY 1;



SELECT * FROM latest_vault.marketing_gsheets.tv_spend_data tsd;
SELECT * FROM latest_vault.marketing_gsheets.tv_production_data tpd;
