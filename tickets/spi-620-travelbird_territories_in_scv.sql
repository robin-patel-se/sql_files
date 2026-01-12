USE WAREHOUSE pipe_xlarge;

SELECT
    mtmc.touch_hostname,
    mtmc.touch_affiliate_territory,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_hostname IN ('travelbird.be',
                              'fr.travelbird.be')
GROUP BY 1, 2
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_affiliate_territory = 'TB-BE_FR'
WHERE target.touch_hostname = 'fr.travelbird.be'
  AND target.touch_hostname_territory NOT IN ('SE TECH', 'ANOMALOUS');


UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
SET target.touch_affiliate_territory = 'TB-BE_NL'
WHERE target.touch_hostname = 'travelbird.be'
  AND target.touch_hostname_territory NOT IN ('SE TECH', 'ANOMALOUS');


SELECT
    mtmc.touch_hostname,
    mtmc.touch_affiliate_territory,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_hostname IN ('travelbird.be',
                              'fr.travelbird.be')
GROUP BY 1, 2
;

