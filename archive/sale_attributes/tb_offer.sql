self_describing_task --include 'task_catalogue/dv/dwh/transactional/tb_offer.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'



CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer clone hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer;

SELECT * FROM hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer oo;

SELECT * FROM data_vault_mvp_dev_robin.dwh.tb_offer t;

self_describing_task --include 'task_catalogue/se/data/dim_sale.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
/Users/robin/myrepos/one-data-pipeline/biapp/

SELECT * FROM data_vault_mvp_dev_robin.dwh.tb_offer t WHERE t.se_sale_id = 'A13847';
SELECT * FROM se_dev_robin.data.dim_sale ds WHERE ds.se_sale_id = 'A13847';

self_describing_task --include 'task_catalogue/se/data/tb_offer.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT * FROM se_dev_robin.data.tb_offer;
------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer oo
WHERE oo.external_reference = 'A13847';

SELECT *
FROM raw_vault_mvp.cms_reports.sales
WHERE id = 'A13847';

SELECT *
FROM data_vault_mvp.travelbird_cms.partners_partner_snapshot pps
WHERE id = 1821737;

SELECT *
FROM data_vault_mvp.travelbird_cms.offers_offerconcept_snapshot oos;

SELECT bs.*
FROM data_vault_mvp.dwh.tb_offer t
         INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.base_sale bs ON t.se_sale_id = bs.sale_id

SELECT TO_NUMBER(mtba.attributed_user_id)                                    AS shiro_user_id, --filtering to only logged in/stitched sessions
       mtba.touch_start_tstamp::DATE                                         AS session_date,
       se.data.channel_category(mtmc.touch_mkt_channel)                      AS channel,
       se.data.posa_category_from_territory(mtmc.touch_affiliate_territory)  AS posa_territory,
       COUNT(*)                                                              AS sessions,
       SUM(CASE WHEN mtba.touch_experience = 'native app' THEN 0 ELSE 1 END) AS non_app_sessions,
       SUM(CASE WHEN mtba.touch_experience = 'native app' THEN 1 ELSE 0 END) AS app_sessions

FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
WHERE mtba.stitched_identity_type = 'se_user_id'
GROUP BY 1, 2, 3, 4;

