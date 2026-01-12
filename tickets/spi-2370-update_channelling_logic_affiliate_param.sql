SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;






SELECT
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_mkt_channel = 'Direct'; --216,432,115

SELECT
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_mkt_channel = 'Direct'
  AND mtmc.affiliate IS NOT NULL; -- 24,353,622

WITH distinct_url_string AS (
    SELECT
        a2.url_string,
        a2.category
    FROM latest_vault.cms_mysql.affiliate a2
        QUALIFY ROW_NUMBER() OVER (PARTITION BY a2.url_string ORDER BY a2.last_updated DESC) = 1
)
SELECT
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
    INNER JOIN distinct_url_string a ON mtmc.affiliate = a.url_string
WHERE mtmc.touch_mkt_channel = 'Direct'
  AND mtmc.affiliate IS NOT NULL; -- 23,961,342


SELECT
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_mkt_channel = 'Direct'
  AND se.data.partner_affiliate_param(mtmc.affiliate); -- 4,761,199


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_mkt_channel = 'Direct'
  AND se.data.partner_affiliate_param(mtmc.affiliate); -- 4,761,199


SELECT
    params.value['title']::VARCHAR AS badges,
    *
FROM se.data.sales_kingfisher kf,
     LATERAL FLATTEN(INPUT => kf.badges, OUTER => TRUE) params;

SELECT *
FROM latest_vault.marketing_gsheets.display_cpl_data dcd;

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
WHERE mtmc.touch_mkt_channel = 'Direct'
  AND mtmc.affiliate IS NOT NULL;

SELECT
    mtmc.affiliate,
    sa.category,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
    LEFT JOIN se.data.se_affiliate sa ON mtmc.affiliate = sa.url_string
WHERE mtmc.touch_mkt_channel = 'Direct'
  AND mtmc.affiliate IS NOT NULL
GROUP BY 1, 2
ORDER BY 3 DESC;


SELECT
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
    LEFT JOIN se.data.se_affiliate sa ON mtmc.affiliate = sa.url_string
WHERE sa.url_string IS NULL
  AND mtmc.affiliate IS NOT NULL
  AND mtmc.touch_mkt_channel = 'Direct'; --392,352