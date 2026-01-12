USE WAREHOUSE pipe_large;

SELECT
    s.salesforce_opportunity_id,
    LOWER(o.stagename)                                   AS stage_name,
    s.se_sale_id,
    so.base_offer_id,
    IFF(s.sale_active AND so.offer_active, 'live', NULL) AS cms_status,
    COALESCE(cms_status, stage_name)                     AS deal_stage,
    CASE
        WHEN LOWER(deal_stage) = 'pre-approved' THEN '1.pre-approved'
        WHEN LOWER(deal_stage) = 'final approved' THEN '2.final approved'
        WHEN LOWER(deal_stage) = 'live' THEN '3.live'
        END                                              AS deal_stage_rank
FROM hygiene_snapshot_vault_mvp.sfsc.opportunity o
    LEFT JOIN data_vault_mvp.dwh.se_sale s ON s.salesforce_opportunity_id_full = o.id
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.hotel_sale_offer hso ON s.se_sale_id = 'A' || hso.hotel_sale_id
    LEFT JOIN data_vault_mvp.dwh.se_offer so ON so.base_offer_id = hso.offer_id AND so.data_model = 'New Data Model'
WHERE LOWER(s.product_configuration) = 'hotel'
  AND ((s.sale_active AND so.offer_active)
    OR
       LOWER(o.stagename) IN ('pre-approved', 'final approved'))
    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.salesforce_opportunity_id, so.base_offer_id ORDER BY deal_stage_rank DESC) = 1

SELECT * FROM snowplow.atomic.events e;



SELECT PARSE_URL('https://www.secretescapes.com/current-sales?affiliate=goosups&awadgroupid=50557271023&awadposition=&awcampaignid=1055466949&awcreative=584317734381&awdevice=c&awkeyword=secret+escapes.&awloc_interest_ms=&awloc_physical_ms=1007097&awmatchtype=e&awplacement=&awtargetid=kwd-61789019645&saff=Desktop_Brand_Pure_Exact&utm_campaign=Desktop_Brand_Pure_Exact&utm_adgroup=Desktop_Brand_Secret_Escapes_Exact&gclid=Cj0KCQjwpcOTBhCZARIsAEAYLuWnUz1BtezdBOd9-ikz5Lu4La3DIycZOR_ofVn3Ka6wP03yDI_1maEaAloBEALw_wcB&affiliateUrlString=goosups&referrerId=')