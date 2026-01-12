USE WAREHOUSE pipe_xlarge;

SELECT oa.*
FROM se.data.se_offer_attributes oa
         INNER JOIN se.data.se_sale_attributes sat ON oa.se_offer_id = IFF(sat.data_model = 'New Data Model', 'A' || sat.default_hotel_offer_id, sat.default_hotel_offer_id::varchar)
         LEFT JOIN  data_vault_mvp.cms_mysql_snapshots.base_offer_translation_snapshot bot ON oa.base_offer_id = bot.offer_id AND LOWER(bot.locale) = 'en_gb'
WHERE bot.name IS NULL
  AND oa.offer_active
  AND sat.sale_active