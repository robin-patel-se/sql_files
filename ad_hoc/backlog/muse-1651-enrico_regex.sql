SELECT --od.description_gb,
       REGEXP_REPLACE(REGEXP_SUBSTR(od.description_gb, 'was checked on (.*)\\.', 1, 1, 'e'), '[\\s|,]'),
       TRY_TO_DATE(REGEXP_REPLACE(REGEXP_SUBSTR(od.description_gb, 'was checked on (.*)\\..*', 1, 1, 'e'), '[\\s|,]'), 'ddMMMMyyyy'),
       COUNT(*)
FROM collab.quality_assurance.offer_description od
WHERE od.description_gb LIKE '%was checked on%'
GROUP BY 1, 2
ORDER BY 3 DESC;

WITH regex_stuff AS (
    SELECT od.description_gb,
           REGEXP_REPLACE(od.description_gb, '[\\s|,]')                                         AS remove_chars,
           REGEXP_SUBSTR(remove_chars, 'wascheckedon(\\d{1,2}[a-zA-Z]*\\d{4})\\..*', 1, 1, 'e') AS substring,
           TRY_TO_DATE(substring, 'ddMMMMyyyy')                                                 AS date
    FROM collab.quality_assurance.offer_description od
    WHERE od.description_gb LIKE '%was checked on%'
)
SELECT *
FROM regex_stuff rs
WHERE rs.date IS NULL
;

SELECT REGEXP_SUBSTR(
               'Thediscount:</b>Ourdiscountiscalculatedbyreferencetothehotel''sownpricesorbyreferencetoanappropriatesourcereflectiveofthehotel''sownprices.Thediscountwascheckedon25March2021.Thisofferisonlyapplicablefornewreservationsandwillnotapplytoanyexistingreservations.'
           , 'wascheckedon(\\d{2}[a-zA-Z]*\\d{4})\\..*', 1, 1, 'e')


------------------------------------------------------------------------------------------------------------------------


WITH live_offers AS (
    SELECT base_offer_id,
           hotel_code
    FROM se.data.se_offer_attributes
    WHERE connected_active_sales > 0
      AND cms_active_flag = 'TRUE'
      AND offer_active = 'TRUE'
),
     live_offers AS (
         SELECT shrar.hotel_code
              , SUM(shrar.no_available_rooms) AS total_avail
         FROM se.data.se_hotel_rooms_and_rates shrar
         GROUP BY 1
     )
SELECT DISTINCT
       od.salesforce_opportunity_id      AS opp
     , od.salesforce_proposed_start_date AS psd
     , od.company_name
     , comp.cms_offer_active
     , od.sale_active
     , od.product_configuration
     , od.offer_id
     , od.offer_name_gb
     , od.description_gb
     , lo.base_offer_id
     , od.description_gb
FROM collab.quality_assurance.offer_description od
    LEFT JOIN collab.coo.cms_mari_salesforce_comparison comp ON comp.cms_base_offer_id = od.offer_id::VARCHAR
    LEFT JOIN live_offers lo ON lo.base_offer_id = od.offer_id
WHERE sale_active = TRUE
  AND base_offer_id IS NOT NULL
  AND comp.cms_offer_active

  --lift out the date value from the string and compare it to current date minus 3 months
  AND TRY_TO_DATE(
              REGEXP_SUBSTR(
                      REGEXP_REPLACE(od.description_gb, '[\\s|,]'),
                      'wascheckedon(\\d{1,2}[a-zA-Z]*\\d{4})\\..*', 1, 1, 'e'),
              'ddMMMMyyyy') >= DATEADD(MONTH, -3, CURRENT_DATE)
ORDER BY psd DESC
       , opp
