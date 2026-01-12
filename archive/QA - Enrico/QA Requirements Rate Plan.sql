CREATE OR REPLACE VIEW collab.quality_assurance.cms_mari_rate_plan COPY GRANTS AS
(
WITH agg_sale AS (
    SELECT DISTINCT
           hsos.hotel_offer_id,
           ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.salesforce_account_id,
           ssa.salesforce_proposed_start_date,
           ssa.default_hotel_offer_id,
           MIN(ssa.start_date)  AS min_sale_start_date,
           MAX(ssa.sale_active) AS sale_active_in_one_territory
    FROM se.data.se_sale_attributes ssa
             INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hsos
                        ON ssa.se_sale_id = 'A' || hsos.hotel_sale_id

    GROUP BY 1, 2, 3, 4, 5, 6
),
     mari_details AS (
         SELECT rps.name           AS mari_rate_plan_name,
                hs.code            AS mari_hotel_code,
                rps.code           AS mari_rate_code,
                rps.rack_code      AS mari_rack_rate_code,
                rts.name           AS mari_room_name,
                rts.description    AS mari_room_type_description,
                hs.name            AS mari_hotel_name,
                rps.cts_commission AS mari_cash_to_settle_commission
         FROM data_vault_mvp.mari_snapshots.rate_plan_snapshot rps
                  INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rps.room_type_id = rts.id
                  INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
     ),
     avg_offer_rate AS (
         SELECT shorar.hotel_code,
                shorar.offer_id,
                AVG(shorar.rate) AS avg_rate
         FROM se.data.se_hotel_offer_rooms_and_rates shorar
         GROUP BY 1, 2
     )


SELECT bos.id                                                          AS offer_id,
       bos.id = ags.default_hotel_offer_id                             AS default_offer,
       bots.name                                                       AS offer_name,
       IFF(ags.hotel_offer_id IS NOT NULL, TRUE, FALSE)                AS offer_linked_to_sale,
       ags.salesforce_opportunity_id,
       ags.company_name,
       ags.salesforce_proposed_start_date,
       ags.min_sale_start_date,
       ags.salesforce_account_id,
       ags.sale_active_in_one_territory,
       cml.hotel_rate_plan_id,
       cml.product_id,
       cml.hotel_code,
       cml.rate_code,
       cml.rack_rate_code,
       cml.hotel_rate_rack_code,
       md.mari_rate_plan_name,
       md.mari_hotel_name,
       md.mari_room_name,
       md.mari_room_type_description,
       md.mari_cash_to_settle_commission,
       aor.avg_rate,
       MIN(avg_rate) OVER (PARTITION BY cml.hotel_code)                AS min_rate,
       aor.avg_rate = MIN(avg_rate) OVER (PARTITION BY cml.hotel_code) AS is_the_cheapest_hotel_rate
FROM data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bos
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_translation_snapshot bots
                    ON bos.id = bots.offer_id AND bots.locale = 'en_GB'
         LEFT JOIN  agg_sale ags ON bos.id = ags.hotel_offer_id
         INNER JOIN data_vault_mvp.dwh.cms_mari_link cml ON bos.id = cml.offer_id
         LEFT JOIN  mari_details md ON cml.rate_code = md.mari_rate_code AND
                                       cml.rack_rate_code = md.mari_rack_rate_code AND
                                       cml.hotel_code = md.mari_hotel_code
         LEFT JOIN  avg_offer_rate aor ON bos.id = aor.offer_id
    )
;


GRANT SELECT ON TABLE collab.quality_assurance.cms_mari_rate_plan TO ROLE personal_role__enricosanson;

--to check offers that are associated to multiple rate plans
SELECT cmrp.offer_id,
       cmrp.offer_name,
       COUNT(DISTINCT cmrp.hotel_rate_plan_id) AS count_rate
FROM collab.quality_assurance.cms_mari_rate_plan cmrp
WHERE cmrp.sale_active_in_one_territory
GROUP BY 1, 2
HAVING count_rate > 1;

SELECT *
FROM collab.quality_assurance.cms_mari_rate_plan cmrp
WHERE cmrp.hotel_code = '0016900002eY33l';

SELECT shrar.hotel_code
FROM se.data.se_hotel_rooms_and_rates shrar
WHERE shrar.hotel_code = '0016900002eY33l'


SELECT shorar.hotel_code,
       offer_id,
       AVG(rate) AS avg_rate
FROM se.data.se_hotel_offer_rooms_and_rates shorar
WHERE shorar.hotel_code = '0016900002eY33l'
GROUP BY 1, 2;

SELECT *
FROM raw_vault_mvp.cms_mysql.base_offer bo;

SELECT * FROm collab.quality_assurance.cms_mari_rate_plan;

