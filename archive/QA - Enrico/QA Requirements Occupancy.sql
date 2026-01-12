CREATE OR REPLACE VIEW collab.quality_assurance.child_policy_cms_mari COPY GRANTS AS
(
WITH mari_details AS (
    SELECT rps.code                                  AS rate_code,
           rps.name                                  AS rate_plan_name,
           rps.rack_code                             AS rack_rate_code,
           hs.code                                   AS hotel_code,
           hs.name,
           rts.code                                  AS room_type_code,
           rts.max_adults,
           rts.max_children,
           rts.max_infants,
           rts.max_dependants,
           rps.free_children,
           rps.free_infants,
           IFF(MAX(rs.child_rate) > 0, TRUE, FALSE)  AS has_a_child_rate,
           IFF(MAX(rs.infant_rate) > 0, TRUE, FALSE) AS has_a_infant_rate
    FROM data_vault_mvp.mari_snapshots.rate_snapshot rs
             INNER JOIN data_vault_mvp.mari_snapshots.rate_plan_snapshot rps ON rs.rate_plan_id = rps.id
             INNER JOIN data_vault_mvp.mari_snapshots.room_type_snapshot rts ON rps.room_type_id = rts.id
             INNER JOIN data_vault_mvp.mari_snapshots.hotel_snapshot hs ON rts.hotel_id = hs.id
    WHERE rs.date >= CURRENT_DATE -- remove historic rates
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
)

SELECT ss.se_sale_id,
       ss.salesforce_opportunity_id,
       ss.company_name,
       ss.sale_active,
       ss.start_date,
       ss.product_configuration,
       ss.salesforce_proposed_start_date,
       bos.id                                                                           AS offer_id,
       bots.name                                                                        AS offer_name_gb,
       bos.active,
       cml.product_id,
       hrps.rate_code,
       hrps.rack_rate_code,
       h.hotel_code,
       bos.max_adults                                                                   AS cms_max_adults,
       bos.max_children                                                                 AS cms_max_children,
       bos.max_infants                                                                  AS cms_max_infants,
       bos.max_dependants                                                               AS cms_max_dependants,
       bos.max_child_age                                                                AS cms_max_child_age,
       bos.child_age_description                                                        AS cms_child_age_description,
       bos.infant_age_description                                                       AS cms_infant_age_description,
       REGEXP_REPLACE(bots.child_policy,
                      '&nbsp;|<span style="font-size: 10pt;">|</?span>|</?div>|</?br>') AS child_policy_gb,
       md.rate_plan_name                                                                AS mari_rate_plan_name,
       md.max_adults                                                                    AS mari_max_adults,
       md.max_children                                                                  AS mari_max_children,
       md.max_infants                                                                   AS mari_max_infants,
       md.max_dependants                                                                AS mari_max_dependants,
       md.free_children                                                                 AS mari_free_children,
       md.free_infants                                                                  AS mari_free_infants,
       md.has_a_child_rate                                                              AS mari_has_a_child_rate,
       md.has_a_infant_rate                                                             AS mari_has_a_infant_rate

FROM data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bos
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hsos ON bos.id = hsos.hotel_offer_id
         INNER JOIN data_vault_mvp.dwh.se_sale ss ON 'A' || hsos.hotel_sale_id = ss.se_sale_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_translation_snapshot bots
                    ON bos.id = bots.offer_id AND bots.locale = 'en_GB'
         INNER JOIN data_vault_mvp.dwh.cms_mari_link cml ON bos.id = cml.offer_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot ps ON cml.product_id = ps.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrps ON ps.id = hrps.hotel_product_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON ps.hotel_id = h.id
         LEFT JOIN mari_details md ON hrps.rate_code = md.rate_code AND
                                      hrps.rack_rate_code = md.rack_rate_code AND
                                      h.hotel_code = md.hotel_code
WHERE LOWER(ss.data_model) = 'new data model'
    )
;

--
-- SELECT hrp.rack_rate_code,
--        hrp.rate_code,
--        h.hotel_code,
--        so.offer_active
-- FROM data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrp
--          INNER JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot p ON hrp.hotel_product_id = p.id
--          INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON p.hotel_id = h.id
--          INNER JOIN data_vault_mvp.dwh.se_offer so ON p.id = so.product_id;


GRANT SELECT ON TABLE collab.quality_assurance.child_policy_cms_mari TO ROLE personal_role__enricosanson;

SELECT *
FROM collab.quality_assurance.child_policy_cms_mari;



SELECT REGEXP_REPLACE(
               'This room type can sleep up to 2 adults and 1 infant (aged 0-2 years old) and/or&nbsp;<span style="font-size: 10pt;">children (aged 3-13 years old). Infants will sleep in a cot and children will have their own roll-out bed.</span><div><span style="font-size: 10pt;"><br></span></div><div>A cot and roll-out bed are availabile on request. Please contact the hotel directly prior to your arrival to arrange.</div>',
               '&nbsp;|<span style="font-size: 10pt;">|</?span>|</?div>|</?br>')


