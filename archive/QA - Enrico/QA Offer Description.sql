CREATE OR REPLACE VIEW collab.quality_assurance.offer_description COPY GRANTS AS
(
SELECT ss.se_sale_id,
       ss.salesforce_opportunity_id,
       ss.company_name,
       ss.sale_active,
       ss.start_date,
       ss.product_configuration,
       ss.salesforce_proposed_start_date,
       bos.id                                                                                              AS offer_id,
       bots.name                                                                                           AS offer_name_gb,
       bos.active,
       cml.product_id,
       hrps.rate_code,
       hrps.rack_rate_code,
       h.hotel_code,
       bos.max_adults                                                                                      AS cms_max_adults,
       bos.max_children                                                                                    AS cms_max_children,
       bos.max_infants                                                                                     AS cms_max_infants,
       bos.max_dependants                                                                                  AS cms_max_dependants,
       bos.max_child_age                                                                                   AS cms_max_child_age,
       bos.child_age_description                                                                           AS cms_child_age_description,
       bos.infant_age_description                                                                          AS cms_infant_age_description,
       REGEXP_REPLACE(bots.description,
                      '&nbsp;|<span style="font-size: 10pt;">|</?span>|</?div>|</?br>|<div style="">|<b>') AS description_gb,

       description_gb REGEXP '.*(Pets are not allowed in this hotel.).*'                                   AS pets_not_allowed_description,
       o.parking_charges__c                                                                                AS parking_charges,
       o.local_taxes__c                                                                                    AS local_taxes,
       o.pets_accepted__c                                                                                  AS pets_accepted,
       o.pets_taxes_specifications__c                                                                      AS pets_taxes_specifications

FROM data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bos
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hsos ON bos.id = hsos.hotel_offer_id
         INNER JOIN data_vault_mvp.dwh.se_sale ss ON 'A' || hsos.hotel_sale_id = ss.se_sale_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_translation_snapshot bots
                    ON bos.id = bots.offer_id AND bots.locale = 'en_GB'
         INNER JOIN data_vault_mvp.dwh.cms_mari_link cml ON bos.id = cml.offer_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot ps ON cml.product_id = ps.id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrps ON ps.id = hrps.hotel_product_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON ps.hotel_id = h.id
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON ss.salesforce_opportunity_id = LEFT(o.id, 15)
    );

GRANT SELECT ON TABLE collab.quality_assurance.offer_description TO ROLE personal_role__enricosanson;