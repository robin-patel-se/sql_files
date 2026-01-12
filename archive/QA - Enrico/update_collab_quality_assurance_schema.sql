SHOW VIEWS IN SCHEMA collab.quality_assurance;

CREATE OR REPLACE VIEW collab.quality_assurance.cancellation_policy_qa COPY GRANTS AS
(
WITH agg_to_global AS (
    SELECT DISTINCT
           ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.sale_active,
           ssa.cancellation_policy_id IS NOT NULL AS cancellable,
           ssa.cancellation_policy_number_of_days,
           ssa.salesforce_proposed_start_date     AS sf_proposed_start_date
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.sale_type = 'Hotel'
)
SELECT o.id                                   AS salesforce_opportunity_id,
       atg.company_name,
       atg.sale_active,
       atg.sf_proposed_start_date,
       o.cancellation_terms__c                AS sf_cancellation_terms,
       atg.cancellable                        AS cms_cancellable,
       atg.cancellation_policy_number_of_days AS cms_cancellation_policy_number_of_days
FROM agg_to_global atg
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(o.id, 15)
    )
;
------------------------------------------------------------------------------------------------------------------------
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
    FROM latest_vault.mari.rate rs
        INNER JOIN latest_vault.mari.rate_plan rps ON rs.rate_plan_id = rps.id
        INNER JOIN latest_vault.mari.room_type rts ON rps.room_type_id = rts.id
        INNER JOIN latest_vault.mari.hotel hs ON rts.hotel_id = hs.id
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
       bos.id                                                                             AS offer_id,
       bots.name                                                                          AS offer_name_gb,
       bos.active,
       cml.product_id,
       hrps.rate_code,
       hrps.rack_rate_code,
       h.hotel_code,
       bos.max_adults                                                                     AS cms_max_adults,
       bos.max_children                                                                   AS cms_max_children,
       bos.max_infants                                                                    AS cms_max_infants,
       bos.max_dependants                                                                 AS cms_max_dependants,
       bos.max_child_age                                                                  AS cms_max_child_age,
       bos.child_age_description                                                          AS cms_child_age_description,
       bos.infant_age_description                                                         AS cms_infant_age_description,
       REGEXP_REPLACE(bots.child_policy,
                      '&nbsp;|<span style=""font-size: 10pt;"">|</?span>|</?div>|</?br>') AS child_policy_gb,
       md.rate_plan_name                                                                  AS mari_rate_plan_name,
       md.max_adults                                                                      AS mari_max_adults,
       md.max_children                                                                    AS mari_max_children,
       md.max_infants                                                                     AS mari_max_infants,
       md.max_dependants                                                                  AS mari_max_dependants,
       md.free_children                                                                   AS mari_free_children,
       md.free_infants                                                                    AS mari_free_infants,
       md.has_a_child_rate                                                                AS mari_has_a_child_rate,
       md.has_a_infant_rate                                                               AS mari_has_a_infant_rate

FROM data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bos
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hsos ON bos.id = hsos.hotel_offer_id
    INNER JOIN data_vault_mvp.dwh.se_sale ss ON 'A' || hsos.hotel_sale_id = ss.se_sale_id
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_translation_snapshot bots
               ON bos.id = bots.offer_id AND bots.locale = 'en_GB'
    INNER JOIN data_vault_mvp.dwh.cms_mari_link cml ON bos.id = cml.offer_id
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot ps ON cml.product_id = ps.id
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrps ON ps.id = hrps.hotel_product_id
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON ps.hotel_id = h.id
    LEFT JOIN  mari_details md ON hrps.rate_code = md.rate_code AND
                                  hrps.rack_rate_code = md.rack_rate_code AND
                                  h.hotel_code = md.hotel_code
WHERE LOWER(ss.data_model) = 'new data model'
    );
------------------------------------------------------------------------------------------------------------------------

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
                rps.cts_commission AS mari_cash_to_settle_commission,
                scml.offer_id      AS cms_offer_id
         FROM latest_vault.mari.rate_plan rps
             INNER JOIN latest_vault.mari.room_type rts ON rps.room_type_id = rts.id
             INNER JOIN latest_vault.mari.hotel hs ON rts.hotel_id = hs.id
             LEFT JOIN  se.data.se_cms_mari_link scml ON scml.hotel_rate_rack_code = hs.code || ':' || rps.code || ':' || rps.rack_code
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
       md.cms_offer_id,
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
    );
------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW collab.quality_assurance.commission_qa AS
(
WITH agg_to_global AS (
    SELECT ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.hotel_id,
           ssa.commission                 AS cms_sale_commission,
           ssa.commission_type            AS cms_sale_commission_type,
           ssa.sale_active,
           COUNT(DISTINCT ssa.se_sale_id) AS territory_sales
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.sale_type = 'Hotel'
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT atg.salesforce_opportunity_id,
--        atg.hotel_id,
       atg.company_name                                                         AS cms_company_name,
       a.name                                                                   AS sf_account_name,
       atg.sale_active,
       atg.territory_sales,
       atg.cms_sale_commission,
       atg.cms_sale_commission_type,
       h.commission                                                             AS cms_hotel_commission,
       h.commission_type                                                        AS cms_hotel_commission_type,
       o.proposed_start_date__c                                                 AS sf_proposed_start_date,
       o.percentage_commission__c / 100                                         AS sf_opportunity_commission,
       os.name                                                                  AS sf_offer_name,
       os.percentage_commission__c / 100                                        AS sf_offer_commission,
       COALESCE(cms_sale_commission = cms_hotel_commission
                    AND cms_hotel_commission = sf_opportunity_commission
                    AND sf_opportunity_commission = sf_offer_commission, FALSE) AS all_collumns_match
FROM agg_to_global atg
    LEFT JOIN hygiene_snapshot_vault_mvp.cms_mysql.hotel h ON atg.hotel_id = h.id
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(o.id, 15)
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.offers os ON atg.salesforce_opportunity_id = LEFT(os.opportunity__c, 15)
    );
------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW collab.quality_assurance.currency_mari_qa COPY GRANTS AS
(
WITH agg_to_global AS (
    SELECT ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.sale_active,
           ssa.base_currency                      AS cms_sale_currency,
           COUNT(DISTINCT ssa.se_sale_id)         AS territory_sales,
           LISTAGG(DISTINCT ssa.se_sale_id, ', ') AS list_sale_ids
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.sale_type = 'Hotel'
    GROUP BY 1, 2, 3, 4
),
     offer_details AS (
         SELECT hrp.rack_rate_code,
                hrp.rate_code,
                h.hotel_code,
                so.offer_active,
                so.se_offer_id
         FROM data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrp
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.product p ON hrp.hotel_product_id = p.id
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.hotel h ON p.hotel_id = h.id
             INNER JOIN data_vault_mvp.dwh.se_offer so ON p.id = so.product_id
     )

SELECT atg.salesforce_opportunity_id,
       atg.list_sale_ids,
       hs.code                                  AS hotel_code,
       od.se_offer_id,
       atg.company_name                         AS cms_company_name,
       a.name                                   AS sf_account_name,
       a.currencyisocode                        AS sf_account_currency,
       a2.name                                  AS sf_third_party_account_name,
       a2.currencyisocode                       AS sf_third_party_account_currency,
       atg.sale_active,
       atg.territory_sales,
       atg.cms_sale_currency,
       o.proposed_start_date__c                 AS sf_proposed_start_date,
       hs.currency                              AS mari_hotel_currency,
       rps.name                                 AS mari_rate_plan_name,
       rps.currency                             AS mari_rate_plan_currency,
       IFF(od.offer_active = TRUE, TRUE, FALSE) AS offer_active,
       COALESCE(atg.cms_sale_currency = mari_hotel_currency
                    AND mari_hotel_currency = mari_rate_plan_currency
                    AND mari_rate_plan_currency = sf_third_party_account_currency,
                FALSE)                          AS all_columns_match
FROM agg_to_global atg
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(id, 15)
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a2 ON o.third_party_provider__c = a2.id
    LEFT JOIN latest_vault.mari.hotel hs ON LEFT(a.id, 15) = hs.code
    LEFT JOIN latest_vault.mari.room_type rts ON hs.id = rts.hotel_id
    LEFT JOIN latest_vault.mari.rate_plan rps ON rts.id = rps.room_type_id
    LEFT JOIN offer_details od ON hs.code = od.hotel_code AND rps.code = od.rate_code AND rps.rack_code = od.rack_rate_code
    )
;
------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW collab.quality_assurance.currency_sf_qa COPY GRANTS AS
(
WITH agg_to_global AS (
    SELECT ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.sale_active,
           ssa.base_currency              AS cms_sale_currency,
           COUNT(DISTINCT ssa.se_sale_id) AS territory_sales
    FROM se.data.se_sale_attributes ssa
    WHERE ssa.sale_type = 'Hotel'
    GROUP BY 1, 2, 3, 4
)

SELECT atg.salesforce_opportunity_id,
       atg.company_name         AS cms_company_name,
       a.name                   AS sf_account_name,
       atg.sale_active,
       atg.territory_sales,
       atg.cms_sale_currency,
       o.proposed_start_date__c AS sf_proposed_start_date,
       o.currencyisocode        AS sf_opportunity_currency,
       a.currencyisocode        AS sf_account_currency,
       a2.name                  AS sf_third_party_account_name,
       a2.currencyisocode       AS sf_third_party_account_currency,
       os.name                  AS sf_offer_name,
       os.currencyisocode       AS sf_offer_currency,
       COALESCE(atg.cms_sale_currency = sf_opportunity_currency
                    AND sf_opportunity_currency = sf_account_currency
                    AND sf_account_currency = sf_offer_currency
                    AND sf_account_currency = sf_third_party_account_currency
           , FALSE)             AS all_columns_match
FROM agg_to_global atg
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON atg.salesforce_opportunity_id = LEFT(id, 15)
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a ON o.accountid = a.id
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account a2 ON o.third_party_provider__c = a2.id
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.offers os ON o.id = os.opportunity__c
    );

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW collab.quality_assurance.emails_cms_salesforce_comparison COPY GRANTS AS
--This query will compare the booking summary email addresses between CMS and Salesforce
WITH reservation_email AS
         (
             SELECT company_id,
                    LISTAGG(TRIM(email_address_string), ';') AS reservation_email_address
             FROM (
                 SELECT *
                 FROM raw_vault_mvp.cms_mysql.company_email_address
                     QUALIFY ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY loaded_at DESC) = 1
             ) cea
             GROUP BY 1
         )
SELECT DISTINCT
       sa.salesforce_account_id,
       sa.hotel_code,
       sa.sale_type,
       sa.product_configuration,
       sa.product_type,
       sa.product_line,
       sa.company_id,
       sa.salesforce_proposed_start_date,
       REPLACE(REPLACE(sca.weekly_summary_receiver_emails, ' ', ''), ';', ',') AS cms_weekly_summary_receiver_emails,
       REPLACE(REPLACE(sfa.booking_summaries_emails__c, ' ', ''), ';', ',')    AS salesforce_booking_summaries_emails,
       REPLACE(REPLACE(sfa.confirmation_emails__c, ' ', ''), ';', ',')         AS salesforce_confirmation_emails,
       REPLACE(REPLACE(re.reservation_email_address, ' ', ''), ';', ',')       AS cms_reservation_email
FROM se.data.se_sale_attributes sa
    LEFT JOIN se.data.se_company_attributes sca ON sca.company_id::VARCHAR = sa.company_id::VARCHAR
    LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.account sfa ON sfa.id = sa.salesforce_account_id
    LEFT JOIN reservation_email re ON re.company_id::VARCHAR = sa.company_id::VARCHAR;
------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW collab.quality_assurance.location_by_hotel COPY GRANTS AS
SELECT h.hotel_code,
       li.id                                           AS location_id,
       co.name                                         AS country_name,
       cd.name                                         AS country_division_name,
       ci.name                                         AS city_name,
       co.name || ' > ' || cd.name || ' > ' || ci.name AS string_name
FROM data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.location_info_snapshot li ON li.id = h.location_info_id
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.city_snapshot ci ON ci.id = li.city_id
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_division_snapshot cd ON cd.id = li.division_id
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_snapshot co ON co.id = li.country_id
;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW collab.quality_assurance.location_options COPY GRANTS AS
SELECT li.id                                           AS location_id,
       co.name                                         AS country_name,
       cd.name                                         AS country_division_name,
       ci.name                                         AS city_name,
       co.name || ' > ' || cd.name || ' > ' || ci.name AS string_name
FROM data_vault_mvp.cms_mysql_snapshots.location_info_snapshot li
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.city_snapshot ci ON ci.id = li.city_id
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_division_snapshot cd ON cd.id = li.division_id
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.country_snapshot co ON co.id = li.country_id
;
------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW collab.quality_assurance.missing_sf_offer_ids COPY GRANTS AS
WITH missing_sf_offer_ids AS (
    SELECT mr.*,
           off.offer_id__c::varchar AS salesforce_offer_id,
           off.name                 AS salesforce_offer_name,
           comp.cms_base_offer_id,
           comp.cms_offer_active
    FROM collab.quality_assurance.cms_mari_rate_plan mr
        LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity op ON LEFT(op.id, 15) = mr.salesforce_opportunity_id
        LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.offers off ON op.id = off.opportunity__c
        AND mr.cms_offer_id::varchar = off.offer_id__c::varchar
        LEFT JOIN collab.coo.cms_mari_salesforce_comparison comp ON comp.cms_base_offer_id = mr.cms_offer_id::varchar
    WHERE off.offer_id__c IS NULL --and SALESFORCE_PROPOSED_START_DATE between '2020-04-01' and current_date
      AND sale_active_in_one_territory
      AND comp.cms_offer_active
      AND comp.cms_base_offer_id = offer_id --and mr.SALESFORCE_OPPORTUNITY_ID in ('0066900001Nl2RF')
    --and salesforce_proposed_start_date BETWEEN CURRENT_DATE-7 AND CURRENT_DATE
)

SELECT salesforce_opportunity_id      AS opp,
       salesforce_account_id          AS sf_acc,
       company_name,
       salesforce_proposed_start_date AS psd,
       offer_id,
       offer_name,
       rate_code,
       hotel_rate_rack_code,
       cms_offer_id,
       mari_rate_plan_name,
       mari_room_name,
       salesforce_offer_id            AS sf_offer_id,
       salesforce_offer_name          AS sf_offer_name
FROM missing_sf_offer_ids
ORDER BY psd DESC, opp
;
------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW collab.quality_assurance.offer_description COPY GRANTS AS
(
SELECT ss.se_sale_id,
       ss.salesforce_opportunity_id,
       ss.company_name,
       ss.sale_active,
       ss.start_date,
       ss.product_configuration,
       ss.salesforce_proposed_start_date,
       bos.id                                                                                                  AS offer_id,
       bots.name                                                                                               AS offer_name_gb,
       bos.active,
       cml.product_id,
       hrps.rate_code,
       hrps.rack_rate_code,
       h.hotel_code,
       bos.max_adults                                                                                          AS cms_max_adults,
       bos.max_children                                                                                        AS cms_max_children,
       bos.max_infants                                                                                         AS cms_max_infants,
       bos.max_dependants                                                                                      AS cms_max_dependants,
       bos.max_child_age                                                                                       AS cms_max_child_age,
       bos.child_age_description                                                                               AS cms_child_age_description,
       bos.infant_age_description                                                                              AS cms_infant_age_description,
       REGEXP_REPLACE(bots.description,
                      '&nbsp;|<span style=""font-size: 10pt;"">|</?span>|</?div>|</?br>|<div style="""">|<b>') AS description_gb,
       REGEXP_REPLACE(bots.summary,
                      '&nbsp;|<span style=""font-size: 10pt;"">|</?span>|</?div>|</?br>|<div style="""">|<b>') AS summary_gb,

       description_gb REGEXP '.*(Pets are not allowed in this hotel.).*'                                       AS pets_not_allowed_description,
       o.parking_charges__c                                                                                    AS parking_charges,
       o.local_taxes__c                                                                                        AS local_taxes,
       o.pets_accepted__c                                                                                      AS pets_accepted,
       o.pets_taxes_specifications__c                                                                          AS pets_taxes_specifications

FROM data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bos
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hsos ON bos.id = hsos.hotel_offer_id
    INNER JOIN data_vault_mvp.dwh.se_sale ss ON 'A' || hsos.hotel_sale_id = ss.se_sale_id
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_translation_snapshot bots
               ON bos.id = bots.offer_id AND bots.locale = 'en_GB'
    INNER JOIN data_vault_mvp.dwh.cms_mari_link cml ON bos.id = cml.offer_id
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.product_snapshot ps ON cml.product_id = ps.id
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_rate_plan_snapshot hrps ON ps.id = hrps.hotel_product_id
    INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot h ON ps.hotel_id = h.id
    LEFT JOIN  hygiene_snapshot_vault_mvp.sfsc.opportunity o ON ss.salesforce_opportunity_id = LEFT(o.id, 15)
    );

------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE VIEW collab.quality_assurance.vat_qa COPY GRANTS AS
(
WITH agg_to_global AS (
    SELECT DISTINCT
           ssa.salesforce_opportunity_id,
           ssa.company_name,
           ssa.posu_country                   AS cms_posu_country,
           o.country__c                       AS sf_posu_country,
           ssa.sale_active,
           ssa.hotel_code,
           ssa.start_date::DATE               AS sale_start_date,
           ssa.salesforce_proposed_start_date AS sf_proposed_start_date
    FROM se.data.se_sale_attributes ssa
        LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON ssa.salesforce_opportunity_id = LEFT(o.id, 15)
    WHERE ssa.sale_type = 'Hotel'
)
SELECT atg.salesforce_opportunity_id,
       atg.company_name,
       atg.cms_posu_country,
       atg.sf_posu_country,
       atg.sale_active,
       atg.hotel_code,
       atg.sale_start_date,
       atg.sf_proposed_start_date,
       IFF(hs.vat_exclusive = 1, 'YES', 'NO') AS apply_vat,
       CASE
           WHEN atg.sf_posu_country = 'UNITED KINGDOM' AND hs.vat_exclusive = 1
               THEN TRUE
           WHEN atg.sf_posu_country != 'UNITED KINGDOM' AND hs.vat_exclusive = 0
               THEN TRUE
           ELSE FALSE
           END                                AS has_vat_applied_correctly
FROM agg_to_global atg
    LEFT JOIN data_vault_mvp.cms_mysql_snapshots.hotel_snapshot hs ON atg.hotel_code = hs.hotel_code
    )
;


SELECT GET_DDL('table', 'collab.coo.cms_mari_salesforce_comparison');


CREATE OR REPLACE VIEW collab.coo.cms_mari_salesforce_comparison
    COPY GRANTS
AS
WITH mari_details AS (
    --collate mari details at rate plan level
    SELECT rps.id,
           rps.name                                           AS rate_plan_name,
           rts.name                                           AS room_type_name,
           rps.date_created,
           rps.room_type_id,
           hs.code || ':' || rps.code || ':' || rps.rack_code AS hotel_rate_rack_code,
           hs.name                                            AS hotel_name,
           hs.code                                            AS hotel_code,
           rps.code                                           AS rate_code,
           rps.rack_code,
           rps.currency
    FROM latest_vault.mari.rate_plan rps
        INNER JOIN latest_vault.mari.room_type rts ON rps.room_type_id = rts.id
        INNER JOIN latest_vault.mari.hotel hs ON rts.hotel_id = hs.id
),
     live_acc AS (

         SELECT salesforce_account_id,
                MIN(start_date) AS sale_start_date
         FROM se.data.se_sale_attributes
         WHERE sale_active
         GROUP BY 1
         HAVING sale_start_date >= '2020-05-01'
     )
SELECT soa.se_offer_id           AS cms_se_offer_id,
       soa.base_offer_id         AS cms_base_offer_id,
       soa.offer_name            AS cms_offer_name,
       soa.offer_name_object     AS cms_offer_name_object,
       soa.offer_active          AS cms_offer_active,
       soa.hotel_rate_plan_id    AS cms_hotel_rate_plan_id,
       soa.hotel_code            AS cms_hotel_code,
       soa.rate_code             AS cms_rate_code,
       soa.rack_rate_code        AS cms_rack_rate_code,
       scml.product_id           AS link_product_id,
       scml.hotel_code           AS link_hotel_code,
       scml.rate_code            AS link_rate_code,
       scml.rack_rate_code       AS link_rack_rate_code,
       scml.hotel_rate_rack_code AS link_hotel_rate_rack_code,
       la.sale_start_date        AS cms_sale_start_date,
       md.id                     AS mari_rate_plan_id,
       md.rate_plan_name         AS mari_rate_plan_name,
       md.room_type_name         AS mari_room_type_name,
       md.date_created           AS mari_date_created,
       md.room_type_id           AS mari_room_type_id,
       md.hotel_name             AS mari_hotel_name,
       md.hotel_code             AS mari_hotel_code,
       md.rate_code              AS mari_rate_code,
       md.rack_code              AS mari_rack_code,
       md.currency               AS mari_currency,
       os.id                     AS sf_offer_id,
       os.opportunity__c         AS sf_opportunity_id,
       o.accountid               AS sf_account_id,
       os.offer_id__c            AS sf_offer__c_id
FROM se.data.se_offer_attributes soa
    INNER JOIN se.data.se_cms_mari_link scml ON soa.base_offer_id = scml.offer_id
    INNER JOIN mari_details md ON scml.hotel_rate_rack_code = md.hotel_rate_rack_code
    INNER JOIN live_acc la ON LEFT(la.salesforce_account_id, 15) = md.hotel_code
    LEFT JOIN  hygiene_snapshot_vault_mvp.sfsc.offers os ON soa.base_offer_id::VARCHAR = os.offer_id__c
    LEFT JOIN  hygiene_snapshot_vault_mvp.sfsc.opportunity o ON os.opportunity__c = o.id;
