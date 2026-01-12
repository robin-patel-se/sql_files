SELECT DISTINCT sk.row_loaded_at
FROM latest_vault.se_api.sales_kingfisher sk;

SELECT COUNT(*)
FROM latest_vault.se_api.sales_kingfisher sk;

WITH live_sales_details AS (
    SELECT sk.id                                            AS sale_id,
           sk.second_opinion,
           sk.summary,
           sk.we_like,
           sk.main_paragraph,
           sk.hotel_details,
           sk.room_description,
           sk.travel_details,
           sk.destination_name,
           sk.deal_includes,
           sk.title,
           sk.reason_to_love,
           sk.current_sale_visitors,
           sk.times_booked,
           sk.dates_start,
           sk.dates_end,
           sk.end_date_display,
           sk.type,
           sk.offer_ids,
           sk.photos,
           sk.tags,
           sk.badges,
           sk.current_sale,
           sk.continent_id,
           sk.continent_name,
           sk.division_id,
           sk.division_name,
           sk.country_id,
           sk.country_name,
           sk.city_id,
           sk.city_name,
           sk.city_district_id,
           sk.city_district_name,
           sk.latitude,
           sk.longitude,
           sk.is_hotel_chain,
           sk.is_deposit_sale,
           sk.is_time_limited,
           sk.is_hidden_for_app,
           sk.is_catalogue,
           sk.is_connected,
           sk.display_order,
           sk.is_zero_deposit,
           sk.is_refundable,
           sk.is_dynamic_package,
           sk.is_exclusive,
           sk.is_current,
           sk.is_smart_stay,
           sk.is_editors_pick,
           sk.is_hidden_for_whitelabels,
           sk.is_mysterious,
           sk.is_package,
           sk.sale_url,
           sk.links_sale,
           sk.links_price_comparison,
           sk.links_trip_advisor,
           sk.number_of_hotel_nights,
           sk.discount_tooltip,
           sk.discount,
           sk.discount_display,
           sk.pricing_model_for_display,
           sk.rack_rate_unit,
           sk.rack_rate_for_display,
           sk.rack_rate_unit_per_person,
           sk.deposit_from_price_unit,
           sk.deposit_from_price_for_display,
           sk.deposit_from_price_unit_per_person,
           sk.currency_code,
           sk.max_number_of_adults,
           sk.show_rack_rate,
           sk.show_prices,
           sk.show_discount,
           sk.lead_rate_unit_label,
           sk.lead_rate_label,
           sk.lead_rate_tooltip,
           sk.lead_rate_unit,
           sk.lead_rate_for_display,
           sk.lead_rate_unit_per_person,
           sk.travel_type,
           sk.has_flights_available,
           sk.has_flights_included,
           sk.cancellation_summary,
           sk.cancellation_description_warning,
           sk.cancellation_description,
           sk.hash,
           sk.promotion,
           sk.month_availability,
           sk.territory,
           sk.record,
           sk.row_loaded_at = MAX(sk.row_loaded_at) OVER () AS sale_is_live
    FROM latest_vault.se_api.sales_kingfisher sk
),
     default_affiliate AS (
         -- affiliate information for the default territory affiliate
         SELECT lsd.*,
                t.id                        AS territory_id,
                t.locale,
                a.id                        AS affiliate_id,
                TRUE                        AS default_affiliate,
                a.name                      AS affiliate_name,
                a.domain                    AS affiliate_domain,
                a.url_string                AS affiliate_url_string,
                a.domain || lsd.links_sale  AS affiliate_sale_url,
                a.active = 1
                    AND main_for_domain = 1 AS affiliate_is_live
         FROM live_sales_details lsd
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.territory t ON lsd.territory = t.name
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.affiliate a ON t.default_affiliate_id = a.id
     ),
     additional_affiliates AS (
         -- list of affiliates
         SELECT lsd.*,
                t.id                          AS territory_id,
                t.locale,
                a.id                          AS affiliate_id,
                FALSE                         AS default_affiliate,
                a.name                        AS affiliate_name,
                a.domain                      AS affiliate_domain,
                a.url_string                  AS affiliate_url_string,
                a.domain || lsd.links_sale    AS affiliate_sale_url,
                a.active = 1
                    AND a.main_for_domain = 1
                    AND a.mailing IS NOT NULL AS affiliate_is_live
         FROM live_sales_details lsd
             --explode sales list based on all the affiliates are actively associated to the sale territory
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.territory t ON lsd.territory = t.name
             INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.affiliate a ON t.id = a.territory_id
     ),
     union_affiliate_lists AS (
         SELECT *
         FROM default_affiliate da
         UNION
         -- incase the logic in additional affiliates that strips default affiliate stops holding true
         SELECT *
         FROM additional_affiliates aa
     )
SELECT SHA2(aul.sale_id || aul.affiliate_id, 256) AS catalogue_key,
       *
FROM union_affiliate_lists aul
WHERE aul.sale_id = 'A38995'
;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.affiliate a
WHERE active = 1
  AND main_for_domain = 1
  AND a.territory_id = 4
;

SELECT domain, *
FROM se.data.se_affiliate
WHERE main_for_domain = 1
  AND active = 1
  AND territory_id = 4;

------------------------------------------------------------------------------------------------------------------------
-- OPTION 1:
-- HANDLE deactivations
-- take list of sale ids in current export set as active (batch)
-- take list of sale ids that are marked as active in prod table
-- delta of these are sales that have been deactivated
-- update prod table to set these is_live = FALSE and update updated_at

-- HANDLE activations/changes

------------------------------------------------------------------------------------------------------------------------
--OPTION 2:
-- Process all data in latest vault, attach an is_live column
-- hash the row hash from kingfisher with the new columns added (inc is_live)
-- on merge into table check on new row hash

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.shiro_user su
WHERE su.password_hash IS NULL;

SELECT id,
       name,
       sa.domain
FROM se.data.se_affiliate sa

WHERE domain IN (
                 'be.secretescapes.com',
                 'ch.secretescapes.com',
                 'dk.secretescapes.com',
                 'es.secretescapes.com',
                 'escapes.radiotimes.com',
                 'escapes.timeout.com',
                 'escapes.travelbook.de',
                 'id.secretescapes.com',
                 'independent.secretescapes.com',
                 'it.secretescapes.com',
                 'luxusreiseclub.urlaubsplus.de',
                 'nl.secretescapes.com',
                 'no.secretescapes.com',
                 'travelbird.at',
                 'travelbird.be',
                 'travelbird.ch',
                 'travelbird.de',
                 'travelbird.dk',
                 'travelbird.fi',
                 'travelbird.nl',
                 'travelbird.no',
                 'travelbird.se',
                 'www.confidentialescapes.co.uk',
                 'www.eveningstandardescapes.com',
                 'www.guardianescapes.com',
                 'www.hand-picked.telegraph.co.uk',
                 'www.independentescapes.com',
                 'www.lateluxury.com',
                 'www.secretescapes.com',
                 'www.secretescapes.de',
                 'www.secretescapes.se',
                 'www.travelescapes.ch'
    )
  AND active = 1
  AND main_for_domain = 1;


SELECT *
FROM se.data.se_affiliate sa
WHERE sa.domain IN ('independent.secretescapes.com');

SELECT *
FROM se.data.se_affiliate sa
WHERE sa.domain IN ('www.eveningstandardescapes.com');

SELECT *
FROM se.data.se_affiliate sa
WHERE sa.domain IN ('www.independentescapes.com');
SELECT *
FROM se.data.se_affiliate sa
WHERE sa.territory_id = 10;


------------------------------------------------------------------------------------------------------------------------
--course correct, utilising a static list of affiliates due to processing issues with active affiliates and the additional
--rows necessary to compute delta loads of affiliates that have gone inactive
CREATE OR REPLACE TRANSIENT TABLE collab.iterable_data.test_product_catalogue AS (
    WITH kingfisher_sale_details AS (
        SELECT sk.id                                            AS se_sale_id,
               sk.id,
               sk.second_opinion,
               sk.summary,
               sk.we_like,
               sk.main_paragraph,
               sk.hotel_details,
               sk.room_description,
               sk.travel_details,
               sk.destination_name,
               sk.deal_includes,
               sk.title,
               sk.reason_to_love,
               sk.current_sale_visitors,
               sk.times_booked,
               sk.dates_start,
               sk.dates_end,
               sk.end_date_display,
               sk.type,
               sk.offer_ids,
               sk.photos,
               sk.tags,
               sk.badges,
               sk.current_sale,
               sk.continent_id,
               sk.continent_name,
               sk.division_id,
               sk.division_name,
               sk.country_id,
               sk.country_name,
               sk.city_id,
               sk.city_name,
               sk.city_district_id,
               sk.city_district_name,
               sk.latitude,
               sk.longitude,
               sk.is_hotel_chain,
               sk.is_deposit_sale,
               sk.is_time_limited,
               sk.is_hidden_for_app,
               sk.is_catalogue,
               sk.is_connected,
               sk.display_order,
               sk.is_zero_deposit,
               sk.is_refundable,
               sk.is_dynamic_package,
               sk.is_exclusive,
               sk.is_current,
               sk.is_smart_stay,
               sk.is_editors_pick,
               sk.is_hidden_for_whitelabels,
               sk.is_mysterious,
               sk.is_package,
               sk.sale_url,
               sk.links_sale,
               sk.links_price_comparison,
               sk.links_trip_advisor,
               sk.number_of_hotel_nights,
               sk.discount_tooltip,
               sk.discount,
               sk.discount_display,
               sk.pricing_model_for_display,
               sk.rack_rate_unit,
               sk.rack_rate_for_display,
               sk.rack_rate_unit_per_person,
               sk.deposit_from_price_unit,
               sk.deposit_from_price_for_display,
               sk.deposit_from_price_unit_per_person,
               sk.currency_code,
               sk.max_number_of_adults,
               sk.show_rack_rate,
               sk.show_prices,
               sk.show_discount,
               sk.lead_rate_unit_label,
               sk.lead_rate_label,
               sk.lead_rate_tooltip,
               sk.lead_rate_unit,
               sk.lead_rate_for_display,
               sk.lead_rate_unit_per_person,
               sk.travel_type,
               sk.has_flights_available,
               sk.has_flights_included,
               sk.cancellation_summary,
               sk.cancellation_description_warning,
               sk.cancellation_description,
               sk.hash,
               sk.promotion,
               sk.month_availability,
               sk.territory,
               sk.record,
               sk.row_loaded_at = MAX(sk.row_loaded_at) OVER () AS sale_active
        FROM latest_vault.se_api.sales_kingfisher sk
    ),
         static_list_of_affiliates AS (
             -- static list of affiliates provided by CRM team
             -- ideally would like to replace this with an automated solution
             -- however the existing 'active' statuses in the cms affiliate table aren't up to date
             SELECT column1 AS affiliate_id,
                    column2 AS affiliate_name
             FROM
             VALUES (932, 'Secret Escapes BE'),
                    (809, 'Secret Escapes CH'),
                    (602, 'Secret Escapes DK'),
                    (893, 'Secret Escapes ES'),
                    (398, 'Time Out'),
                    (614, 'Travelbook Escapes'),
                    (1593, 'Secret Escapes ID'),
                    (874, 'Secret Escapes IT'),
                    (888, 'Urlaubsplus Luxusreiseclub'),
                    (885, 'Secret Escapes NL'),
                    (665, 'Secret Escapes NO'),
                    (2935, 'TravelBird AT'),
                    (2929, 'TravelBird BE-NL'),
                    (2934, 'TravelBird CH'),
                    (2928, 'TravelBird DE'),
                    (2931, 'TravelBird DK'),
                    (2966, 'TravelBird FI'),
                    (2927, 'TravelBird NL'),
                    (2937, 'TravelBird NO'),
                    (2932, 'TravelBird SE'),
                    (437, 'Guardian Escapes'),
                    (115, 'Telegraph Travel Hand-picked'),
                    (817, 'LateLuxury'),
                    (24, 'Secret Escapes UK'),
                    (362, 'Secret Escapes DE'),
                    (348, 'Secret Escapes SE'),
                    (770, 'Travel Escapes')
         )

    SELECT ksd.id || '-' || a.id                    AS compound_id,
           ksd.*,
           t.id                                     AS territory_id,
           t.locale                                 AS territory_locale,
           a.id                                     AS affiliate_id,
           t.default_affiliate_id = a.id            AS affiliate_is_default,
           a.name                                   AS affiliate_name,
           a.domain                                 AS affiliate_domain,
           a.url_string                             AS affiliate_url_string,
           a.domain || ksd.links_sale               AS affiliate_sale_url,
           a.active = 1                             AS affiliate_active,
           a.main_for_domain IS NOT DISTINCT FROM 1 AS affiliate_is_main_for_domain,
           --used in merge to check if columns in the row have changed
           SHA2(
                       ksd.hash ||
                       ksd.sale_active ||
                       t.id ||
                       t.locale ||
                       affiliate_id ||
                       affiliate_is_default ||
                       affiliate_name ||
                       affiliate_domain ||
                       affiliate_url_string ||
                       affiliate_sale_url ||
                       affiliate_active ||
                       affiliate_is_main_for_domain
               , 256)                               AS row_hash
    FROM kingfisher_sale_details ksd
        INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.territory t ON ksd.territory = t.name
        INNER JOIN hygiene_snapshot_vault_mvp.cms_mysql.affiliate a ON t.id = a.territory_id
        INNER JOIN static_list_of_affiliates sloa ON a.id = sloa.affiliate_id
-- WHERE ksd.se_sale_id = 'A38995';
);

USE WAREHOUSE pipe_xlarge;

GRANT USAGE ON SCHEMA collab.iterable_data TO ROLE personal_role__saurdash;

GRANT SELECT ON TABLE collab.iterable_data.test_product_catalogue TO ROLE personal_role__saurdash;

SELECT *
FROM collab.iterable_data.test_product_catalogue;

SELECT GET_DDL('table', 'collab.iterable_data.test_product_catalogue');

CREATE OR REPLACE TRANSIENT TABLE test_product_catalogue
(
    se_sale_id                         VARCHAR,
    id                                 VARCHAR,
    second_opinion                     VARCHAR,
    summary                            VARCHAR,
    we_like                            VARCHAR,
    main_paragraph                     VARCHAR,
    hotel_details                      VARCHAR,
    room_description                   VARCHAR,
    travel_details                     VARCHAR,
    destination_name                   VARCHAR,
    deal_includes                      VARCHAR,
    title                              VARCHAR,
    reason_to_love                     VARCHAR,
    current_sale_visitors              NUMBER(38, 0),
    times_booked                       NUMBER(38, 0),
    dates_start                        TIMESTAMP_NTZ,
    dates_end                          TIMESTAMP_NTZ,
    end_date_display                   VARCHAR,
    type                               VARCHAR,
    offer_ids                          ARRAY,
    photos                             ARRAY,
    lead_image_url_array               ARRAY,
    lead_image_url_with_size_array     ARRAY,
    tags                               ARRAY,
    badges                             ARRAY,
    current_sale                       BOOLEAN,
    continent_id                       NUMBER(38, 0),
    continent_name                     VARCHAR,
    division_id                        NUMBER(38, 0),
    division_name                      VARCHAR,
    country_id                         NUMBER(38, 0),
    country_name                       VARCHAR,
    city_id                            NUMBER(38, 0),
    city_name                          VARCHAR,
    city_district_id                   VARCHAR,
    city_district_name                 VARCHAR,
    latitude                           NUMBER(9, 6),
    longitude                          NUMBER(9, 6),
    is_hotel_chain                     BOOLEAN,
    is_deposit_sale                    BOOLEAN,
    is_time_limited                    BOOLEAN,
    is_hidden_for_app                  BOOLEAN,
    is_catalogue                       BOOLEAN,
    is_connected                       BOOLEAN,
    display_order                      NUMBER(38, 0),
    is_zero_deposit                    BOOLEAN,
    is_refundable                      BOOLEAN,
    is_dynamic_package                 BOOLEAN,
    is_exclusive                       BOOLEAN,
    is_current                         BOOLEAN,
    is_smart_stay                      BOOLEAN,
    is_editors_pick                    BOOLEAN,
    is_hidden_for_whitelabels          BOOLEAN,
    is_mysterious                      BOOLEAN,
    is_package                         BOOLEAN,
    sale_url                           VARCHAR,
    links_sale                         VARCHAR,
    links_price_comparison             VARCHAR,
    links_trip_advisor                 VARCHAR,
    number_of_hotel_nights             NUMBER(38, 0),
    discount_tooltip                   VARCHAR,
    discount                           NUMBER(13, 2),
    discount_display                   VARCHAR,
    pricing_model_for_display          VARCHAR,
    rack_rate_unit                     NUMBER(13, 2),
    rack_rate_for_display              VARCHAR,
    rack_rate_unit_per_person          NUMBER(13, 2),
    deposit_from_price_unit            NUMBER(13, 2),
    deposit_from_price_for_display     VARCHAR,
    deposit_from_price_unit_per_person NUMBER(13, 2),
    currency_code                      VARCHAR,
    max_number_of_adults               NUMBER(38, 0),
    show_rack_rate                     BOOLEAN,
    show_prices                        BOOLEAN,
    show_discount                      BOOLEAN,
    lead_rate_unit_label               VARCHAR,
    lead_rate_label                    VARCHAR,
    lead_rate_tooltip                  VARCHAR,
    lead_rate_unit                     NUMBER(13, 2),
    lead_rate_for_display              VARCHAR,
    lead_rate_unit_per_person          NUMBER(13, 2),
    travel_type                        VARCHAR,
    has_flights_available              BOOLEAN,
    has_flights_included               BOOLEAN,
    cancellation_summary               VARCHAR,
    cancellation_description_warning   VARCHAR,
    cancellation_description           VARCHAR,
    hash                               VARCHAR,
    promotion                          VARCHAR,
    month_availability                 ARRAY,
    territory                          VARCHAR,
    record                             VARIANT,
    sale_active                        BOOLEAN,
    territory_id                       NUMBER(38, 0),
    locale                             VARCHAR,
    affiliate_id                       NUMBER(38, 0),
    affiliate_is_default               BOOLEAN,
    affiliate_name                     VARCHAR,
    affiliate_domain                   VARCHAR,
    affiliate_url_string               VARCHAR,
    affiliate_sale_url                 VARCHAR,
    affiliate_active                   BOOLEAN,
    affiliate_is_main_for_domain       BOOLEAN,
    row_hash                           VARCHAR
);


------------------------------------------------------------------------------------------------------------------------
--unpack the url
SELECT '[
    {
        "urlWithSize": "https://secretescapes-web.imgix.net/hotels/867/1f3b274a_e062_4e18_8977_8b1ef729891f.jpg?w=$width&h=$height&fit=crop&crop=entropy&auto=format,compress",
        "caption": "",
        "url": "https://secretescapes-web.imgix.net/hotels/867/1f3b274a_e062_4e18_8977_8b1ef729891f.jpg?auto=format,compress"
    },
    {
        "urlWithSize": "https://secretescapes-web.imgix.net/hotels/867/85148210_cfba_4ad9_a16c_579028522093.jpg?w=$width&h=$height&fit=crop&crop=entropy&auto=format,compress",
        "caption": null,
        "url": "https://secretescapes-web.imgix.net/hotels/867/85148210_cfba_4ad9_a16c_579028522093.jpg?auto=format,compress"
    },
    {
        "urlWithSize": "https://secretescapes-web.imgix.net/hotels/867/c4fd5d3f_5357_40dc_b076_2ccabe7c29ec.jpg?w=$width&h=$height&fit=crop&crop=entropy&auto=format,compress",
        "caption": null,
        "url": "https://secretescapes-web.imgix.net/hotels/867/c4fd5d3f_5357_40dc_b076_2ccabe7c29ec.jpg?auto=format,compress"
    },
    {
        "urlWithSize": "https://secretescapes-web.imgix.net/hotels/867/b9891c14_2d19_4aba_bbd6_1663590b88c4.jpg?w=$width&h=$height&fit=crop&crop=entropy&auto=format,compress",
        "caption": null,
        "url": "https://secretescapes-web.imgix.net/hotels/867/b9891c14_2d19_4aba_bbd6_1663590b88c4.jpg?auto=format,compress"
    },
    {
        "urlWithSize": "https://secretescapes-web.imgix.net/hotels/867/780ab412_2d6f_46d3_8ccd_efcd69f374eb.jpg?w=$width&h=$height&fit=crop&crop=entropy&auto=format,compress",
        "caption": "Carbon Sense City Spa",
        "url": "https://secretescapes-web.imgix.net/hotels/867/780ab412_2d6f_46d3_8ccd_efcd69f374eb.jpg?auto=format,compress"
    }
]'::array AS url_field;

WITH unpack_url AS (
    --unpack the url field from the json objects within array
    SELECT sk.id,
           sk.photos,
           params.index,
           params.value['url']::VARCHAR         AS url,
           params.value['urlWithSize']::VARCHAR AS url_with_size
    FROM latest_vault.se_api.sales_kingfisher sk,
         LATERAL FLATTEN(INPUT => sk.photos, OUTER => TRUE) params
    WHERE sk.id = '113565'
)
--aggregate the url columns back up to original grain
SELECT uu.id,
       ARRAY_AGG(uu.url) WITHIN GROUP ( ORDER BY index)           AS url_array,
       ARRAY_AGG(uu.url_with_size) WITHIN GROUP ( ORDER BY index) AS url_with_size_array
FROM unpack_url uu
GROUP BY 1;

CREATE SCHEMA latest_vault_dev_robin.se_api;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.se_api.sales_kingfisher CLONE latest_vault.se_api.sales_kingfisher;

self_describing_task --include 'dv/dwh/iterable/product.py'  --method 'run' --start '2021-10-06 00:00:00' --end '2021-10-06 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__product__model_data;
DROP TABLE data_vault_mvp_dev_robin.dwh.iterable__product;

------------------------------------------------------------------------------------------------------------------------
-- manufacture a sale going offline
SELECT MAX(row_loaded_at)
FROM latest_vault_dev_robin.se_api.sales_kingfisher sk;
--2021-10-07 11:05:25.219000000
-- Find 10 sales that is currently live
SELECT *
FROM latest_vault_dev_robin.se_api.sales_kingfisher sk
WHERE sk.row_loaded_at IS NOT DISTINCT FROM '2021-10-07 11:05:25.219000000';
-- Chosen these:
/*
ID
A39702
A39375
A39693
A39692
A39374
A39002
A39682
A39538
A39684
A39623
 */
--update the row loaded at of these sales as if they've gone offline
UPDATE latest_vault_dev_robin.se_api.sales_kingfisher target
SET target.row_loaded_at = '2021-10-06 11:05:25.219000000'
WHERE id IN (
             'A39702',
             'A39375',
             'A39693',
             'A39692',
             'A39374',
             'A39002',
             'A39682',
             'A39538',
             'A39684',
             'A39623'
    );


SELECT *
FROM latest_vault_dev_robin.se_api.sales_kingfisher sk
WHERE id IN (
             'A39702',
             'A39375',
             'A39693',
             'A39692',
             'A39374',
             'A39002',
             'A39682',
             'A39538',
             'A39684',
             'A39623'
    );

--check current rows in product table
SELECT DISTINCT ip.updated_at FROM data_vault_mvp_dev_robin.dwh.iterable__product ip
--all rows have updated at of 2021-10-07 13:17:40.454000000


-- run job again
self_describing_task --include 'dv/dwh/iterable/product.py'  --method 'run' --start '2021-10-06 00:00:00' --end '2021-10-06 00:00:00'

-- grab query id of merge and check this against snowflake history
-- 019f7367-3200-eafa-0000-02ddba5eb30a
-- shows 24 rows updated

-- check updated at times
SELECT ip.updated_at, count(*) FROM data_vault_mvp_dev_robin.dwh.iterable__product ip GROUP BY 1;
-- two updated at times now
-- UPDATED_AT	COUNT(*)
-- 2021-10-07 13:17:40.454000000	57950
-- 2021-10-07 15:03:14.902000000	24

-- check the rows that have been updated:
SELECT * FROM data_vault_mvp_dev_robin.dwh.iterable__product ip WHERE ip.updated_at = '2021-10-07 15:03:14.902000000';
-- all have been marked 'sale_active' as false

-- check se_sale_id's of updated rows
SELECT DISTINCT se_sale_id FROm data_vault_mvp_dev_robin.dwh.iterable__product ip WHERE ip.updated_at = '2021-10-07 15:03:14.902000000'
-- SE_SALE_ID
-- A39002
-- A39702
-- A39623
-- A39684
-- A39375
-- A39692
-- A39538
-- A39693
-- A39682
-- A39374
-- match list of sales updated


------------------------------------------------------------------------------------------------------------------------

(24, 'Secret Escapes UK'),
(115, 'Telegraph Travel Hand-picked'),
(348, 'Secret Escapes SE'),
(362, 'Secret Escapes DE'),
(398, 'Time Out'),
(437, 'Guardian Escapes'),
(602, 'Secret Escapes DK'),
(614, 'Travelbook Escapes'),
(665, 'Secret Escapes NO'),
(770, 'Travel Escapes'),
(809, 'Secret Escapes CH'),
(817, 'LateLuxury'),
(874, 'Secret Escapes IT'),
(885, 'Secret Escapes NL'),
(888, 'Urlaubsplus Luxusreiseclub'),
(893, 'Secret Escapes ES'),
(932, 'Secret Escapes BE'),
(1593, 'Secret Escapes ID'),
(2927, 'TravelBird NL'),
(2928, 'TravelBird DE'),
(2929, 'TravelBird BE-NL'),
(2931, 'TravelBird DK'),
(2932, 'TravelBird SE'),
(2934, 'TravelBird CH'),
(2935, 'TravelBird AT'),
(2937, 'TravelBird NO'),
(2966, 'TravelBird FI')


inspect_dependencies biapp/task_catalogue/dv/dwh/iterable/catalogue_product.py --downstream


SELECT * FROM data_vault_mvp.dwh.iterable__catalogue_product;
