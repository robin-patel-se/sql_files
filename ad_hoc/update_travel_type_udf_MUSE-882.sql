self_describing_task --include 'se/data/udfs/udf_functions.py'  --method 'run' --start '2021-08-25 00:00:00' --end '2021-08-25 00:00:00'

self_describing_task --include 'se/finance/udfs/udf_functions.py'  --method 'run' --start '2021-08-25 00:00:00' --end '2021-08-25 00:00:00'


SELECT DISTINCT
       se.data.posa_category_from_territory(ds.posa_territory)         AS posa_category,
       se.data.se_sale_travel_type(ds.posa_territory, ds.posu_country) AS travel_type,
       ds.posu_country,
       ds.cm_region
FROM se.data.dim_sale ds
WHERE ds.travel_type = 'Domestic'
  AND se.data.posa_category_from_territory(ds.posa_territory) = 'DACH'
;

--found instances where Italy is displaying in CM region for DACH domestic sales
------------------------------------------------------------------------------------------------------------------------

SELECT ds.se_sale_id,
       ds.posa_territory,
       ds.posu_country,
       ds.posu_city,
       ds.posu_division,
       ds.cm_region
FROM dim_sale ds
WHERE ds.travel_type = 'Domestic'
  AND se.data.posa_category_from_territory(ds.posa_territory) = 'DACH'
  AND ds.posu_country = 'Switzerland'
  AND ds.cm_region = 'Italy';

SELECT *
FROM se.data.se_booking sb
WHERE sb.booking_status = 'COMPLETE';



SELECT CASE

           WHEN posa_territory IN ('BE', 'TB-BE_FR', 'TB-BE_NL')
               AND cm_region = 'BENL'
               THEN 'Domestic'

           WHEN posa_territory IN ('CH', 'DE', 'AT')
               AND cm_region IN ('AT & CH', 'Germany')
               THEN 'Domestic'

           WHEN posa_territory = 'DK'
               AND cm_region = 'Scandi'
               THEN 'Domestic'

           WHEN posa_territory = 'ES'
               AND cm_region = 'Spain & Portugal'
               THEN 'Domestic'

           WHEN posa_territory IN ('HK', 'ID', 'MY', 'SG')
               AND cm_region = 'APAC'
               THEN 'Domestic'

           WHEN posa_territory = 'IT'
               AND cm_region IN ('Italy', 'South Tyrol')
               THEN 'Domestic'

           WHEN posa_territory IN ('NL', 'TB-NL')
               AND cm_region = 'BENL'
               THEN 'Domestic'

           WHEN posa_territory = 'NO'
               AND cm_region = 'Scandi'
               THEN 'Domestic'

           WHEN posa_territory = 'SE'
               AND cm_region = 'Scandi'
               THEN 'Domestic'

           WHEN posa_territory = 'UK'
               AND cm_region = 'UK'
               THEN 'Domestic'

           WHEN posa_territory = 'US'
               AND cm_region = 'Americas'
               THEN 'Domestic'

           WHEN posa_territory = 'PL'
               AND cm_region = 'Poland'
               THEN 'Domestic'

           WHEN posa_territory = 'FR'
               AND cm_region = 'France'
               THEN 'Domestic'

           WHEN posa_territory IN ('CZ', 'HU', 'SK')
               AND cm_region IN ('Balkan CEE', 'Non-Balkan CEE')
               THEN 'Domestic'

           ELSE 'International'
           END AS travel_type
FROM se.data.dim_sale ds;
self_describing_task --include 'se/data/udfs/udf_functions.py'  --method 'run' --start '2021-08-26 00:00:00' --end '2021-08-26 00:00:00'
self_describing_task --include 'se/finance/udfs/udf_functions.py'  --method 'run' --start '2021-08-26 00:00:00' --end '2021-08-26 00:00:00'


------------------------------------------------------------------------------------------------------------------------
--check udf changes
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.posu_categorisation;
DROP TABLE collab.fpa.travel_type_udf_test;
CREATE OR REPLACE VIEW collab.fpa.travel_type_udf_test COPY GRANTS AS
(
SELECT ds.posa_territory,
       ds.posu_country,
       ds.posu_division,
       ds.posu_city,
       ds.se_sale_id,
       ds.sale_active,
       se_dev_robin.data.se_sale_travel_type(ds.posa_territory, ds.posu_country)               AS old_travel_type,
       se_dev_robin.data.se_sale_travel_type(ds.posa_territory, ds.posu_country, ds.posu_city) AS new_travel_type
FROM se.data.dim_sale ds
WHERE old_travel_type != new_travel_type
    );

GRANT SELECT ON TABLE collab.fpa.travel_type_udf_test TO ROLE personal_role__dharmitabhanderi;
GRANT SELECT ON TABLE collab.fpa.travel_type_udf_test TO ROLE personal_role__samanthamandeldallal;
GRANT SELECT ON TABLE collab.fpa.travel_type_udf_test TO ROLE personal_role__niroshanbalakumar;

SELECT *
FROM collab.fpa.travel_type_udf_test;



SELECT CASE
           WHEN posa_territory IN ('AT', 'CH', 'DE')
               AND posu_country IN ('Austria', 'Switzerland', 'Germany')
               AND posu_city NOT IN ('Ascona', 'Bagnes', 'Bellinzona', 'Brigels', 'Champery', 'Chandolin', 'Crans-Montana', 'Geneva', 'Geneve', 'Lausanne', 'Le Chable', 'Les Crosets',
                                     'Locarno', 'Lugano', 'Martigny', 'Minusio', 'Mont Pelerin', 'Montana', 'Montreux', 'Morges', 'Munster', 'Ollon', 'Randogne', 'Saillon', 'Verbier',
                                     'Vercorin', 'Vevey', 'Vico Morcote')
               THEN 'Domestic'

           WHEN posa_territory IN ('BE', 'TB-BE_FR', 'TB-BE_NL')
               AND posu_country IN ('Belgium', 'Netherlands', 'Luxemburg')
               AND posu_city NOT IN ('s Gravenvoeren', 'Sint-Niklaas')
               THEN 'Domestic'


           WHEN posa_territory = 'DK'
               AND posu_country IN ('Sweden', 'Denmark', 'Norway')
               THEN 'Domestic'

           WHEN posa_territory = 'ES'
               AND posu_country IN ('Portugal', 'Spain', 'Andorra')
               THEN 'Domestic'

           WHEN posa_territory IN ('HK', 'ID', 'MY', 'SG')
               AND posu_country IN ('Japan', 'Malaysia', 'Indonesia', 'Maldives', 'Thailand', 'Singapore', 'China')
               THEN 'Domestic'

           WHEN posa_territory = 'IT'
               AND posu_country IN ('Italy')
               THEN 'Domestic'

           WHEN posa_territory = 'IT'
               AND posu_country IN ('Switzerland', 'San Marino')
               AND posu_city IN ('Ascona', 'Bellinzona', 'Locarno', 'Lugano', 'Minusio', 'San Marino', 'Vico Morcote')
               THEN 'Domestic'

           WHEN posa_territory IN ('NL', 'TB-NL')
               AND posu_country IN ('Belgium', 'Netherlands', 'Luxemburg')
               AND posu_city NOT IN ('s Gravenvoeren', 'Sint-Niklaas')
               THEN 'Domestic'

           WHEN posa_territory = 'NO'
               AND posu_country IN ('Sweden', 'Denmark', 'Norway')
               THEN 'Domestic'

           WHEN posa_territory = 'SE'
               AND posu_country = 'Sweden'
               THEN 'Domestic'


           WHEN posa_territory IN ('UK', 'Conde Nast UK', 'Guardian - UK')
               AND posu_country IN ('England', 'Ireland', 'Northern Ireland', 'Republic Of Ireland', 'Scotland', 'Wales/Cymru')
               THEN 'Domestic'

           WHEN posa_territory = 'US'
               AND posu_country IN ('Canada', 'USA')
               THEN 'Domestic'

           WHEN posa_territory = 'PL'
               AND posu_country IN ('Poland', 'Polska')
               THEN 'Domestic'

           ELSE 'International'
           END
;
USE ROLE personal_role__robinpatel;

SELECT se_dev_robin.data.review_type('A12345')
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer CLONE data_vault_mvp.dwh.tb_offer;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_company_attributes CLONE data_vault_mvp.dwh.se_company_attributes;
--Need to test


-- se.bi.dim_sale
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale CLONE data_vault_mvp.dwh.dim_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.fact_sale_metrics CLONE data_vault_mvp.bi.fact_sale_metrics;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.global_sale_attributes CLONE data_vault_mvp.dwh.global_sale_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active CLONE data_vault_mvp.dwh.sale_active;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_company_attributes CLONE data_vault_mvp.dwh.se_company_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags CLONE data_vault_mvp.dwh.se_sale_tags;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_offer CLONE data_vault_mvp.dwh.tb_offer;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_translation CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_translation;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.sale_territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.sale_territory;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.territory CLONE hygiene_snapshot_vault_mvp.cms_mysql.territory;

-- dv.bi.tableau.deal_model.dim_sale
self_describing_task --include 'dv/bi/tableau/deal_model/dim_sale.py'  --method 'run' --start '2021-11-09 00:00:00' --end '2021-11-09 00:00:00'

-- dv.dwh.user_booking_review
self_describing_task --include 'dv/dwh/reviews/user_booking_review.py'  --method 'run' --start '2021-11-10 00:00:00' --end '2021-11-10 00:00:00'

-- dv.dwh.dim_sale
self_describing_task --include '/dv/dwh/transactional/dim_sale.py'  --method 'run' --start '2021-11-09 00:00:00' --end '2021-11-09 00:00:00'

-- dv.dwh.fact_booking
self_describing_task --include 'dv/dwh/transactional/fact_booking.py'  --method 'run' --start '2021-11-10 00:00:00' --end '2021-11-10 00:00:00'

-- dv.dwh.se_booking
self_describing_task --include 'dv/dwh/transactional/se_booking.py'  --method 'run' --start '2021-11-10 00:00:00' --end '2021-11-10 00:00:00'

-- dv.dwh.se_sale
self_describing_task --include 'dv/dwh/transactional/se_sale.py'  --method 'run' --start '2021-11-10 00:00:00' --end '2021-11-10 00:00:00'

-- dv.dwh.tb_booking
self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2021-11-10 00:00:00' --end '2021-11-10 00:00:00'

-- dv.dwh.tb_offer
self_describing_task --include 'dv/dwh/transactional/tb_offer.py'  --method 'run' --start '2021-11-10 00:00:00' --end '2021-11-10 00:00:00'

-- dv.dwh.user_activity
self_describing_task --include 'dv/dwh/user_attributes/user_activity.py'  --method 'run' --start '2021-11-10 00:00:00' --end '2021-11-10 00:00:00'

-- dv.dwh.user_segmentation
CREATE OR REPLACE TRANSIENT TABLE DATA_VAULT_MVP_DEV_ROBIN.DWH.USER_SUBSCRIPTION CLONE data_vault_mvp.dwh.user_subscription;
self_describing_task --include 'dv/dwh/user_attributes/user_segmentation.py'  --method 'run' --start '2021-11-10 00:00:00' --end '2021-11-10 00:00:00'

-- dv.dwh.chiasma_external_booking
self_describing_task --include 'se/data/dwh/chiasma_external_booking.py'  --method 'run' --start '2021-11-10 00:00:00' --end '2021-11-10 00:00:00'



CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.salesforce_sale_opportunity CLONE data_vault_mvp.dwh.salesforce_sale_opportunity;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale CLONE data_vault_mvp.dwh.se_sale;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.fact_booking CLONE data_vault_mvp.dwh.fact_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.sale_active CLONE data_vault_mvp.dwh.sale_active;


self_describing_task --include 'dv/dwh/transactional/global_sale_attributes.py'  --method 'run' --start '2021-11-10 00:00:00' --end '2021-11-10 00:00:00'