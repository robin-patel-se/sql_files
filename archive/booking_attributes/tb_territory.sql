-- tb site id
-- case [Site Id]
-- WHEN 44 then "BE"
-- WHEN 43 then "DK"
-- WHEN 42 then "NL"
-- WHEN 2 then "TB-BE_NL"
-- WHEN 4 then "TB-BE_FR"
-- WHEN 3 then "DE"
-- WHEN 23 then "DK"
-- WHEN 1 then "TB-NL"
-- WHEN 46 then "TL"
-- WHEN 47 then "UK"
-- WHEN 45 then "DE"
-- WHEN NULL then "undefined"
-- else "unknown"
-- END


SELECT *
FROM data_vault_mvp.travelbird_cms.orders_order_snapshot oos;
SELECT domain,
       id,
       CASE
           WHEN id = 1 THEN 'TB-NL' --sales.travelbird.nl
           WHEN id = 2 THEN 'TB-BE_NL' --sales.travelbird.be
           WHEN id = 3 THEN 'DE' --sales.travelbird.de
           WHEN id = 4 THEN 'TB-BE_FR' --sales.fr.travelbird.be
           WHEN id = 23 THEN 'DK' --sales.travelbird.dk
           WHEN id = 42 THEN 'NL' --nl.sales.secretescapes.com
           WHEN id = 43 THEN 'DK' -- dk.sales.secretescapes.com
           WHEN id = 44 THEN 'BE' -- be.sales.secretescapes.com
           WHEN id = 45 THEN 'DE' --de.sales.secretescapes.com
           WHEN id = 46 THEN 'TL' --oferty.travelist.pl
           WHEN id = 47 THEN 'UK' --co.uk.sales.secretescapes.com
           WHEN id IS NULL THEN 'undefined'
           ELSE 'unknown'
           END AS territory
FROM data_vault_mvp.travelbird_cms.django_site_snapshot dss;

SELECT site_id,
       CASE
           WHEN tb.site_id = 1 THEN 'TB-NL' --sales.travelbird.nl
           WHEN tb.site_id = 2 THEN 'TB-BE_NL' --sales.travelbird.be
           WHEN tb.site_id = 3 THEN 'DE' --sales.travelbird.de
           WHEN tb.site_id = 4 THEN 'TB-BE_FR' --sales.fr.travelbird.be
           WHEN tb.site_id = 23 THEN 'DK' --sales.travelbird.dk
           WHEN tb.site_id = 42 THEN 'NL' --nl.sales.secretescapes.com
           WHEN tb.site_id = 43 THEN 'DK' -- dk.sales.secretescapes.com
           WHEN tb.site_id = 44 THEN 'BE' -- be.sales.secretescapes.com
           WHEN tb.site_id = 45 THEN 'DE' --de.sales.secretescapes.com
           WHEN tb.site_id = 46 THEN 'TL' --oferty.travelist.pl
           WHEN tb.site_id = 47 THEN 'UK' --co.uk.sales.secretescapes.com
           WHEN tb.site_id IS NULL THEN 'undefined'
           ELSE 'unknown'
           END AS territory,
       se.data.SE_SALE_TRAVEL_TYPE(territory, cou)
       count(*)
FROM data_vault_mvp.dwh.tb_booking tb
GROUP BY 1, 2;


self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2020-11-30 00:00:00' --end '2020-11-30 00:00:00'

self_describing_task --include 'se/data/dwh/tb_booking.py'  --method 'run' --start '2020-11-30 00:00:00' --end '2020-11-30 00:00:00'
self_describing_task --include 'se/data/dwh/fact_booking.py'  --method 'run' --start '2020-11-30 00:00:00' --end '2020-11-30 00:00:00'
self_describing_task --include 'se/data/dwh/fact_complete_booking.py'  --method 'run' --start '2020-11-30 00:00:00' --end '2020-11-30 00:00:00';

SELECT * FROM se.data.dim_sale ds

SELECT get_ddl('table', 'se.data.dim_sale');


CREATE OR REPLACE VIEW se_dev_robin.data.dim_sale
    COPY GRANTS
    AS (

        (SELECT
            se.se_sale_id,
            se.sale_name,
            se.sale_product,
            se.sale_type,
            se.product_type,
            se.product_configuration,
            se.product_line,
            se.data_model,
            se.start_date as sale_start_date,
            se.end_date as sale_end_date,
            se.sale_active,
            se.posa_territory,
            se.posa_country,
            se.posu_country,
            se.posu_division,
            se.posu_city,
            se.travel_type,
            se.target_account_list,
            COALESCE(pc.posu_sub_region, 'Other') AS posu_sub_region,
            COALESCE(pc.posu_region, 'Other') AS posu_region,
            COALESCE(pc.posu_cluster, 'Other') AS posu_cluster,
            COALESCE(pc.posu_cluster_region, 'Other') AS posu_cluster_region,
            COALESCE(pc.posu_cluster_sub_region, 'Other') AS posu_cluster_sub_region,
            'SECRET_ESCAPES' AS tech_platform

        FROM data_vault_mvp_dev_robin.dwh.se_sale se
            LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation pc ON se.posu_categorisation_id = pc.posu_categorisation_id
        WHERE se.class IS DISTINCT FROM 'com.flashsales.sale.ConnectedWebRedirectSale'
        )

        UNION ALL

        (SELECT
            tb.se_sale_id,
            tb.short_title as sale_name,
            tb.sale_product,
            tb.sale_type,
            tb.product_type,
            tb.product_configuration,
            tb.product_line,
            tb.data_model,
            tb.pub_date as sale_start_date,
            tb.end_date as sale_end_date,
            tb.sale_active,
            tb.posa_territory,
            tb.posa_country,
            tb.posu_country,
            tb.posu_division,
            tb.posu_city,
            tb.travel_type,
            tb.target_account_list,
            COALESCE(pc.posu_sub_region, 'Other') AS posu_sub_region,
            COALESCE(pc.posu_region, 'Other') AS posu_region,
            COALESCE(pc.posu_cluster, 'Other') AS posu_cluster,
            COALESCE(pc.posu_cluster_region, 'Other') AS posu_cluster_region,
            COALESCE(pc.posu_cluster_sub_region, 'Other') AS posu_cluster_sub_region,
            'TRAVELBIRD' AS tech_platform

        FROM data_vault_mvp_dev_robin.dwh.tb_offer tb
            LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.posu_categorisation pc ON tb.posu_categorisation_id = pc.posu_categorisation_id
            -- currently have some offers in tb that don't have a se sale id
         WHERE tb.se_sale_id IS NOT NULL
         -- currently have some offers in tb that have the same sale id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tb.se_sale_id ORDER BY tb.updated_at DESC) = 1)
    );


SELECT * FROM se_dev_robin.data.fact_booking fb;
SELECT * FROM se_dev_robin.data.fact_complete_booking fb;