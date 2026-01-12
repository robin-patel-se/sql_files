WITH tag_first_observed AS (
    SELECT
        ssts.se_sale_id,
        MIN(ssts.view_date) AS first_view_date
    FROM se.data.se_sale_tags_snapshot ssts
    WHERE ssts.has_flash_tag
    GROUP BY 1
)
SELECT
    ssa.se_sale_id,
    sst.tag_name,
    ssa.salesforce_opportunity_id,
    ssa.posa_territory_id,
    ssa.posa_territory,
    ssa.sale_name,
    ssa.company_name,
    tfo.first_view_date
FROM se.data.se_sale_tags sst
    INNER JOIN data_vault_mvp.dwh.se_sale ssa ON sst.se_sale_id = ssa.se_sale_id
    LEFT JOIN  tag_first_observed tfo ON sst.se_sale_id = tfo.se_sale_id
WHERE sst.tag_name = 'zz_flash'
  AND ssa.sale_active;


SELECT
    ssa.company_name,
    ssa.salesforce_opportunity_id,
    LISTAGG(ssa.se_sale_id, ', ')              AS list_sale_ids,
    LISTAGG(DISTINCT ssa.sale_name, ', ')      AS list_sale_name,
    LISTAGG(DISTINCT ssa.posa_territory, ', ') AS territores
FROM se.data.se_sale_tags sst
    INNER JOIN data_vault_mvp.dwh.se_sale ssa ON sst.se_sale_id = ssa.se_sale_id
WHERE sst.tag_name = 'zz_flash'
GROUP BY 1, 2;


SELECT *
FROM data_vault_mvp.dwh.se_sale
WHERE se_sale.salesforce_opportunity_id = '0066900001ZQJsy';
SELECT *
FROM se.data.se_sale_tags_snapshot ssts
WHERE ssts.has_flash_tag;

SELECT
    ssts.view_date,
    ssts.se_sale_id,
    ssts.tag_array,
    ssts.number_of_tags,
    ssts.number_of_campaign_tags,
    ssts.number_of_permanent_tags,
    ssts.has_flash_tag,
    ssts.has_no_athena_tag,
    ssts.has_hotel_only_tag,
    ssts.has_refundable_rates_tag
FROM se.data.se_sale_tags_snapshot ssts;

SELECT *,
       DATEDIFF(DAY, first_view_date, CURRENT_DATE),
       DATE_TRUNC(WEEK, first_view_date) + 7 = DATE_TRUNC(WEEK, CURRENT_DATE)
FROM dbt_dev.dbt_robinpatel_data_platform.dp_athena_new_flash_deal_category;

SELECT *
FROM dbt.bi_data_platform.dp_athena_new_flash_deal_category;


SELECT DATE_TRUNC(WEEK, CURRENT_DATE);



------------------------------------------------------------------------------------------------------------------------
--check new flash hero territory tags


/*WITH tag_first_observed AS (
    SELECT
        se_sale_id,
        MIN(view_date) AS first_view_date
    FROM data_vault_mvp.dwh.se_sale_tags_snapshot ssts
    WHERE has_flash_tag
    GROUP BY 1
)

SELECT
    ssa.se_sale_id,
    ssa.posa_territory_id,
    sst.tag_name,
    ssa.salesforce_opportunity_id,
    ssa.posa_territory,
    ssa.sale_name,
    ssa.company_name,
    tag_first_observed.first_view_date
FROM data_vault_mvp.dwh.se_sale_tags AS sst
    INNER JOIN
              data_vault_mvp.dwh.se_sale AS ssa
              ON sst.se_sale_id = ssa.se_sale_id
    LEFT JOIN tag_first_observed
              ON sst.se_sale_id = tag_first_observed.se_sale_id
WHERE sst.tag_name = 'zz_flash'
  AND ssa.sale_active
  AND DATE_TRUNC(
              WEEK, tag_first_observed.first_view_date
          ) + 7 = DATE_TRUNC(WEEK, CURRENT_DATE)*/

WITH flash_sales AS (
--list of sales with zz_flash tag and status on specific territories or all territories
    SELECT
        sst.se_sale_id,
        IFF(MAX(IFF(sst.tag_name LIKE 'zz_flash_%', 1, 0)) > 0, 'specific territories', 'all territories') AS flash_territory_status,
        LISTAGG(DISTINCT sst.tag_name, ', ') WITHIN GROUP ( ORDER BY sst.tag_name )                        AS flash_tag_list
    FROM se.data.se_sale_tags sst
        INNER JOIN data_vault_mvp.dwh.se_sale AS ssa ON sst.se_sale_id = ssa.se_sale_id
    WHERE sst.tag_name LIKE 'zz_flash%'
      AND ssa.sale_active
    GROUP BY 1
),
     territory_specific_sales AS (
         -- logic to only return territories that match the territory specific tag
         SELECT
             fs.se_sale_id,
             fs.flash_territory_status,
             fs.flash_tag_list,
             ss.salesforce_opportunity_id,
             s.tag_name,
             UPPER(SPLIT_PART(s.tag_name, '_', -1)) AS flash_territory,
             ss.posa_territory
         FROM flash_sales fs
             INNER JOIN data_vault_mvp.dwh.se_sale_tags s ON fs.se_sale_id = s.se_sale_id
             INNER JOIN data_vault_mvp.dwh.se_sale ss ON fs.se_sale_id = ss.se_sale_id
         WHERE fs.flash_territory_status = 'specific territories'
           AND s.tag_name LIKE 'zz_flash_%'
           -- filter to only return territory sales that have a posa matching the suffix of the territory specific tag
           AND flash_territory = ss.posa_territory
     ),
     stack AS (
         -- stack sales with territory specific flags ontop of sales without any

         -- sales that don't have a territory specific flash sale tag
         SELECT
             fs.se_sale_id,
             fs.flash_territory_status,
             fs.flash_tag_list
         FROM flash_sales fs
         WHERE fs.flash_territory_status = 'all territories'

         UNION ALL

         -- sales that have a territory specific flash sale tag
         SELECT
             tss.se_sale_id,
             tss.flash_territory_status,
             tss.flash_tag_list
         FROM territory_specific_sales tss
     ),
     tag_first_observed AS (
         -- we only want to return sales that are in their first week of being live
         -- we're deducing from the snapshot when was the first date a sale tag was added
         SELECT
             se_sale_id,
             MIN(view_date) AS first_view_date
         FROM data_vault_mvp.dwh.se_sale_tags_snapshot ssts
         WHERE has_flash_tag
         GROUP BY 1
     )

SELECT
    s.se_sale_id,
    s.flash_territory_status,
    s.flash_tag_list,
    ssa.se_sale_id,
    ssa.posa_territory_id,
    ssa.salesforce_opportunity_id,
    ssa.posa_territory,
    ssa.sale_name,
    ssa.company_name,
    tag_first_observed.first_view_date
FROM stack s
    INNER JOIN data_vault_mvp.dwh.se_sale AS ssa ON s.se_sale_id = ssa.se_sale_id
    LEFT JOIN  tag_first_observed ON s.se_sale_id = tag_first_observed.se_sale_id
WHERE DATE_TRUNC(
              WEEK, tag_first_observed.first_view_date
          ) + 7 = DATE_TRUNC(WEEK, CURRENT_DATE)
;



WITH flash_sales AS (
--list of sales with zz_flash tag and status on specific territories or all territories
    SELECT
        sst.se_sale_id,
        IFF(MAX(IFF(sst.tag_name LIKE 'zz_flash_%', 1, 0)) > 0, 'specific territories', 'all territories') AS flash_territory_status,
        LISTAGG(DISTINCT sst.tag_name, ', ') WITHIN GROUP ( ORDER BY sst.tag_name )                        AS flash_tag_list
    FROM se.data.se_sale_tags sst
        INNER JOIN data_vault_mvp.dwh.se_sale AS ssa ON sst.se_sale_id = ssa.se_sale_id
    WHERE sst.tag_name LIKE 'zz_flash%'
      AND ssa.sale_active
    GROUP BY 1
),
     territory_specific_sales AS (
         -- logic to only return territories that match the territory specific tag
         SELECT
             fs.se_sale_id,
             fs.flash_territory_status,
             fs.flash_tag_list,
             ss.salesforce_opportunity_id,
             s.tag_name,
             UPPER(SPLIT_PART(s.tag_name, '_', -1)) AS flash_territory,
             ss.posa_territory
         FROM flash_sales fs
             INNER JOIN data_vault_mvp.dwh.se_sale_tags s ON fs.se_sale_id = s.se_sale_id
             INNER JOIN data_vault_mvp.dwh.se_sale ss ON fs.se_sale_id = ss.se_sale_id
         WHERE fs.flash_territory_status = 'specific territories'
           AND s.tag_name LIKE 'zz_flash_%'
           -- filter to only return territory sales that have a posa matching the suffix of the territory specific tag
           AND flash_territory = ss.posa_territory
     ),
     stack AS (
         -- stack sales with territory specific flags ontop of sales without any

         -- sales that don't have a territory specific flash sale tag
         SELECT
             fs.se_sale_id,
             fs.flash_territory_status,
             fs.flash_tag_list
         FROM flash_sales fs
         WHERE fs.flash_territory_status = 'all territories'

         UNION ALL

         -- sales that have a territory specific flash sale tag
         SELECT
             tss.se_sale_id,
             tss.flash_territory_status,
             tss.flash_tag_list
         FROM territory_specific_sales tss
     ),
     tag_modelling AS (
         -- sales can go in and out of flash mode, we want to compute the most recent initialisation date
         -- this cte computes all initialisations, following cte will return the most recent
         SELECT
             sd.se_sale_id,
             sd.view_date,
             sd.has_flash_tag,
             LAG(sd.has_flash_tag) OVER (PARTITION BY se_sale_id ORDER BY sd.view_date) AS last_has_flash_tag,
             sd.has_flash_tag AND COALESCE(last_has_flash_tag, FALSE) = FALSE           AS flash_sale_initiated
         FROM data_vault_mvp.dwh.se_sale_tags_snapshot sd
     ),
     tag_initialisation AS (
         -- aggregate to sale level to output most recent initialisation date
         SELECT
             tm.se_sale_id,
             MAX(tm.view_date) AS tag_inititated_date
         FROM tag_modelling tm
         WHERE tm.flash_sale_initiated
         GROUP BY 1
     )
SELECT
    s.se_sale_id,
    s.flash_territory_status,
    s.flash_tag_list,
    ssa.se_sale_id,
    ssa.posa_territory_id,
    ssa.salesforce_opportunity_id,
    ssa.posa_territory,
    ssa.sale_name,
    ssa.company_name,
    ti.tag_inititated_date
FROM stack s
    INNER JOIN data_vault_mvp.dwh.se_sale AS ssa ON s.se_sale_id = ssa.se_sale_id
    LEFT JOIN  tag_initialisation ti ON s.se_sale_id = ti.se_sale_id
-- we only want to return sales that are in their first week of being initialised
WHERE DATE_TRUNC(
              WEEK, ti.tag_inititated_date
          ) + 7 = DATE_TRUNC(WEEK, CURRENT_DATE)
;


------------------------------------------------------------------------------------------------------------------------
-- testing to get most recent initialisation of flash

WITH sale_had_a_flash_tag AS (
    SELECT DISTINCT
        ssts.se_sale_id
    FROM data_vault_mvp.dwh.se_sale_tags_snapshot ssts
    WHERE ssts.has_flash_tag
)
SELECT
    s.view_date,
    s.se_sale_id,
    s.has_flash_tag
FROM data_vault_mvp.dwh.se_sale_tags_snapshot s
    INNER JOIN sale_had_a_flash_tag shft ON s.se_sale_id = shft.se_sale_id


WITH sample_data AS (

    SELECT
        column1 AS se_sale_id,
        column2 AS view_date,
        column3 AS has_flash_tag
    FROM
    VALUES ('A10819', '2023-01-12', TRUE),
           ('A10819', '2023-01-13', TRUE),
           ('A10819', '2023-01-14', TRUE),
           ('A10819', '2023-01-15', TRUE),
           ('A10819', '2023-01-16', TRUE),
           ('A10819', '2023-01-17', TRUE),
           ('A10819', '2023-01-18', TRUE),
           ('A10819', '2023-01-19', FALSE),
           ('A10819', '2023-01-20', TRUE),
           ('A10819', '2023-01-21', TRUE),
           ('A10819', '2023-01-22', TRUE),
           ('A10819', '2023-01-23', TRUE),
           ('A10819', '2023-01-24', TRUE),
           ('A10819', '2023-01-25', TRUE),
           ('A10819', '2023-01-26', TRUE)
),
     tag_modelling AS (
         SELECT
             sd.se_sale_id,
             sd.view_date,
             sd.has_flash_tag,
             LAG(sd.has_flash_tag) OVER (PARTITION BY se_sale_id ORDER BY sd.view_date) AS last_has_flash_tag,
             sd.has_flash_tag AND COALESCE(last_has_flash_tag, FALSE) = FALSE           AS flash_sale_initiated
         FROM sample_data sd
     )
SELECT
    tag_modelling.se_sale_id,
    MAX(tag_modelling.view_date) AS tag_inititated_date
FROM tag_modelling
WHERE tag_modelling.flash_sale_initiated
GROUP BY 1;



------------------------------------------------------------------------------------------------------------------------
/*SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_athena_new_flash_deal_category;

SELECT *
FROM dbt.bi_data_platform.dp_athena_new_flash_deal_category;

SELECT *
FROM data_vault_mvp.dwh.se_sale ss ssa
WHERE ssa.posa_territory_id = 'DE__4';


SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.territory t
WHERE id = 'DE__4';*/


/*WITH flash_sale_type AS (
    SELECT
        sst.se_sale_id,
        IFF(MAX(IFF(sst.tag_name LIKE 'zz_flash_%', 1, 0)) > 0, 'specific territories', 'all territories') AS flash_territory_status,
        LISTAGG(DISTINCT sst.tag_name, ', ') WITHIN GROUP ( ORDER BY sst.tag_name )                        AS flash_tag_list
    FROM se.data.se_sale_tags sst
        INNER JOIN data_vault_mvp.dwh.se_sale AS ssa ON sst.se_sale_id = ssa.se_sale_id
    WHERE sst.tag_name LIKE 'zz_flash%'
      AND ssa.sale_active
    GROUP BY 1
)
SELECT
    fst.se_sale_id,
    fst.flash_territory_status,
    fst.flash_tag_list,
    st.territory_id
FROM flash_sale_type fst
    INNER JOIN data_vault_mvp.dwh.se_sale ss ON fst.se_sale_id = ss.se_sale_id
    LEFT JOIN  hygiene_snapshot_vault_mvp.cms_mysql.sale_territory st
               ON TRY_TO_NUMBER(fst.se_sale_id) = st.sale_id
                   AND ss.data_model = 'Old Data Model'*/


WITH flash_sale_tag_list AS (
    SELECT
        sst.se_sale_id,
        ssa.data_model,
        COALESCE(st.territory_id, TRY_TO_NUMBER(ssa.posa_territory_id)) AS posa_territory_id,
        COALESCE(t.name, ssa.posa_territory)                            AS posa_territory,
        ssa.salesforce_opportunity_id,
        sst.tag_name,
        ssa.sale_name,
        ssa.company_name
    FROM se.data.se_sale_tags sst
        INNER JOIN data_vault_mvp.dwh.se_sale AS ssa ON sst.se_sale_id = ssa.se_sale_id
        LEFT JOIN  hygiene_snapshot_vault_mvp.cms_mysql.sale_territory st
                   ON TRY_TO_NUMBER(sst.se_sale_id) = st.sale_id
                       AND ssa.data_model = 'Old Data Model'
        LEFT JOIN  hygiene_snapshot_vault_mvp.cms_mysql.territory t ON st.territory_id = t.id
    WHERE sst.tag_name LIKE 'zz_flash%'
      AND ssa.sale_active
),
     flash_sales AS (
         --list of sales with zz_flash tag and status on specific territories or all territories
         SELECT
             fstl.se_sale_id,
             fstl.posa_territory_id,
             fstl.posa_territory,
             fstl.salesforce_opportunity_id,
             fstl.sale_name,
             fstl.company_name,
             IFF(MAX(IFF(fstl.tag_name LIKE 'zz_flash_%', 1, 0)) > 0, 'specific territories', 'all territories') AS flash_territory_status,
             LISTAGG(DISTINCT fstl.tag_name, ', ') WITHIN GROUP ( ORDER BY fstl.tag_name )                       AS flash_tag_list
         FROM flash_sale_tag_list fstl
         GROUP BY 1, 2, 3, 4, 5, 6
     ),
     territory_specific_sales AS (
         -- logic to only return territories that match the territory specific tag
         SELECT
             fs.se_sale_id,
             fs.flash_territory_status,
             fs.flash_tag_list,
             fs.salesforce_opportunity_id,
             s.tag_name,
             fs.sale_name,
             fs.company_name,
             fs.posa_territory_id,
             fs.posa_territory,
             UPPER(SPLIT_PART(s.tag_name, '_', -1)) AS flash_territory
         FROM flash_sales fs
             INNER JOIN data_vault_mvp.dwh.se_sale_tags s ON fs.se_sale_id = s.se_sale_id
         WHERE fs.flash_territory_status = 'specific territories'
           AND s.tag_name LIKE 'zz_flash_%'
           -- filter to only return territory sales that have a posa matching the suffix of the territory specific tag
           AND flash_territory = fs.posa_territory
     ),
     stack AS (

         -- stack sales with territory specific flags on top of sales without any

         -- sales that don't have a territory specific flash sale tag
         SELECT
             fs.se_sale_id,
             fs.posa_territory_id,
             fs.posa_territory,
             fs.sale_name,
             fs.company_name,
             fs.flash_territory_status,
             fs.flash_tag_list
         FROM flash_sales fs
         WHERE fs.flash_territory_status = 'all territories'

         UNION ALL

         -- sales that have a territory specific flash sale tag
         SELECT
             tss.se_sale_id,
             tss.posa_territory_id,
             tss.posa_territory,
             tss.sale_name,
             tss.company_name,
             tss.flash_territory_status,
             tss.flash_tag_list
         FROM territory_specific_sales tss
     ),
     tag_modelling AS (
         -- sales can go in and out of flash mode, we want to compute the most recent initialisation date
         -- this cte computes all initialisations, following cte will return the most recent
         SELECT
             sd.se_sale_id,
             sd.view_date,
             sd.has_flash_tag,
             LAG(sd.has_flash_tag) OVER (PARTITION BY se_sale_id ORDER BY sd.view_date) AS last_has_flash_tag,
             sd.has_flash_tag AND COALESCE(last_has_flash_tag, FALSE) = FALSE           AS flash_sale_initiated
         FROM data_vault_mvp.dwh.se_sale_tags_snapshot sd
     ),
     tag_initialisation AS (
         -- aggregate to sale level to output most recent initialisation date
         SELECT
             tm.se_sale_id,
             MAX(tm.view_date) AS tag_inititated_date
         FROM tag_modelling tm
         WHERE tm.flash_sale_initiated
         GROUP BY 1
     )
SELECT
    s.se_sale_id,
    s.flash_territory_status,
    s.flash_tag_list,
    s.posa_territory_id,
    s.posa_territory,
    s.sale_name,
    s.company_name,
    ti.tag_inititated_date
FROM stack s
    LEFT JOIN tag_initialisation ti ON s.se_sale_id = ti.se_sale_id
-- we only want to return sales that are in their first week of being initialised
WHERE DATE_TRUNC(
              WEEK, ti.tag_inititated_date
          ) + 7 = DATE_TRUNC(WEEK, CURRENT_DATE)
;

SELECT *
FROM dbt_dev.dbt_robinpatel_data_platform.dp_athena_new_flash_deal_category;


SELECT *
FROM dbt.bi_data_platform.dp_athena_new_flash_deal_category;


------------------------------------------------------------------------------------------------------------------------


WITH model_data AS (
    SELECT
        date,
        se_sale_id,
        has_flash_tag,
        LAG(has_flash_tag) OVER (PARTITION BY se_sale_id ORDER BY date)            AS previous_has_flag,
        IFF(has_flash_tag AND previous_has_flag IS DISTINCT FROM TRUE, date, NULL) AS flag
    FROM data_vault_mvp.bi.fact_sale_metrics
    WHERE se_sale_id = 'A22682'
      AND date >= CURRENT_DATE - 30
),
     persist_start_date AS (
         SELECT
             date,
             se_sale_id,
             has_flash_tag,
             MAX(flag)
                 OVER (PARTITION BY se_sale_id, has_flash_tag ORDER BY date ROWS UNBOUNDED PRECEDING) AS flash_start_date_persisted
         FROM model_data
     )
SELECT
    date,
    se_sale_id,
    has_flash_tag,
    IFF(has_flash_tag, ROW_NUMBER() OVER (PARTITION BY se_sale_id, has_flash_tag, flash_start_date_persisted ORDER BY date), NULL) AS flash_live_cumaltive
FROM persist_start_date;



WITH input_data AS (

    SELECT
        column1 AS date,
        column2 AS se_sale_id,
        column3 AS has_flash_tag,
        column4 AS sale_active
    FROM
    VALUES ('2023-01-03', 'A22682', FALSE, 1),
           ('2023-01-04', 'A22682', FALSE, 1),
           ('2023-01-05', 'A22682', TRUE, 1),
           ('2023-01-06', 'A22682', TRUE, 1),
           ('2023-01-07', 'A22682', TRUE, 1),
           ('2023-01-08', 'A22682', TRUE, 0),
           ('2023-01-09', 'A22682', TRUE, 1),
           ('2023-01-10', 'A22682', FALSE, 1),
           ('2023-01-11', 'A22682', FALSE, 1),
           ('2023-01-12', 'A22682', FALSE, 0),
           ('2023-01-13', 'A22682', FALSE, 1),
           ('2023-01-14', 'A22682', FALSE, 1),
           ('2023-01-15', 'A22682', FALSE, 0),
           ('2023-01-16', 'A22682', TRUE, 1),
           ('2023-01-17', 'A22682', TRUE, 1),
           ('2023-01-18', 'A22682', FALSE, 1),
           ('2023-01-19', 'A22682', TRUE, 1),
           ('2023-01-20', 'A22682', TRUE, 1),
           ('2023-01-21', 'A22682', TRUE, 1),
           ('2023-01-22', 'A22682', TRUE, 0),
           ('2023-01-23', 'A22682', FALSE, 1),
           ('2023-01-24', 'A22682', TRUE, 1),
           ('2023-01-25', 'A22682', TRUE, 1),
           ('2023-01-26', 'A22682', TRUE, 1),
           ('2023-01-27', 'A22682', FALSE, 1),
           ('2023-01-28', 'A22682', FALSE, 1),
           ('2023-01-29', 'A22682', TRUE, 1),
           ('2023-01-30', 'A22682', TRUE, 1),
           ('2023-01-31', 'A22682', TRUE, 1),
           ('2023-02-01', 'A22682', FALSE, 1),
           ('2023-02-02', 'A22682', FALSE, 1)
),
     model_data AS (
         SELECT
             date,
             se_sale_id,
             has_flash_tag,
             sale_active,
             has_flash_tag AND sale_active = 1 previous_has_flag,
             LAG(in_flash_mode) OVER (PARTITION BY se_sale_id ORDER BY date)            AS previous_has_flag,
             IFF(in_flash_mode AND previous_has_flag IS DISTINCT FROM TRUE, date, NULL) AS flag
         FROM input_data
         WHERE se_sale_id = 'A22682'
           AND date >= CURRENT_DATE - 30
     ),
     persist_start_date AS (
         SELECT
             date,
             se_sale_id,
             has_flash_tag,
             sale_active,
             in_flash_mode,
             MAX(flag)
                 OVER (PARTITION BY se_sale_id, previous_has_flag ORDER BY date ROWS UNBOUNDED PRECEDING) AS flash_start_date_persisted
         FROM model_data
     )
SELECT
    date,
    se_sale_id,
    has_flash_tag,
    sale_active,
    in_flash_mode,
    IFF(in_flash_mode, ROW_NUMBER() OVER (PARTITION BY se_sale_id, has_flash_tag, flash_start_date_persisted ORDER BY date), NULL) AS flash_live_cumaltive
FROM persist_start_date;



SELECT
    date,
    se_sale_id,
    has_flash_tag
FROM data_vault_mvp.bi.fact_sale_metrics
WHERE se_sale_id = 'A22682'
  AND date >= CURRENT_DATE - 30



