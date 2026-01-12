WITH sale_table AS (SELECT ds.se_sale_id,
                           ds.salesforce_opportunity_id,
                           ds.sale_start_date,
                           LAST_VALUE(
                                   ssa.company_id)
                                   OVER (PARTITION BY ds.salesforce_opportunity_id ORDER BY ds.sale_start_date ASC) AS company_id,
                           LAST_VALUE(
                                   COALESCE(ssa.company_name, 'Not in Salesforce'))
                                   OVER (PARTITION BY ds.salesforce_opportunity_id ORDER BY ds.sale_start_date ASC) AS company_name,
                           LAST_VALUE(
                                   COALESCE(ssa.current_contractor_name, 'Not in Salesforce'))
                                   OVER (PARTITION BY ds.salesforce_opportunity_id ORDER BY ds.sale_start_date ASC) AS current_contractor_name,
                           ds.posu_cluster,--uses se.bi.dim_sale as source table rather than sale attributes
                           ds.posu_cluster_region,--uses se.bi.dim_sale as source table rather than sale attributes
                           LAST_VALUE(
                                   ds.posu_country)
                                   OVER (PARTITION BY ds.salesforce_opportunity_id ORDER BY ds.sale_start_date ASC) AS posu_country,
                           LAST_VALUE(
                                   ds.posu_cluster_sub_region)
                                   OVER (PARTITION BY ds.salesforce_opportunity_id ORDER BY ds.sale_start_date ASC) AS posu_cluster_sub_region,
                           --ds.posu_cluster_sub_region,--use se.bi.dim_sale as source table rather than sale attributes
                           --ds.posu_country,--uses se.bi.dim_sale as source table rather than sale attributes
                           ds.posa_territory,
                           ds.product_type,--use se.bi.dim_sale as source table rather than sale attributes
                           ds.product_configuration--use se.bi.dim_sale as source table rather than sale attributes
                    FROM se.data.dim_sale ds
                             LEFT JOIN se.data.se_sale_attributes ssa ON ds.se_sale_id = ssa.se_sale_id
                             LEFT JOIN se.data.se_company_attributes sca
                                       ON ssa.company_id = sca.company_id::VARCHAR
                             LEFT JOIN se.data.global_sale_attributes gsa --SELECT * FROM se.data.dim_sale; SELECT * FROM se.data.se_company_attributes
                                       ON ds.salesforce_opportunity_id = gsa.global_sale_id
                    ),
     margin
         AS (SELECT ds.salesforce_opportunity_id,
                    ds.posa_territory,
                    sum(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp_lifetime,
                    SUM(IFF(
                                DATE_TRUNC('YEAR', booking_completed_date) =
                                '2019-01-01',
                                fcb.margin_gross_of_toms_gbp_constant_currency,
                                0))                                     AS margin_gbp_2019,
                    SUM(IFF(
                                DATE_TRUNC('YEAR', booking_completed_date) =
                                '2020-01-01',
                                fcb.margin_gross_of_toms_gbp_constant_currency,
                                0))                                     AS margin_gbp_2020,
                    SUM(IFF(
                                DATE_TRUNC('YEAR', booking_completed_date) =
                                '2021-01-01',
                                fcb.margin_gross_of_toms_gbp_constant_currency,
                                0))                                     AS margin_gbp_2021,
                    SUM(IFF(
                                DATE_TRUNC('YEAR', booking_completed_date) =
                                '2022-01-01',
                                fcb.margin_gross_of_toms_gbp_constant_currency,
                                0))                                     AS margin_gbp_2022,
                    SUM(IFF(
                                DATE_TRUNC('YEAR', booking_completed_date) =
                                '2023-01-01',
                                fcb.margin_gross_of_toms_gbp_constant_currency,
                                0))                                     AS margin_gbp_2023
             FROM se.data.fact_complete_booking fcb
                      LEFT JOIN se.bi.dim_sale_territory ds
                                ON fcb.se_sale_id = ds.se_sale_id AND fcb.territory = ds.posa_territory
             GROUP BY 1, 2),
     margin_global
         AS (SELECT ds.salesforce_opportunity_id,
                    sum(fcb.margin_gross_of_toms_gbp_constant_currency) AS margin_gbp_lifetime,
                    SUM(IFF(
                                DATE_TRUNC('YEAR', booking_completed_date) =
                                '2019-01-01',
                                fcb.margin_gross_of_toms_gbp_constant_currency,
                                0))                                     AS margin_gbp_2019,
                    SUM(IFF(
                                DATE_TRUNC('YEAR', booking_completed_date) =
                                '2020-01-01',
                                fcb.margin_gross_of_toms_gbp_constant_currency,
                                0))                                     AS margin_gbp_2020,
                    SUM(IFF(
                                DATE_TRUNC('YEAR', booking_completed_date) =
                                '2021-01-01',
                                fcb.margin_gross_of_toms_gbp_constant_currency,
                                0))                                     AS margin_gbp_2021,
                    SUM(IFF(
                                DATE_TRUNC('YEAR', booking_completed_date) =
                                '2022-01-01',
                                fcb.margin_gross_of_toms_gbp_constant_currency,
                                0))                                     AS margin_gbp_2022,
                    SUM(IFF(
                                DATE_TRUNC('YEAR', booking_completed_date) =
                                '2023-01-01',
                                fcb.margin_gross_of_toms_gbp_constant_currency,
                                0))                                     AS margin_gbp_2023
             FROM se.data.fact_complete_booking fcb
                      LEFT JOIN se.bi.dim_sale_territory ds
                                ON fcb.se_sale_id = ds.se_sale_id AND fcb.territory = ds.posa_territory
             GROUP BY 1),
     sale_active
         AS (SELECT ds.salesforce_opportunity_id,
                    ds.company_id,
                    COALESCE(ds.company_name, 'Not in Salesforce')            AS company_name,--CHANGED coalesce include travelbird
                    COALESCE(ds.current_contractor_name, 'Not in Salesforce') AS current_contractor_name,--CHANGED coalesce include travelbird
                    ds.posu_cluster,--uses se.bi.dim_sale as source table rather than sale attributes
                    ds.posu_cluster_region,--uses se.bi.dim_sale as source table rather than sale attributes
                    ds.posu_cluster_sub_region,--use se.bi.dim_sale as source table rather than sale attributes
                    ds.posu_country,--uses se.bi.dim_sale as source table rather than sale attributes
                    ds.posa_territory,
                    sca.segment_2019                                          AS forecast_segment,--To be left NULL until logic for TravelBird deals is determined
                    gsa.deal_segment                                          AS current_segment, --To be left NULL until logic for TravelBird deals is determined
                    sa.view_date,
                    ds.product_type,--use se.bi.dim_sale as source table rather than sale attributes
                    ds.product_configuration,--use se.bi.dim_sale as source table rather than sale attributes
                    COALESCE(ssa.pulled_type, 'No Reason Given')              AS pulled_type,--COALESCE confirmed by Christie J in CIP
                    COALESCE(ssa.pulled_reason, 'No Reason Given')            AS pulled_reason,--COALESCE confirmed by Christie J in CIP
                    COALESCE(margin.margin_gbp_lifetime, 0)                   AS margin_gbp_lifetime,
                    COALESCE(margin.margin_gbp_2019, 0)                       AS margin_gbp_2019,
                    COALESCE(margin.margin_gbp_2020, 0)                       AS margin_gbp_2020,
                    COALESCE(margin.margin_gbp_2021, 0)                       AS margin_gbp_2021,
                    COALESCE(margin.margin_gbp_2022, 0)                       AS margin_gbp_2022,
                    COALESCE(margin.margin_gbp_2023, 0)                       AS margin_gbp_2023,
                    max(CASE
                            WHEN lower(COALESCE(ssa.target_account_list,'')) LIKE '%hunting%'
                                THEN 1
                            ELSE 0 END)                                       AS hunting_list
             FROM sale_table ds
                      LEFT JOIN se.data.sale_active sa ON ds.se_sale_id = sa.se_sale_id
                      LEFT JOIN se.data.se_sale_attributes ssa ON ds.se_sale_id = ssa.se_sale_id
                      LEFT JOIN se.data.se_company_attributes sca
                                ON ds.company_id = sca.company_id::VARCHAR
                      LEFT JOIN se.data.global_sale_attributes gsa --SELECT * FROM se.data.dim_sale; SELECT * FROM se.data.se_company_attributes
                                ON ds.salesforce_opportunity_id = gsa.global_sale_id
                      LEFT JOIN margin margin
                                ON margin.salesforce_opportunity_id =
                                   ds.salesforce_opportunity_id AND
                                   margin.posa_territory =
                                   ds.posa_territory
             WHERE sa.active = TRUE
             GROUP BY 1, 2, 3, 4, 5, 6, 7, 8,
                      9, 10, 11, 12, 13, 14,
                      15, 16, 17, 18, 19, 20,
                      21, 22),
     sale_active_global
         AS (SELECT ds.salesforce_opportunity_id,
                    ds.company_id,
                    COALESCE(ds.company_name, 'Not in Salesforce')            AS company_name,--CHANGED coalesce include travelbird
                    COALESCE(ds.current_contractor_name, 'Not in Salesforce') AS current_contractor_name,--CHANGED coalesce include travelbird
                    ds.posu_cluster,--uses se.bi.dim_sale as source table rather than sale attributes
                    ds.posu_cluster_region,--uses se.bi.dim_sale as source table rather than sale attributes
                    ds.posu_cluster_sub_region,--use se.bi.dim_sale as source table rather than sale attributes
                    ds.posu_country,--uses se.bi.dim_sale as source table rather than sale attributes
                    'Total'                                                   AS posa_territory,
                    sca.segment_lly                                           AS forecast_segment,--To be left NULL until logic for TravelBird deals is determined
                    gsa.deal_segment                                          AS current_segment, --To be left NULL until logic for TravelBird deals is determined
                    sa.view_date,
                    ds.product_type,--use se.bi.dim_sale as source table rather than sale attributes
                    ds.product_configuration,--use se.bi.dim_sale as source table rather than sale attributes
                    COALESCE(ssa.pulled_type, 'No Reason Given')              AS pulled_type,--COALESCE confirmed by Christie J in CIP
                    COALESCE(ssa.pulled_reason, 'No Reason Given')            AS pulled_reason,--COALESCE confirmed by Christie J in CIP
                    COALESCE(margin_global.margin_gbp_lifetime, 0)            AS margin_gbp_lifetime,
                    COALESCE(margin_global.margin_gbp_2019, 0)                AS margin_gbp_2019,
                    COALESCE(margin_global.margin_gbp_2020, 0)                AS margin_gbp_2020,
                    COALESCE(margin_global.margin_gbp_2021, 0)                AS margin_gbp_2021,
                    COALESCE(margin_global.margin_gbp_2022, 0)                AS margin_gbp_2022,
                    COALESCE(margin_global.margin_gbp_2023, 0)                AS margin_gbp_2023,
                    max(CASE
                            WHEN lower(COALESCE(ssa.target_account_list,'')) LIKE '%hunting%'
                                THEN 1
                            ELSE 0 END)                                       AS hunting_list
             FROM sale_table ds
                      LEFT JOIN se.data.sale_active sa ON ds.se_sale_id = sa.se_sale_id
                      LEFT JOIN se.data.se_sale_attributes ssa ON ds.se_sale_id = ssa.se_sale_id
                      LEFT JOIN se.data.se_company_attributes sca
                                ON ds.company_id = sca.company_id::VARCHAR
                      LEFT JOIN se.data.global_sale_attributes gsa --SELECT * FROM se.data.dim_sale; SELECT * FROM se.data.se_company_attributes
                                ON ds.salesforce_opportunity_id = gsa.global_sale_id
                      LEFT JOIN margin_global margin_global
                                ON margin_global.salesforce_opportunity_id = ds.salesforce_opportunity_id
             WHERE sa.active = TRUE
             GROUP BY 1, 2, 3, 4, 5, 6, 7, 8,
                      9, 10, 11, 12, 13, 14,
                      15, 16, 17, 18, 19, 20,
                      21, 22),
     calendar AS
         (SELECT date_value AS date,
                 week_start
          FROM se.data.se_calendar cal
          WHERE date_value BETWEEN '2020-06-01' AND current_date),
     combine_sale_cal
         AS (SELECT cal.week_start,
                    cal.date,
                    sale_active.salesforce_opportunity_id,
                    sale_active.company_id,
                    sale_active.company_name,
                    sale_active.current_contractor_name,
                    sale_active.posu_cluster,
                    sale_active.posu_cluster_region,
                    sale_active.posu_cluster_sub_region,
                    sale_active.posu_country,
                    sale_active.posa_territory,
                    sale_active.forecast_segment,
                    sale_active.current_segment,
                    sale_active.product_type,
                    sale_active.product_configuration,
                    sale_active.pulled_type,
                    sale_active.pulled_reason,
                    sale_active.margin_gbp_lifetime,
                    sale_active.margin_gbp_2019,
                    sale_active.margin_gbp_2020,
                    sale_active.margin_gbp_2021,
                    sale_active.margin_gbp_2022,
                    sale_active.margin_gbp_2023,
                    sale_active.hunting_list,
                    max(CASE WHEN cal.date = sale_active.view_date THEN 1 ELSE 0 END) AS active
             FROM sale_active,
                  calendar cal
             GROUP BY 1, 2, 3, 4, 5, 6, 7, 8,
                      9, 10, 11, 12, 13, 14,
                      15, 16, 17, 18, 19, 20,
                      21, 22, 23, 24),
     combine_sale_cal_global
         AS (SELECT cal.week_start,
                    cal.date,
                    sale_active_global.salesforce_opportunity_id,
                    sale_active_global.company_id,
                    sale_active_global.company_name,
                    sale_active_global.current_contractor_name,
                    sale_active_global.posu_cluster,
                    sale_active_global.posu_cluster_region,
                    sale_active_global.posu_cluster_sub_region,
                    sale_active_global.posu_country,
                    sale_active_global.posa_territory,
                    sale_active_global.forecast_segment,
                    sale_active_global.current_segment,
                    sale_active_global.product_type,
                    sale_active_global.product_configuration,
                    sale_active_global.pulled_type,
                    sale_active_global.pulled_reason,
                    sale_active_global.margin_gbp_lifetime,
                    sale_active_global.margin_gbp_2019,
                    sale_active_global.margin_gbp_2020,
                    sale_active_global.margin_gbp_2021,
                    sale_active_global.margin_gbp_2022,
                    sale_active_global.margin_gbp_2023,
                    sale_active_global.hunting_list,
                    max(CASE WHEN cal.date = sale_active_global.view_date THEN 1 ELSE 0 END) AS active
             FROM sale_active_global,
                  calendar cal
             GROUP BY 1, 2, 3, 4, 5, 6, 7, 8,
                      9, 10, 11, 12, 13, 14,
                      15, 16, 17, 18, 19, 20,
                      21, 22, 23, 24),
     week_agg AS
         (SELECT week_start,
                 date,
                 salesforce_opportunity_id,
                 company_id,
                 company_name,
                 current_contractor_name,
                 active,
                 posu_cluster,
                 posu_cluster_region,
                 posu_cluster_sub_region,
                 posu_country,
                 posa_territory,
                 forecast_segment,
                 current_segment,
                 margin_gbp_lifetime,
                 margin_gbp_2019,
                 margin_gbp_2020,
                 margin_gbp_2021,
                 margin_gbp_2022,
                 margin_gbp_2023,
                 hunting_list,
                 product_type,
                 product_configuration,
                 pulled_type,
                 pulled_reason,
                 CASE WHEN week_start = date AND active = 1 THEN 1 ELSE 0 END                                                                AS active_start_of_week,
                 CASE WHEN week_start + 6 = date AND active = 1 THEN 1 ELSE 0 END                                                            AS active_end_of_week,
                 lag(CASE
                         WHEN active = 1
                             THEN date END, 1)
                     IGNORE NULLS OVER (PARTITION BY salesforce_opportunity_id, company_name, product_type,posa_territory ORDER BY date ASC) AS previous_active_date,
                 min(CASE
                         WHEN active = 1
                             THEN date END)
                     OVER (PARTITION BY salesforce_opportunity_id, company_name, product_type,posa_territory)                                AS first_active_date
          FROM combine_sale_cal),
     week_agg_global AS
         (SELECT week_start,
                 date,
                 salesforce_opportunity_id,
                 company_id,
                 company_name,
                 current_contractor_name,
                 active,
                 posu_cluster,
                 posu_cluster_region,
                 posu_cluster_sub_region,
                 posu_country,
                 posa_territory,
                 forecast_segment,
                 current_segment,
                 margin_gbp_lifetime,
                 margin_gbp_2019,
                 margin_gbp_2020,
                 margin_gbp_2021,
                 margin_gbp_2022,
                 margin_gbp_2023,
                 hunting_list,
                 product_type,
                 product_configuration,
                 pulled_type,
                 pulled_reason,
                 CASE WHEN week_start = date AND active = 1 THEN 1 ELSE 0 END                                                 AS active_start_of_week,
                 CASE WHEN week_start + 6 = date AND active = 1 THEN 1 ELSE 0 END                                             AS active_end_of_week,
                 lag(CASE
                         WHEN active = 1
                             THEN date END, 1)
                     IGNORE NULLS OVER (PARTITION BY salesforce_opportunity_id, company_name, product_type ORDER BY date ASC) AS previous_active_date,
                 min(CASE
                         WHEN active = 1
                             THEN date END)
                     OVER (PARTITION BY salesforce_opportunity_id, company_name, product_type)                                AS first_active_date
          FROM combine_sale_cal_global),
     aggregating AS (SELECT week_start,
                            salesforce_opportunity_id,
                            company_id,
                            company_name,
                            current_contractor_name,
                            posu_cluster,
                            posu_cluster_region,
                            posu_cluster_sub_region,
                            posu_country,
                            posa_territory,
                            forecast_segment,
                            current_segment,
                            hunting_list,
                            product_type,
                            product_configuration,
                            pulled_type,
                            pulled_reason,
                            margin_gbp_lifetime,
                            margin_gbp_2019,
                            margin_gbp_2020,
                            margin_gbp_2021,
                            margin_gbp_2022,
                            margin_gbp_2023,
                            max(active_start_of_week) AS active_start_of_week,
                            max(active_end_of_week)   AS active_end_of_week,
                            max(active)               AS active_in_week,
                            MIN(previous_active_date) AS previous_active_date,
                            min(first_active_date)    AS first_active_date
                     FROM week_agg
                     GROUP BY 1, 2, 3, 4, 5,
                              6, 7, 8, 9, 10,
                              11, 12, 13, 14,
                              15, 16, 17, 18,
                              19, 20, 21, 22,
                              23),
     aggregating_global
         AS (SELECT DISTINCT week_start,
                             salesforce_opportunity_id,
                             company_id,
                             company_name,
                             current_contractor_name,
                             posu_cluster,
                             posu_cluster_region,
                             posu_cluster_sub_region,
                             posu_country,
                             posa_territory,
                             forecast_segment,
                             current_segment,
                             hunting_list,
                             product_type,
                             product_configuration,
                             pulled_type,
                             pulled_reason,
                             margin_gbp_lifetime,
                             margin_gbp_2019,
                             margin_gbp_2020,
                             margin_gbp_2021,
                             margin_gbp_2022,
                             margin_gbp_2023,

                             max(
                                     active_start_of_week)
                                     over (partition by salesforce_opportunity_id,week_start)     AS active_start_of_week,
                             max(
                                     active_end_of_week)
                                     over (partition by salesforce_opportunity_id,week_start)     AS active_end_of_week,
                             max(active) over (partition by salesforce_opportunity_id,week_start) AS active_in_week,
                             min(
                                     previous_active_date)
                                     over (partition by salesforce_opportunity_id,week_start)     AS previous_active_date,
                             min(
                                     first_active_date)
                                     over (partition by salesforce_opportunity_id,week_start)     AS first_active_date
             FROM week_agg_global),
     master_table_posa AS (SELECT week_start,
                                  salesforce_opportunity_id,
                                  company_id,
                                  company_name,
                                  current_contractor_name,
                                  product_type,
                                  product_configuration,
                                  pulled_type,
                                  pulled_reason,
                                  hunting_list,
                                  margin_gbp_lifetime,
                                  margin_gbp_2019,
                                  margin_gbp_2020,
                                  margin_gbp_2021,
                                  margin_gbp_2022,
                                  margin_gbp_2023,
                                  posu_cluster,
                                  posu_cluster_region,
                                  posu_cluster_sub_region,
                                  se.data.cm_region_from_cluster_and_cluster_region(
                                          posu_cluster,
                                          posu_cluster_region)                                      AS cm_region,
                                  posu_country,
                                  posa_territory,
                                  forecast_segment,
                                  current_segment,
                                  CASE
                                      WHEN first_active_date BETWEEN week_start AND week_start + 6
                                          THEN 1
                                      ELSE 0 END                                                    AS new,
                                  CASE
                                      WHEN first_active_date <
                                           week_start AND
                                           previous_active_date <>
                                           week_start -
                                           1 AND
                                           active_in_week =
                                           1
                                          THEN 1
                                      ELSE 0 END                                                    AS reactivated,
                                  CASE
                                      WHEN (active_in_week = 1 OR previous_active_date = week_start - 1) AND
                                           active_end_of_week =
                                           0
                                          THEN 1
                                      ELSE 0 END                                                    AS pulled,
                                  active_end_of_week,
                                  CASE WHEN previous_active_date = week_start - 1 THEN 1 ELSE 0 END AS active_end_of_prev_week,
                                  CASE
                                      WHEN product_type IN ('Hotel', 'HotelPlus')
                                          THEN posu_cluster
                                      ELSE 'All' END                                                AS dimension_1,
                                  CASE
                                      WHEN product_type IN ('Hotel', 'HotelPlus')
                                          THEN 'All'
                                      WHEN posa_territory IN ('DE', 'CH')
                                          THEN 'DACH'
                                      WHEN posa_territory = 'UK'
                                          THEN 'UK'
                                      ELSE 'ROW' END                                                AS dimension_3
                           FROM aggregating),
     dimension_3_lu
         AS (SELECT DISTINCT salesforce_opportunity_id,
                             week_start,
                             CASE
                                 WHEN product_type IN ('Hotel', 'HotelPlus')
                                     THEN 'All'
                                 --Stop over counting of packages as blowout occured. This takes the first posa active posa
                                 WHEN FIRST_VALUE(
                                              posa_territory)
                                              OVER (partition by week_start,salesforce_opportunity_id ORDER BY active_end_of_prev_week DESC) IN
                                      ('DE',
                                       'CH')
                                     THEN 'DACH'
                                 WHEN FIRST_VALUE(
                                              posa_territory)
                                              OVER (partition by week_start,salesforce_opportunity_id ORDER BY active_end_of_prev_week DESC) =
                                      'UK'
                                     THEN 'UK'
                                 ELSE 'ROW' END AS dimension_3
             FROM master_table_posa),
     master_table_global
         AS (SELECT agg_global.week_start,
                    agg_global.salesforce_opportunity_id,
                    agg_global.company_id,
                    agg_global.company_name,
                    agg_global.current_contractor_name,
                    agg_global.product_type,
                    product_configuration,
                    agg_global.pulled_type,
                    agg_global.pulled_reason,
                    agg_global.hunting_list,
                    agg_global.margin_gbp_lifetime AS margin_gbp_lifetime,
                    agg_global.margin_gbp_2019,
                    agg_global.margin_gbp_2020,
                    agg_global.margin_gbp_2021,
                    agg_global.margin_gbp_2022,
                    agg_global.margin_gbp_2023,
                    agg_global.posu_cluster,
                    agg_global.posu_cluster_region,
                    agg_global.posu_cluster_sub_region,
                    se.data.cm_region_from_cluster_and_cluster_region(
                            posu_cluster,
                            posu_cluster_region)   AS cm_region,
                    agg_global.posu_country,
                    'Total'                        AS posa_territory,
                    agg_global.forecast_segment,
                    agg_global.current_segment,
                    CASE
                        WHEN first_active_date BETWEEN agg_global.week_start AND agg_global.week_start + 6
                            THEN 1
                        ELSE 0 END                 AS new,
                    CASE
                        WHEN first_active_date <
                             agg_global.week_start AND
                             previous_active_date <>
                             agg_global.week_start -
                             1 AND
                             active_in_week =
                             1 THEN 1
                        ELSE 0 END                 AS reactivated,
                    CASE
                        WHEN (active_in_week = 1 OR previous_active_date = agg_global.week_start - 1) AND
                             active_end_of_week =
                             0 THEN 1
                        ELSE 0 END                 AS pulled,
                    active_end_of_week,
                    CASE
                        WHEN previous_active_date = agg_global.week_start - 1
                            THEN 1
                        ELSE 0 END                 AS active_end_of_prev_week,


                    CASE
                        WHEN product_type IN ('Hotel', 'HotelPlus')
                            THEN posu_cluster
                        ELSE 'All' END             AS dimension_1,
                    dim_lu.dimension_3
             FROM aggregating_global agg_global
                      LEFT JOIN dimension_3_lu dim_lu
                                ON agg_global.salesforce_opportunity_id =
                                   dim_lu.salesforce_opportunity_id
                                    AND
                                   agg_global.week_start =
                                   dim_lu.week_start),
     master_table AS (SELECT *
                      FROM master_table_posa
                      UNION
                      SELECT week_start,
                             salesforce_opportunity_id,
                             company_id,
                             company_name,
                             current_contractor_name,
                             product_type,
                             listagg(DISTINCT
                                     product_configuration,
                                     '/ ')
                                     WITHIN GROUP ( ORDER BY product_configuration ) AS product_configuration,
                             pulled_type,
                             pulled_reason,
                             hunting_list,
                             margin_gbp_lifetime,
                             margin_gbp_2019,
                             margin_gbp_2020,
                             margin_gbp_2021,
                             margin_gbp_2022,
                             margin_gbp_2023,
                             posu_cluster,
                             posu_cluster_region,
                             posu_cluster_sub_region,
                             cm_region,
                             posu_country,
                             posa_territory,
                             forecast_segment,
                             current_segment,
                             new,
                             reactivated,
                             pulled,
                             active_end_of_week,
                             active_end_of_prev_week,
                             dimension_1,
                             dimension_3
                      FROM master_table_global
                      GROUP BY 1, 2, 3, 4, 5,
                               6, 8, 9, 10,
                               11, 12, 13, 14,
                               15, 16, 17, 18,
                               19, 20, 21, 22,
                               23, 24, 25, 26,
                               27, 28, 29, 30,
                               31),
     targets AS
         (SELECT target_date,
                 target_name,
                 dimension_1,
                 dimension_2,
                 dimension_3,
                 dimension_4,
                 dimension_5,
                 target_value
          FROM hygiene_snapshot_vault_mvp.fpa_gsheets.generic_targets targets
          WHERE target_name = 'Net_Adds_Target'),
     first_company_id AS
         (SELECT salesforce_opportunity_id,
                 TRIM(company_id.value) AS first_company_id
          FROM data_vault_mvp.dwh.se_sale,
               LATERAL split_to_table(IFNULL(company_id, ''), '|') AS company_id
          QUALIFY ROW_NUMBER() OVER (
              PARTITION BY salesforce_opportunity_id
              ORDER BY company_id ASC
              ) = 1)
SELECT DISTINCT mt.week_start,
                mt.salesforce_opportunity_id,
                fc.first_company_id,
                mt.company_id,
                mt.company_name,
                mt.current_contractor_name,
                mt.product_type,
                mt.product_configuration,
                mt.pulled_type,
                mt.pulled_reason,
                mt.hunting_list,
                mt.margin_gbp_lifetime,
                mt.margin_gbp_2019,
                mt.margin_gbp_2020,
                mt.margin_gbp_2021,
                mt.margin_gbp_2022,
                mt.margin_gbp_2023,
                mt.posu_cluster,
                mt.posu_cluster_region,
                mt.posu_cluster_sub_region,
                mt.cm_region,
                mt.posu_country,
                mt.posa_territory,
                mt.forecast_segment,
                mt.current_segment,
                mt.new,
                mt.reactivated,
                mt.pulled,
                mt.active_end_of_week,
                mt.active_end_of_prev_week,
                tg.target_value       AS net_adds_target,
                tg.target_value * 1.2 AS gross_adds_target,
                mt.dimension_1,
                mt.dimension_3
FROM master_table mt
         LEFT JOIN targets tg
                   ON
                               mt.week_start =
                               tg.target_date
                           AND
                               mt.dimension_1 =
                               tg.dimension_1
                           AND
                               mt.product_type =
                               tg.dimension_2
                           AND
                               mt.dimension_3 =
                               tg.dimension_3
         LEFT JOIN first_company_id fc
                   ON fc.salesforce_opportunity_id = mt.salesforce_opportunity_id