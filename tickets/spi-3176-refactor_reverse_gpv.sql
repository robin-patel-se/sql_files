WITH margin_table AS (
    SELECT
        st.id                             AS territory_id,
        fcb.se_sale_id,
        SUM(fcb.margin_gross_of_toms_gbp) AS margin_gbp
    FROM se.data.fact_complete_booking fcb
        INNER JOIN latest_vault.cms_mysql.territory st
                   ON fcb.territory = st.name
    WHERE fcb.booking_completed_date >= CURRENT_DATE - 7
    GROUP BY 1, 2
),
     total_spv_events AS (
         SELECT
             ude.deal_id,
             ude.territory_id,
             COUNT(*) AS spv
         FROM data_science.predictive_modeling.user_deal_events ude
         WHERE ude.evt_date >= CURRENT_DATE - 7
           AND ude.evt_name = 'deal-view'
         GROUP BY 1, 2
     ),
     max_gpv AS (
         SELECT
             COALESCE(mt.territory_id, tse.territory_id) AS territory_id,
             MAX(mt.margin_gbp / tse.spv)                AS max_gpv_per_territory
         FROM margin_table mt
             INNER JOIN total_spv_events tse
                        ON mt.territory_id = tse.territory_id
                            AND mt.se_sale_id = tse.deal_id
         WHERE tse.spv >= 500
         GROUP BY 1
     )
SELECT
    st.id                                AS territory_id,
    ds.se_sale_id,
    mt.margin_gbp,
    tse.spv,
    mg.max_gpv_per_territory,
    COALESCE(
            IFF(tse.spv >= 500, mt.margin_gbp / tse.spv, UNIFORM(0::float, 0.001::float, RANDOM())),
            UNIFORM(0::float, 0.001::float, RANDOM())
        )                                AS gpv,
    - mg.max_gpv_per_territory - 1 + gpv AS reverse_gpv
FROM se.data.dim_sale ds
    INNER JOIN latest_vault.cms_mysql.territory st ON ds.posa_territory = st.name
    LEFT JOIN  margin_table mt
               ON st.id = mt.territory_id
                   AND ds.se_sale_id = mt.se_sale_id
    LEFT JOIN  total_spv_events tse
               ON ds.se_sale_id = tse.deal_id
                   AND mt.territory_id = tse.territory_id
    LEFT JOIN  max_gpv mg ON COALESCE(mt.territory_id, tse.territory_id) = mg.territory_id;

------------------------------------------------------------------------------------------------------------------------

WITH spvs AS (
    SELECT
        ude.deal_id,
        ude.territory_id,
        COUNT(*) AS spv
    FROM data_science.predictive_modeling.user_deal_events ude
    WHERE ude.evt_date >= CURRENT_DATE - 7
      AND ude.evt_name = 'deal-view'
    GROUP BY 1, 2
),
     std_dev_mean AS (
         SELECT
             s.territory_id,
             AVG(s.spv)        AS avg_spvs,
             STDDEV_POP(s.spv) AS std_spvs,
             std_spvs - avg_spvs
         FROM spvs s
         GROUP BY 1
     )
SELECT
    s.deal_id,
    s.spv,
    sdm.*
FROM spvs s
    INNER JOIN std_dev_mean sdm ON s.territory_id = sdm.territory_id
WHERE s.territory_id = 4
ORDER BY s.spv;


