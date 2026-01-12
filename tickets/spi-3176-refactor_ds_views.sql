-- vw_valid_deals

-- CREATE OR REPLACE VIEW data_science.predictive_modeling.vw_valid_deals AS
--prod
WITH time_constraint AS (
    SELECT
        (CASE
             WHEN DATE_PART('hour', CURRENT_TIMESTAMP) < 6
                 THEN CURRENT_TIMESTAMP
             ELSE DATEADD('day', 1, CURRENT_TIMESTAMP)
            END
            ) AS reference_date
),
     dim_sale AS
         (
             SELECT
                 se_sale_id,
                 sale_active,
                 sale_start_date,
                 sale_end_date,
                 TRIM(territory.value) AS posa_territory
             FROM se.data.dim_sale,
                  LATERAL SPLIT_TO_TABLE(IFNULL(posa_territory, ''), '|') AS territory
         )
SELECT DISTINCT
    sa.se_sale_id AS deal_id,
    ts.id         AS territory_id
FROM se.data.sale_active sa
    LEFT JOIN dim_sale ds
              ON sa.se_sale_id = ds.se_sale_id
    LEFT JOIN latest_vault.cms_mysql.territory ts
              ON ds.posa_territory = ts.name
    LEFT JOIN data_science.operational_output.vw_denylisted_deals vdd
              ON ts.id = vdd.territory_id
                  AND sa.se_sale_id = vdd.deal_id
WHERE sa.view_date = (
    SELECT
        MAX(sa.view_date)
    FROM se.data.sale_active sa
)
  AND ds.sale_active = 1
  AND ds.sale_start_date <= (
    SELECT
        reference_date
    FROM time_constraint
)
  AND (
            ds.sale_end_date > (
            SELECT
                reference_date
            FROM time_constraint
        )
        OR ds.sale_end_date IS NULL
    )
  AND vdd.tag IS NULL
;


-- CREATE OR REPLACE VIEW data_science.predictive_modeling.vw_valid_deals AS

-- dev
WITH time_constraint AS (
    SELECT
        CASE
            WHEN DATE_PART('hour', CURRENT_TIMESTAMP) < 6
                THEN CURRENT_TIMESTAMP
            ELSE DATEADD('day', 1, CURRENT_TIMESTAMP)
            END AS reference_date
),
     dim_sale AS
         (
             SELECT
                 se_sale_id,
                 sale_active,
                 sale_start_date,
                 sale_end_date,
                 TRIM(territory.value) AS posa_territory
             FROM se.data.dim_sale,
                  LATERAL SPLIT_TO_TABLE(IFNULL(posa_territory, ''), '|') AS territory
         )
SELECT DISTINCT
    ds.se_sale_id AS deal_id,
    ts.id         AS territory_id
FROM dim_sale ds
    INNER JOIN latest_vault.cms_mysql.territory ts
               ON ds.posa_territory = ts.name
    LEFT JOIN  data_science.operational_output.vw_denylisted_deals vdd
               ON ts.id = vdd.territory_id
                   AND ds.se_sale_id = vdd.deal_id
WHERE ds.sale_active
  AND ds.sale_start_date <= (
    SELECT
        reference_date
    FROM time_constraint
)
  AND (
            ds.sale_end_date > (
            SELECT
                reference_date
            FROM time_constraint
        )
        OR ds.sale_end_date IS NULL
    )
  AND vdd.tag IS NULL
;

SELECT *
FROM data_science.information_schema.views
WHERE table_name = 'VW_VALID_DEALS';



USE ROLE datasciencerunner;

CREATE OR REPLACE VIEW data_science.predictive_modeling.vw_valid_deals COPY GRANTS AS
(
WITH time_constraint AS (
    SELECT
        CASE
            WHEN DATE_PART('hour', CURRENT_TIMESTAMP) < 6
                THEN CURRENT_TIMESTAMP
            ELSE DATEADD('day', 1, CURRENT_TIMESTAMP)
            END AS reference_date
),
     dim_sale AS
         (
             SELECT
                 se_sale_id,
                 sale_active,
                 sale_start_date,
                 sale_end_date,
                 TRIM(territory.value) AS posa_territory
             FROM se.data.dim_sale,
                  LATERAL SPLIT_TO_TABLE(IFNULL(posa_territory, ''), '|') AS territory
         )
SELECT DISTINCT
    ds.se_sale_id AS deal_id,
    ts.id         AS territory_id
FROM dim_sale ds
    INNER JOIN latest_vault.cms_mysql.territory ts
               ON ds.posa_territory = ts.name
    LEFT JOIN  data_science.operational_output.vw_denylisted_deals vdd
               ON ts.id = vdd.territory_id
                   AND ds.se_sale_id = vdd.deal_id
WHERE ds.sale_active
  AND ds.sale_start_date <= (
    SELECT
        reference_date
    FROM time_constraint
)
  AND (
            ds.sale_end_date > (
            SELECT
                reference_date
            FROM time_constraint
        )
        OR ds.sale_end_date IS NULL
    )
  AND vdd.tag IS NULL
    );

SELECT *
FROM data_science.predictive_modeling.vw_valid_deals;


------------------------------------------------------------------------------------------------------------------------
-- vw_email_eligible_1m_users
USE WAREHOUSE pipe_default;
USE ROLE datasciencerunner;

CREATE OR REPLACE VIEW data_science.predictive_modeling.vw_email_eligible_1m_users COPY GRANTS AS
(
SELECT
    u.user_id,
    u.current_affiliate_territory_id AS territory_id
FROM data_vault_mvp.engagement_stg.user_snapshot u
    INNER JOIN latest_vault.cms_mysql.affiliate a ON a.id = u.current_affiliate_id
WHERE u.email_opt_in > 0
  AND (
            u.last_email_click_tstamp > CURRENT_DATE - 30
        OR u.last_email_open_tstamp > CURRENT_DATE - 30
        OR u.last_sale_pageview_tstamp > CURRENT_DATE - 30
        OR u.last_pageview_tstamp > CURRENT_DATE - 30
        OR u.last_booking_complete_tstamp > CURRENT_DATE - 30
        OR u.last_booking_abandon_tstamp > CURRENT_DATE - 30
    )
  AND u.user_id NOT IN
      (
          SELECT
              user_id
          FROM data_science.operational_output.suppressed_users
      )
  AND a.mailing IS NULL
    );

------------------------------------------------------------------------------------------------------------------------
-- vw_valid_users
USE WAREHOUSE pipe_default;
USE ROLE datasciencerunner;

SELECT GET_DDL('table', 'data_science.predictive_modeling.vw_valid_users');

CREATE OR REPLACE VIEW data_science.predictive_modeling.vw_valid_users COPY GRANTS AS
(
SELECT DISTINCT
    u.user_id,
    u.current_affiliate_territory_id AS territory_id
FROM data_vault_mvp.engagement_stg.user_snapshot u
WHERE u.email_opt_in > 0
  AND (
            u.last_email_click_tstamp > CURRENT_DATE - 30
        OR u.last_email_open_tstamp > CURRENT_DATE - 30
        OR u.last_sale_pageview_tstamp > CURRENT_DATE - 30
        OR u.last_pageview_tstamp > CURRENT_DATE - 30
        OR u.last_booking_complete_tstamp > CURRENT_DATE - 30
        OR u.last_booking_abandon_tstamp > CURRENT_DATE - 30
    )
  AND u.user_id NOT IN
      (
          SELECT
              user_id
          FROM data_science.operational_output.suppressed_users
      )
    );

--dev
SELECT
    COUNT(*)
FROM data_vault_mvp.engagement_stg.user_snapshot u;

--prod
SELECT
    COUNT(*)
FROM data_vault_mvp.engagement_stg.user_snapshot u
    INNER JOIN latest_vault.cms_mysql.affiliate a ON a.id = u.current_affiliate_id;

------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_default;
USE ROLE datasciencerunner;

-- generic_deal_recommendations
CREATE OR REPLACE VIEW data_science.predictive_modeling.generic_deal_recommendations COPY GRANTS AS
(
WITH margin_table AS (
    SELECT
        st.id                             AS territory_id,
        fcb.se_sale_id,
        SUM(fcb.margin_gross_of_toms_gbp) AS margin_gbp
    FROM se.data.fact_complete_booking fcb
        INNER JOIN latest_vault.cms_mysql.territory st ON fcb.territory = st.name
    WHERE fcb.booking_completed_date >= CURRENT_DATE - 7
    GROUP BY 1, 2
),
     total_spv_events AS (
         SELECT
             deal_id,
             territory_id,
             COUNT(*) AS spv
         FROM data_science.predictive_modeling.user_deal_events
         WHERE evt_date > CURRENT_DATE - 7
           AND evt_name = 'deal-view'
         GROUP BY 1, 2
     ),
     gpv_table AS (
         SELECT
             tse.deal_id,
             tse.territory_id,
             tse.spv,
             COALESCE(mt.margin_gbp, 0)                             AS margin_gbp,
             COALESCE(IFF(tse.spv >= 500, mt.margin_gbp / tse.spv, UNIFORM(0.00001::FLOAT, 0.001::FLOAT, RANDOM()))
                 , UNIFORM(0.00001::FLOAT, 0.001::FLOAT, RANDOM())) AS gpv
         FROM total_spv_events tse
             INNER JOIN margin_table mt ON tse.deal_id = mt.se_sale_id
             AND tse.territory_id = mt.territory_id
     )
SELECT
    vvd.territory_id,
    vvd.deal_id,
    gt.margin_gbp,
    gt.spv,
    gt.gpv,
    COALESCE(gt.gpv, UNIFORM(0::FLOAT, 0.09::FLOAT, RANDOM())) AS rank_score
FROM data_science.predictive_modeling.vw_valid_deals vvd
    LEFT JOIN gpv_table gt ON vvd.deal_id = gt.deal_id
    AND vvd.territory_id = gt.territory_id
    );


SELECT GET_DDL('table', 'data_science.predictive_modeling.user_deal_events');

SELECT *
FROM data_science.information_schema.tables
WHERE table_name = 'USER_DEAL_EVENTS';

SHOW GRANTS ON TABLE data_science.predictive_modeling.generic_deal_recommendations;

------------------------------------------------------------------------------------------------------------------------
-- vw_reverse_gpv
--- Augment rank scores with GPV values in case of zero-valued rank-score
CREATE OR REPLACE VIEW data_science.predictive_modeling.vw_reverse_gpv COPY GRANTS AS
(
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
    LEFT JOIN  max_gpv mg ON COALESCE(mt.territory_id, tse.territory_id) = mg.territory_id
    );



------------------------------------------------------------------------------------------------------------------------
-- vw_update_and_favorite_rank_scores

USE ROLE pipelinerunner;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
                                    'data_science.predictive_modeling.vw_update_and_favorite_rank_scores');

SELECT *
FROM scratch.robinpatel.table_usage;

USE ROLE pipelinerunner;

CALL data_vault_mvp.dwh.table_reference_in_view('scratch.robinpatel.table_reference_in_view',
                                                'data_science.predictive_modeling.vw_update_and_favorite_rank_scores',
                                                'data_science, collab');

SELECT *
FROM scratch.robinpatel.table_reference_in_view;


USE ROLE datasciencerunner;

CREATE OR REPLACE VIEW data_science.predictive_modeling.vw_update_and_favorite_rank_scores COPY GRANTS AS
(
WITH improved_deals_table AS (
    SELECT DISTINCT
        CASE tl.type
            WHEN 'sale' -- old data model (will be deprecated)
                THEN tl.tag_ref::VARCHAR
            WHEN 'hotelVoucherSale' -- old data model (will be deprecated)
                THEN tl.tag_ref::VARCHAR
            WHEN 'voucherSale' -- old data model (will be deprecated)
                THEN tl.tag_ref::VARCHAR
            ELSE 'A' || tl.tag_ref::VARCHAR
            END                                          AS deal_id,
        -0.51 + UNIFORM(0::FLOAT, 0.01::FLOAT, RANDOM()) AS rank_score
    FROM latest_vault.cms_mysql.tag_links tl
        INNER JOIN latest_vault.cms_mysql.tags t ON tl.tag_id = t.id
    WHERE t.name IN (
                     'zz_CRM_RateReduction',
                     'zz_CRM_ExtraAvails',
                     'zz_CRM_FreeUpgrade',
                     'zz_CRM_NewRoomType',
                     'zz_CRM_Reactivation'
        )
),
     favorite_table AS (
         SELECT
             f.user_id,
             f.last_updated,
             COALESCE('A' || f.base_sale_id, f.sale_id::VARCHAR) AS deal_id
         FROM latest_vault.cms_mysql.favorite f
     )
SELECT
    ft.user_id,
    ft.deal_id,
    idt.rank_score
FROM favorite_table ft
    INNER JOIN improved_deals_table idt ON ft.deal_id = idt.deal_id
WHERE ft.last_updated > CURRENT_DATE - (365 * 1.5)
    );

------------------------------------------------------------------------------------------------------------------------
USE ROLE datasciencerunner;
CREATE OR REPLACE VIEW data_science.predictive_modeling.vw_new_deals_boost COPY GRANTS AS
(
WITH recommended_deals AS (
    SELECT
        rd.territory_id,
        rd.deal_id,
        COUNT(DISTINCT IFF(rd.rank_score > 0, rd.user_id, NULL)) AS pos_rank_customers
    FROM data_science.operational_output.recommended_deals rd
    WHERE rd.user_id != -1
    GROUP BY 1, 2
),
     recommended_users_in_territory AS (
         SELECT
             rd.territory_id,
             COUNT(DISTINCT rd.user_id) AS total_nr_customers
         FROM data_science.operational_output.recommended_deals rd
         WHERE rd.user_id != -1
         GROUP BY 1
     ),
     all_deal_agg AS (
         SELECT
             vdf.deal_id,
             vvu.total_nr_customers AS total_nr_customers,
             vdf.start_date,
             vdf.territory_id
         FROM data_science.mart_analytics.vw_deal_features vdf
             INNER JOIN recommended_deals vvd
                        ON vdf.territory_id = vvd.territory_id
                            AND vdf.deal_id = vvd.deal_id
             INNER JOIN recommended_users_in_territory vvu ON vdf.territory_id = vvu.territory_id
     ),
     shares_table AS (
         SELECT
             ada.deal_id,
             ada.territory_id,
             ada.total_nr_customers,
             prda.pos_rank_customers,
             COALESCE(prda.pos_rank_customers, 0) / ada.total_nr_customers AS proportion,
             ada.start_date
         FROM all_deal_agg ada
             LEFT JOIN recommended_deals prda
                       ON ada.deal_id = prda.deal_id
                           AND ada.territory_id = prda.territory_id
     ),
     percentiles_compute AS (
         SELECT
             territory_id,
             PERCENTILE_DISC(0.1) WITHIN GROUP (ORDER BY proportion) AS p10
         FROM shares_table
         WHERE start_date >= CURRENT_DATE - 15
         GROUP BY 1
     )
SELECT
    st.deal_id,
    st.territory_id,
    st.proportion,
    st.start_date,
    pc.p10,
    - 0.1 + UNIFORM(0::FLOAT, 0.01::FLOAT, RANDOM()) AS rank_score
FROM shares_table st
    LEFT JOIN percentiles_compute pc ON pc.territory_id = st.territory_id
WHERE st.start_date >= CURRENT_DATE - 15
  AND st.proportion <= pc.p10
    );

SELECT *
FROM data_science.predictive_modeling.vw_new_deals_boost;

ALTER SESSION SET USE_CACHED_RESULT = FALSE;

SELECT *
FROM data_science.predictive_modeling.vw_new_deals_boost;

SELECT GET_DDL('table', 'data_science.predictive_modeling.vw_new_deals_boost');

------------------------------------------------------------------------------------------------------------------------
USE ROLE datasciencerunner;
CREATE OR REPLACE VIEW data_science.predictive_modeling.vw_deal_images COPY GRANTS AS
(
WITH modfified_v4_territories AS (
    SELECT
        sale_id,
        record__o,
        CASE affiliate_url_string
            WHEN 'travelbirdbefr' THEN 'TB-BE_FR'
            WHEN 'travelbirdbe' THEN 'TB-BE_NL'
            WHEN 'travelbirdnl' THEN 'TB-NL'
            WHEN 'es' THEN 'UK'
            WHEN 'sv' THEN 'SE'
            WHEN 'de' THEN 'DE'
            WHEN 'us' THEN 'US'
            WHEN 'dk' THEN 'DK'
            WHEN 'nor' THEN 'NO'
            WHEN 'ch' THEN 'CH'
            WHEN 'it' THEN 'IT'
            WHEN 'nl' THEN 'NL'
            WHEN 'esp' THEN 'ES'
            WHEN 'be' THEN 'BE'
            WHEN 'fr' THEN 'FR'
            WHEN 'sg' THEN 'SG'
            WHEN 'hk' THEN 'HK'
            WHEN 'id' THEN 'ID'
            WHEN 'my' THEN 'MY'
            END AS territory_name
    FROM hygiene_snapshot_vault_mvp.se_api.sales
    WHERE TO_DATE(created_at) = CURRENT_DATE()
)
SELECT
    vdf.deal_id,
    tr.id                                     AS territory_id,
    sales.territory_name,
    sales.record__o:photos[0]['url']::VARCHAR AS photo_url
FROM data_science.mart_analytics.vw_deal_features vdf
    INNER JOIN modfified_v4_territories sales
               ON vdf.territory_name = sales.territory_name
                   AND vdf.deal_id = sales.sale_id
                   AND sales.record__o:photos[0]['url']::VARCHAR IS NOT NULL
    LEFT JOIN  latest_vault.cms_mysql.territory tr ON tr.name = sales.territory_name
    );

SELECT
    sale_id,
    record__o:photos[0]['url']::VARCHAR AS photo_url
FROM hygiene_snapshot_vault_mvp.se_api.sales
;



------------------------------------------------------------------------------------------------------------------------

-- CREATE OR REPLACE VIEW data_science.predictive_modeling.high_potential_deals COPY GRANTS AS
(
WITH dm_deals_gsheets AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rank,
        posa_category,
        se_sale_id
    FROM latest_vault.trading_gsheets.athena_deal_list_gpv_dach

    UNION ALL

    SELECT
        ROW_NUMBER() OVER (ORDER BY RANDOM()) AS rank,
        posa_category,
        se_sale_id
    FROM latest_vault.trading_gsheets.athena_deal_list_gpv_uk
),
     high_potential AS
         (
             SELECT
                 ddg.se_sale_id    AS deal_id,
                 t.id              AS territory_id,
                 ddg.posa_category,
                 ddg.rank,
                 MIN(rank) OVER () AS min_score,
                 MAX(rank) OVER () AS max_score
             FROM dm_deals_gsheets ddg
                 LEFT JOIN se.data.dim_sale ds
                           ON ds.se_sale_id = ddg.se_sale_id
                 LEFT JOIN latest_vault.cms_mysql.territory t
                           ON ds.posa_territory = t.name
         )
SELECT
    deal_id,
    territory_id,
    -0.4 + (1 - (hp.rank - min_score) / (max_score - min_score)) / 10 AS scaled_score
FROM high_potential hp
);
