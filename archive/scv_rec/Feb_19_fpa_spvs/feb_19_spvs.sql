USE SCHEMA data_vault_mvp.single_customer_view_stg;

CREATE OR REPLACE TABLE collab.fpa.dwh_uk_de_spvs AS (
    SELECT CASE
               WHEN stitched_identity_type = 'se_user_id' THEN 'logged_in'
               ELSE 'logged_out' END                    AS login_status,
           spv.event_tstamp::DATE                       AS date, --spv date
           c.touch_hostname_territory,
           c.touch_affiliate_territory,
           b.touch_experience,
           c.touch_mkt_channel                          AS last_non_direct_mkt_channel,
           COUNT(DISTINCT spv.touch_id)                 AS touches,
           COUNT(spv.event_hash)                        AS spvs,
           COUNT(DISTINCT spv.se_sale_id, spv.touch_id) AS unique_spvs
    FROM module_touch_basic_attributes b
             INNER JOIN module_touch_attribution a
                        ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
             INNER JOIN module_touch_marketing_channel c ON a.attributed_touch_id = c.touch_id
             INNER JOIN module_touched_spvs spv ON b.touch_id = spv.touch_id
    WHERE spv.event_tstamp >= '2019-01-01'
      AND (
        --filter territory on attribution AND include both UK/DE in hostname or affiliate territory
            c.touch_hostname_territory IN ('UK', 'DE')
            OR
            c.touch_affiliate_territory IN ('UK', 'DE')
        )
    GROUP BY 1, 2, 3, 4, 5, 6
    ORDER BY 2, 1
)
;

SELECT *
FROM collab.fpa.dwh_uk_de_spvs;
GRANT SELECT ON TABLE collab.fpa.dwh_uk_de_spvs TO ROLE personal_role__gianniraftis;
GRANT SELECT ON TABLE collab.fpa.dwh_uk_de_spvs TO ROLE personal_role__niroshanbalakumar;
GRANT SELECT ON TABLE collab.fpa.dwh_uk_de_spvs TO ROLE personal_role__samanthamandeldallal;
GRANT SELECT ON TABLE collab.fpa.dwh_uk_de_spvs TO ROLE personal_role__carmenmardiros;

ALTER TABLE collab.fpa.dwh_feb_spvs
    RENAME TO COLLAB.FPA.DWH_UK_DE_SPVS;

SELECT b.touch_start_tstamp::DATE                   AS touch_start_date,
       b.touch_posa_territory,
       b.touch_experience,
       b.touch_hostname,
       c.touch_mkt_channel                          AS last_non_direct_mkt_channel,
       sa.product_type,
       sa.product_configuration,
       sa.product_line,
       COUNT(DISTINCT b.attributed_user_id)         AS users,
       COUNT(spv.touch_id)                          AS touches,
       COUNT(spv.event_hash)                        AS spvs,
       COUNT(DISTINCT spv.se_sale_id, spv.touch_id) AS unique_spvs
FROM module_touch_basic_attributes b
         INNER JOIN module_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN module_touch_marketing_channel c ON a.attributed_touch_id = c.touch_id
         INNER JOIN module_touched_spvs spv ON b.touch_id = spv.touch_id
         INNER JOIN se.data.dim_sale sa ON spv.se_sale_id = sa.sale_id
WHERE b.touch_start_tstamp::DATE = '2019-05-23'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY 1;


