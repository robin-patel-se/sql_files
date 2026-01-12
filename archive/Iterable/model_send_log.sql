USE WAREHOUSE pipe_xlarge;
DROP VIEW scratch.robinpatel.vw_recommended_deals_augmented;
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.vw_recommended_deals_augmented AS (
    SELECT *
    FROM data_science.operational_output.vw_recommended_deals_augmented
);
CREATE SCHEMA data_science_dev_robin.operational_output;
CREATE OR REPLACE VIEW data_science_dev_robin.operational_output.vw_recommended_deals_augmented AS
SELECT *
FROM data_science.operational_output.vw_recommended_deals_augmented;

-- SELECT rda.user_id,
--        array_agg(object_construct(*))
-- FROM scratch.robinpatel.vw_recommended_deals_augmented rda
-- WHERE rda.RANK_POSITION <= 50
-- GROUP BY 1;
--
-- SELECT COUNT(*) FROM  scratch.robinpatel.vw_recommended_deals_augmented rda
-- WHERE rda.RANK_POSITION <= 50;
--
-- SELECT count(*) FROM hygiene_snapshot_vault_mvp.sfmc.athena_send_log asl WHERE asl.log_date::DATE = current_date-1;


SELECT CURRENT_DATE AS view_date,
       rda.territory_id,
       rda.user_id,
       rda.deal_id,
       rda.rank_score_raw,
       rda.rank_score_deal_improvement,
       rda.margin_gbp,
       rda.spv,
       rda.gpv,
       rda.max_gpv_per_territory,
       rda.reverse_gpv,
       rda.rank_score_type,
       rda.rank_position,
       rda.rank_score
FROM scratch.robinpatel.vw_recommended_deals_augmented rda
WHERE rda.rank_position <= 50;

SELECT GET_DDL('table', 'scratch.robinpatel.vw_recommended_deals_augmented');

CREATE OR REPLACE TRANSIENT TABLE vw_recommended_deals_augmented
(
    territory_id                NUMBER,
    user_id                     NUMBER,
    deal_id                     VARCHAR,
    rank_score_raw              NUMBER,
    rank_score_deal_improvement FLOAT,
    margin_gbp                  FLOAT,
    spv                         NUMBER,
    gpv                         FLOAT,
    max_gpv_per_territory       FLOAT,
    reverse_gpv                 FLOAT,
    rank_score_type             VARCHAR,
    rank_position               NUMBER,
    rank_score                  FLOAT,
);

self_describing_task --include 'dv/dwh/athena/recommended_deals_snapshot.py'  --method 'run' --start '2021-11-08 00:00:00' --end '2021-11-08 00:00:00'

USE WAREHOUSE pipe_2xlarge;
SELECT *
FROM scratch.robinpatel.vw_recommended_deals_augmented
    QUALIFY COUNT(*) OVER (PARTITION BY user_id, deal_id) > 1;


SELECT *
FROM data_science.operational_output.vw_recommended_deals_augmented
    QUALIFY COUNT(*) OVER (PARTITION BY user_id, deal_id) > 1;

WITH dupes AS (
    SELECT *
    FROM data_science.operational_output.vw_recommended_deals_augmented
        QUALIFY COUNT(*) OVER (PARTITION BY user_id, deal_id) > 1
)
SELECT COUNT(*)
FROM dupes;


SELECT *
FROM data_science.operational_output.daily_deals_selections;
SELECT *
FROM data_science.operational_output.daily_deals_selections
WHERE daily_deals_selections.user_id = -2;

------------------------------------------------------------------------------------------------------------------------
--how many users have a personalised recommendation
SELECT COUNT(DISTINCT ur.user_id)
FROM data_science.operational_output.daily_deals_selections ur
WHERE ur.planning_date = CURRENT_DATE - 1;
--2,846,789 -- user recommendations

--compute user recommendations, these are based on user activity
WITH stack_ranks AS (
    SELECT ur.user_id,
           ur.territory_id,
           ur.planning_position,
           ur.planning_date,
           ur.deal_id,
           ur.load_ts,
           ur.last_modified_ts,
           ur.error_code,
           'user_rank' AS rank_type,
           1           AS rank_type_index --enforce a hieracrhy
    FROM data_science.operational_output.daily_deals_selections ur
    WHERE ur.planning_date = CURRENT_DATE - 1
      AND ur.user_id = 2846789

    UNION ALL

--compute default territory recommendations at user level
    SELECT iup.shiro_user_id,
           dds.territory_id,
           dds.planning_position,
           dds.planning_date,
           dds.deal_id,
           dds.load_ts,
           dds.last_modified_ts,
           dds.error_code,
           'default_territory_rank' AS rank_type,
           2                        AS rank_type_index --enforce a hieracrhy
    FROM data_vault_mvp.dwh.iterable__user_profile iup --only use users that would be in Iterable
        INNER JOIN data_vault_mvp.dwh.user_attributes ua ON iup.shiro_user_id = ua.shiro_user_id
        INNER JOIN data_science.operational_output.daily_deals_selections dds ON ua.current_affiliate_territory_id = dds.territory_id AND dds.user_id = -2
    WHERE iup.shiro_user_id = 2846789
      AND dds.planning_date = CURRENT_DATE - 1
),
     remove_duplicate_recommendations AS (
         --duplications occur across the user recommendations and default territory recommendations so want to remove the later duplicates of these
         SELECT *
         FROM stack_ranks sr
             --if any deal id appears more than once for a user and planning date then select the highest position one
             QUALIFY ROW_NUMBER() OVER (PARTITION BY sr.user_id, sr.planning_date, sr.deal_id ORDER BY sr.rank_type_index, sr.planning_position) = 1
     )
SELECT rdr.user_id,
       rdr.territory_id,
       rdr.planning_position,
       rdr.planning_date,
       rdr.deal_id,
       rdr.load_ts,
       rdr.last_modified_ts,
       rdr.error_code,
       rdr.rank_type,
       rdr.rank_type_index,
       ROW_NUMBER() OVER (PARTITION BY rdr.user_id, rdr.planning_date::DATE ORDER BY rdr.rank_type_index, rdr.planning_position) AS derived_email_position
FROM remove_duplicate_recommendations rdr
;



SELECT *
FROM data_science.operational_output.daily_deals_selections dds
WHERE dds.planning_date = CURRENT_DATE - 1
  AND dds.user_id = -2
  AND dds.territory_id = 10


SELECT posa_territory,
       COUNT(DISTINCT ssa.salesforce_opportunity_id) AS sales
FROM se.data.se_sale_attributes ssa
WHERE ssa.sale_active
GROUP BY 1