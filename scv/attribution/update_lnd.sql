SELECT COUNT(*)
FROM se.data.scv_touch_basic_attributes stba;
--522,240,610


--inserted 1,631,387
--updated 120,925,753
CREATE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
USE WAREHOUSE pipe_xlarge;

DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
WHERE mta.attribution_model = 'last non direct';

MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution AS target
    USING (
        WITH all_touches_from_users AS (
            --create a proxy touch id and touch tstamp and nullify it if the touch is mkt channel direct
            SELECT c.touch_id,
                   b.touch_start_tstamp,
                   c.touch_mkt_channel,
                   c.attributed_user_id,
                   CASE
                       --don't nullify if first touch
                       WHEN LAG(c.touch_mkt_channel)
                                OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                           THEN c.touch_id
                       --nullify if is a direct channel
                       WHEN c.touch_mkt_channel = 'Direct'
                           THEN NULL
                       ELSE c.touch_id
                       END AS nullify_touch_id,
                   --we will also bring the touch date down so we can compare the date of the attributed
                   --touch to the current touch
                   CASE
                       --don't nullify if first touch
                       WHEN LAG(c.touch_mkt_channel)
                                OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                           THEN b.touch_start_tstamp
                       --nullify if is a direct channel
                       WHEN c.touch_mkt_channel = 'Direct'
                           THEN NULL
                       ELSE b.touch_start_tstamp
                       END AS nullify_touch_start_tstamp
            FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                     INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
                                ON c.touch_id = b.touch_id
            -- get all touches from users who have had a new touch
        ),
             last_value AS (
                 --use proxy touch id and touch tstamp to back fill nulls
                 SELECT touch_id,
                        touch_start_tstamp,
                        touch_mkt_channel,
                        attributed_user_id,
                        LAST_VALUE(nullify_touch_id) IGNORE NULLS OVER
                            (PARTITION BY attributed_user_id ORDER BY touch_start_tstamp
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS persisted_touch_id,
                        LAST_VALUE(nullify_touch_start_tstamp) IGNORE NULLS OVER
                            (PARTITION BY attributed_user_id ORDER BY touch_start_tstamp
                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS persisted_touch_start_tstamp
                 FROM all_touches_from_users
             )
             --check that the back fills don't persist longer than 6months
        SELECT touch_id,
               --        touch_start_tstamp,
               --        touch_mkt_channel,
               --        attributed_user_id,
               --        persisted_touch_id,
               --        persisted_touch_start_tstamp,
               CASE
                   WHEN touch_id != persisted_touch_id AND
                       -- if a different non direct touch id exists AND its within 6 months then use it
                        DATEDIFF(DAY, persisted_touch_start_tstamp, touch_start_tstamp) <= 30
                       THEN persisted_touch_id
                   ELSE touch_id END AS attributed_touch_id,
               'last non direct'     AS attribution_model,
               1                     AS attributed_weight
        FROM last_value

    ) AS batch ON target.touch_id = batch.touch_id
        AND target.attributed_touch_id = batch.attributed_touch_id
        AND target.attribution_model = batch.attribution_model
    WHEN NOT MATCHED
        THEN INSERT (
                     schedule_tstamp,
                     run_tstamp,
                     operation_id,
                     created_at,
                     updated_at,
                     touch_id,
                     attributed_touch_id,
                     attribution_model,
                     attributed_weight
        )
        VALUES ('2021-01-19 03:00:00',
                '2021-01-20 04:39:55',
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py__20210119T030000__daily_at_03h00',
                CURRENT_TIMESTAMP()::TIMESTAMP,
                CURRENT_TIMESTAMP()::TIMESTAMP,
                batch.touch_id,
                batch.attributed_touch_id,
                batch.attribution_model,
                batch.attributed_weight)
    WHEN MATCHED THEN UPDATE SET
        target.schedule_tstamp = '2021-01-19 03:00:00',
        target.run_tstamp = '2021-01-20 04:39:55',
        target.operation_id =
                'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py__20210119T030000__daily_at_03h00',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.touch_id = batch.touch_id,
        target.attributed_touch_id = batch.attributed_touch_id,
        target.attribution_model = batch.attribution_model,
        target.attributed_weight = batch.attributed_weight
;
--prod lnd
SELECT mtmc.touch_mkt_channel,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
                    ON mtba.touch_id = mta.touch_id AND mta.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
                    ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp >= '2020-01-01'
AND mtba.touch_start_tstamp < CURRENT_DATE - 1
GROUP BY 1;

--prod lc
SELECT mtmc.touch_mkt_channel,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
                    ON mtba.touch_id = mtmc.touch_id
     --just adding to keep filtering consistent
--          INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
--                     ON mtba.touch_id = mta.touch_id AND mta.attribution_model = 'last non direct'
WHERE mtba.touch_start_tstamp >= '2020-01-01'
  AND mtba.touch_start_tstamp < CURRENT_DATE - 1
GROUP BY 1;

--dev
SELECT mtmc.touch_mkt_channel,
       COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
         INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
                    ON mtba.touch_id = mta.touch_id AND mta.attribution_model = 'last non direct'
         INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
                    ON mta.attributed_touch_id = mtmc.touch_id
WHERE mtba.touch_start_tstamp >= '2020-01-01'
AND mtba.touch_start_tstamp < CURRENT_DATE - 1
GROUP BY 1;

SELECT * FROM se.data_pii.se_user_subscription_event suse
limit 10;


SELECT * FROM data_vault_mvp.sfsc_snapshots.inclusion_snapshot i;