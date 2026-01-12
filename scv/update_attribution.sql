CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20210202 CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

DELETE FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta WHERE mta.attribution_model = 'last non direct';

USE WAREHOUSE pipe_xlarge;

MERGE INTO data_vault_mvp.single_customer_view_stg.module_touch_attribution AS target
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
        VALUES (CURRENT_TIMESTAMP::TIMESTAMP,
                CURRENT_TIMESTAMP::TIMESTAMP,
                'DEV-50120 - refactor_lookback_window_repopulate',
                CURRENT_TIMESTAMP()::TIMESTAMP,
                CURRENT_TIMESTAMP()::TIMESTAMP,
                batch.touch_id,
                batch.attributed_touch_id,
                batch.attribution_model,
                batch.attributed_weight)
    WHEN MATCHED THEN UPDATE SET
        target.schedule_tstamp = CURRENT_TIMESTAMP::TIMESTAMP,
        target.run_tstamp = CURRENT_TIMESTAMP::TIMESTAMP,
        target.operation_id =
                'DEV-50120 - refactor_lookback_window_repopulate',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.touch_id = batch.touch_id,
        target.attributed_touch_id = batch.attributed_touch_id,
        target.attribution_model = batch.attribution_model,
        target.attributed_weight = batch.attributed_weight
;


------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20210210 CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;

USE WAREHOUSE  pipe_xlarge;
DELETE FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta WHERE mta.attribution_model = 'last paid';

MERGE INTO data_vault_mvp.single_customer_view_stg.module_touch_attribution AS target
    USING (
             WITH all_touches_from_users AS (
                 --create a proxy touch id and touch tstamp and nullify it if the touch is mkt channel is not paid
                 SELECT c.touch_id,
                        b.touch_start_tstamp,
                        c.touch_mkt_channel,
                        c.attributed_user_id,
                        CASE
                            --don't nullify if first touch
                            WHEN LAG(c.touch_mkt_channel)
                                     OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                                THEN c.touch_id
                            --nullify if is a not a paid channel
                            WHEN c.touch_mkt_channel NOT IN ('PPC - Brand',
                                                             'PPC - Non Brand CPA',
                                                             'PPC - Non Brand CPL',
                                                             'PPC - Undefined',
                                                             'Display CPA',
                                                             'Display CPL',
                                                             'Paid Social CPA',
                                                             'Paid Social CPL')
                                THEN NULL
                            ELSE c.touch_id
                            END AS nullify_touch_id,
                        CASE
                            --don't nullify if first touch
                            WHEN LAG(c.touch_mkt_channel)
                                     OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                                THEN b.touch_start_tstamp
                            --nullify if is a not a paid channel
                            WHEN c.touch_mkt_channel NOT IN
                                 ('PPC - Brand',
                                 'PPC - Non Brand CPA',
                                 'PPC - Non Brand CPL',
                                 'PPC - Undefined',
                                 'Display CPA',
                                 'Display CPL',
                                 'Paid Social CPA',
                                 'Paid Social CPL')
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
             --check that the back fills don't persist longer than 30 days
        SELECT touch_id,
               --        touch_start_tstamp,
               --        touch_mkt_channel,
               --        attributed_user_id,
               --        persisted_touch_id,
               --        persisted_touch_start_tstamp,
               CASE
                   WHEN touch_id != persisted_touch_id AND
                       -- if a different paid touch id exists AND its within 30 days then use it
                        DATEDIFF(DAY, persisted_touch_start_tstamp, touch_start_tstamp) <= 30
                       THEN persisted_touch_id
                   ELSE touch_id END AS attributed_touch_id,
               'last paid'           AS attribution_model,
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
        VALUES ('2021-02-08 03:00:00',
                '2021-02-10 10:32:33',
                'refactor_lookback_window_repopulate',
                CURRENT_TIMESTAMP()::TIMESTAMP,
                CURRENT_TIMESTAMP()::TIMESTAMP,
                batch.touch_id,
                batch.attributed_touch_id,
                batch.attribution_model,
                batch.attributed_weight)
    WHEN MATCHED THEN UPDATE SET
        target.schedule_tstamp = '2021-02-08 03:00:00',
        target.run_tstamp = '2021-02-10 10:32:33',
        target.operation_id =
                'refactor_lookback_window_repopulate',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.touch_id = batch.touch_id,
        target.attributed_touch_id = batch.attributed_touch_id,
        target.attribution_model = batch.attribution_model,
        target.attributed_weight = batch.attributed_weight;

