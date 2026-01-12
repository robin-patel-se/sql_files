target_table_ref = DBObjectRef(
        db_name=VAULTS['dv'],
        schema_name=schema_name,
        object_name='module_touch_attribution',
    )

    clone_target_table_ref = DBObjectRef(
        db_name=target_table_ref.db_name,
        schema_name=target_table_ref.schema_name,
        object_name=f"{target_table_ref.object_name}_clone",
    )

    t_basic_attributes_table_ref = DBObjectRef(
        db_name=VAULTS['dv'],
        schema_name=schema_name,
        object_name='module_touch_basic_attributes',
    )

    t_marketing_channel_table_ref = DBObjectRef(
        db_name=VAULTS['dv'],
        schema_name=schema_name,
        object_name='module_touch_marketing_channel',
    )

    third_input_table_ref = DBObjectRef(
        db_name=VAULTS['dv'],
        schema_name=schema_name,
        object_name='module_touchification',
    )


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution CLONE data_vault_mvp.single_customer_view_stg.module_touch_attribution;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;


self_describing_task --include 'dv/dwh/events/06_touch_attribution/01_module_touch_attribution.py'  --method 'run' --start '2021-04-14 00:00:00' --end '2021-04-14 00:00:00'

SELECT mta.attribution_model, COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
GROUP BY 1;

SELECT count(*) FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba;
SELECT mta.attribution_model, COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
GROUP BY 1;


--delete existing attribution

TRUNCATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_20210415 clone data_vault_mvp.single_customer_view_stg.module_touch_attribution;


--recalculate history
MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution AS target
    USING (
        WITH all_touches_from_users AS (
            --create a proxy touch id and touch tstamp and nullify it if the touch is mkt channel is not paid
            SELECT c.touch_id,
                   b.touch_start_tstamp,
                   c.touch_mkt_channel,
                   c.attributed_user_id,
                   --channels to nullify
                   IFF(c.touch_mkt_channel NOT IN (
                                                   'PPC - Brand',
                                                   'PPC - Non Brand CPA',
                                                   'PPC - Non Brand CPL',
                                                   'PPC - Undefined',
                                                   'Display CPA',
                                                   'Display CPL',
                                                   'Paid Social CPA',
                                                   'Paid Social CPL',
                                                   'Affiliate Program'), TRUE, FALSE) AS is_nullify_channel,
                   CASE
                       --don't nullify if first touch
                       WHEN LAG(c.touch_mkt_channel)
                                OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                           THEN c.touch_id
                       --nullify if is a not a paid channel
                       WHEN is_nullify_channel THEN NULL
                       ELSE c.touch_id
                       END                                                            AS nullify_touch_id,
                   CASE
                       --don't nullify if first touch
                       WHEN LAG(c.touch_mkt_channel)
                                OVER (PARTITION BY c.attributed_user_id ORDER BY b.touch_start_tstamp) IS NULL
                           THEN b.touch_start_tstamp
                       --nullify if is a not a paid channel
                       WHEN is_nullify_channel THEN NULL
                       ELSE b.touch_start_tstamp
                       END                                                            AS nullify_touch_start_tstamp
            FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel c
                     INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
                                ON c.touch_id = b.touch_id
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
        THEN INSERT VALUES ('2021-04-13 03:00:00',
                            '2021-04-15 12:03:49',
                            'DEV-52487-include_affiliate_in_last_paid_attribution',
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            CURRENT_TIMESTAMP()::TIMESTAMP,
                            batch.touch_id,
                            batch.attributed_touch_id,
                            batch.attribution_model,
                            batch.attributed_weight)
    WHEN MATCHED THEN UPDATE SET
        target.schedule_tstamp = '2021-04-13 03:00:00',
        target.run_tstamp = '2021-04-15 12:03:49',
        target.operation_id =
                'DEV-52487-include_affiliate_in_last_paid_attribution',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.touch_id = batch.touch_id,
        target.attributed_touch_id = batch.attributed_touch_id,
        target.attribution_model = batch.attribution_model,
        target.attributed_weight = batch.attributed_weight;

USE WAREHOUSE pipe_2xlarge;

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
        THEN INSERT VALUES (
                CURRENT_TIMESTAMP::TIMESTAMP,
                CURRENT_TIMESTAMP::TIMESTAMP,
                'DEV-52487-include_affiliate_in_last_paid_attribution',
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
                'DEV-52487-include_affiliate_in_last_paid_attribution',
        target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

        target.touch_id = batch.touch_id,
        target.attributed_touch_id = batch.attributed_touch_id,
        target.attribution_model = batch.attribution_model,
        target.attributed_weight = batch.attributed_weight
;

SELECT COUNT(*) FROM se.data.scv_touch_basic_attributes stba;




