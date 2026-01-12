SELECT e.event_hash,
       t.touch_id,
       e.event_tstamp,
       e.se_sale_id,
       'screen views'      AS event_category,
       'SPV'               AS event_subcategory,
       '{schedule_tstamp}' AS schedule_tstamp

FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
WHERE e.device_platform = 'native app'
  AND t.updated_at >= TIMESTAMPADD('day', -1, '{schedule_tstamp}'::TIMESTAMP)
  AND (
        ( --for spvs that happen in the app
                e.event_name = 'screen_view' AND
                (
                        ( -- old world native app event data
                                e.collector_tstamp < '2020-02-28 00:00:00'
                                AND
                                se_sale_id IS NOT NULL
                            )
                        OR
                        ( -- new world native app event data
                                e.collector_tstamp >= '2020-02-28 00:00:00'
                                AND
                                e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
                            )
                    )
            )
        OR
        ( --to include catalogue spvs that occur on tb in wrap
                e.event_name = 'page_view'
                AND e.is_server_side_event = TRUE
                AND e.contexts_com_secretescapes_product_display_context_1[0]['tech_platform']::VARCHAR =
                    'Travelbird Platform'
                AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
            )
    );

USE WAREHOUSE pipe_xlarge;

--catalogue spvs in app

SELECT e.event_hash,
       t.touch_id,
       e.event_tstamp,
       e.se_sale_id,
       'screen views' AS event_category,
       'SPV'          AS event_subcategory
--        ,
--        '{schedule_tstamp}' AS schedule_tstamp

FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
WHERE e.collector_tstamp >= '2020-02-01'
  AND e.device_platform = 'native app'
  AND e.event_name = 'page_view'
  AND e.is_server_side_event = TRUE
  AND e.contexts_com_secretescapes_product_display_context_1[0]['tech_platform']::VARCHAR = 'Travelbird Platform'
  AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
;


SELECT count(*)
FROM data_vault_mvp.single_customer_view_stg.module_touchification t
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
WHERE e.collector_tstamp >= '2020-02-01'
  AND e.device_platform = 'native app'
  AND e.event_name = 'page_view'
  AND e.contexts_com_secretescapes_product_display_context_1[0]['tech_platform']::VARCHAR = 'Travelbird Platform'
  AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
;

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

self_describing_task --include 'dv/dwh_rec/events/07_events_of_interest/01_module_touched_spvs'  --method 'run' --start '2020-04-29 00:00:00' --end '2020-04-29 00:00:00'

DROP SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;
DROP TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream;
USE WAREHOUSE pipe_large;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs_bkup CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.n_app_cat_spvs AS (
    SELECT current_date                         AS schedule_tstamp,
           current_timestamp::TIMESTAMP         AS run_tstamp,
           'backfill native app catalogue spvs' AS operation_id,
           current_timestamp::TIMESTAMP         AS created_at,
           current_timestamp::TIMESTAMP         AS updated_at,

           e.event_hash,
           t.touch_id,
           e.event_tstamp,
           e.se_sale_id,
           'screen views'                       AS event_category,
           'SPV'                                AS event_subcategory

    FROM data_vault_mvp.single_customer_view_stg.module_touchification t
             INNER JOIN hygiene_vault_mvp.snowplow.event_stream e ON e.event_hash = t.event_hash
    WHERE e.collector_tstamp >= '2020-02-01'
      AND e.device_platform = 'native app'
      AND e.event_name = 'page_view'
      AND e.is_server_side_event = TRUE
      AND e.contexts_com_secretescapes_product_display_context_1[0]['tech_platform']::VARCHAR = 'Travelbird Platform'
      AND e.contexts_com_secretescapes_content_context_1[0]['sub_category']::VARCHAR = 'sale'
);

MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS target
    USING data_vault_mvp_dev_robin.single_customer_view_stg.n_app_cat_spvs AS batch ON target.event_hash = batch.event_hash
    WHEN NOT MATCHED
        THEN INSERT VALUES (batch.schedule_tstamp,
                            batch.run_tstamp,
                            batch.operation_id,
                            batch.created_at,
                            batch.updated_at,
                            batch.event_hash,
                            batch.touch_id,
                            batch.event_tstamp,
                            batch.se_sale_id,
                            batch.event_category,
                            batch.event_subcategory);

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.n_app_cat_spvs;

--on production run:
CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs;

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs;

SELECT event_tstamp::DATE AS date, count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs
WHERE event_tstamp >= '2020-02-01'
GROUP BY 1;

SELECT event_tstamp::DATE AS date, count(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs_bkup
WHERE event_tstamp >= '2020-02-01'
GROUP BY 1;
