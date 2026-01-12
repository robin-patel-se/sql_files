-- ratio of null se_sale_id spvs
SELECT sts.event_tstamp::DATE                 AS date,
       COUNT(*)                               AS spvs,
       SUM(IFF(sts.se_sale_id IS NULL, 1, 0)) AS null_se_sale_ids,
       null_se_sale_ids / spvs
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp >= CURRENT_DATE - 30
GROUP BY 1
;


-- ratio of null se_sale_id spvs for app
SELECT sts.event_tstamp::DATE                 AS date,
       COUNT(*)                               AS spvs,
       SUM(IFF(sts.se_sale_id IS NULL, 1, 0)) AS null_se_sale_ids,
       null_se_sale_ids / spvs
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp >= '2021-01-01'
  AND sts.event_category = 'screen views'
GROUP BY 1
;


SELECT CASE
           WHEN --ss events
                       v_tracker LIKE 'py-%' --TB ss events
                   OR
                       v_tracker LIKE 'java-%' --SE core ss events
                   OR
                   --cs native ios app screen view events after new world adjustments
                       (app_id LIKE 'ios_app%' AND collector_tstamp::DATE >= '2020-02-28')
                   OR
                   --cs native android app screen view events
                       (v_tracker LIKE 'andr-%' AND collector_tstamp::DATE >= '2020-02-28')
               THEN COALESCE(contexts_com_secretescapes_secret_escapes_sale_context_1[0]['id']::VARCHAR,
                             ses.contexts_com_secretescapes_sale_page_context_1[0]['sale_id']::VARCHAR
               )
           -->get the sale id from the secret escapes sale context for CS events
           -- nullif due to implementation issue that set sale id to 0.
           ELSE NULLIF(contexts_com_secretescapes_sale_page_context_1[0]['sale_id']::VARCHAR, '0')
           END AS se_sale_id,
       contexts_com_secretescapes_secret_escapes_sale_context_1[0]['sale_id']::VARCHAR,
       ses.contexts_com_secretescapes_sale_page_context_1
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_hash = '481bb95666b07157e07e03cc2e4f47dbc9e0711040f040c26b17bc30d5115c5b'
  AND ses.event_tstamp::DATE = '2022-02-21';


SELECT DISTINCT
       es.posa_territory
FROM hygiene_vault_mvp.snowplow.event_stream es;





UPDATE data_vault_mvp.single_customer_view_stg.module_touched_spvs AS target
SET target.se_sale_id = batch.se_sale_id
FROM (
    SELECT mts.event_hash,
           es.contexts_com_secretescapes_sale_page_context_1[0]['sale_id']::VARCHAR AS se_sale_id
    FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
        INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mts.event_hash = es.event_hash
    WHERE mts.se_sale_id IS NULL
      AND mts.event_category = 'screen views'
) AS batch
WHERE target.event_hash = batch.event_hash
  AND target.se_sale_id IS NULL
  AND target.event_category = 'screen views'


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;

USE WAREHOUSE pipe_2xlarge;

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS target
SET target.se_sale_id = batch.se_sale_id
FROM (
    SELECT mts.event_hash,
           es.contexts_com_secretescapes_sale_page_context_1[0]['sale_id']::VARCHAR AS se_sale_id
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs mts
        INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mts.event_hash = es.event_hash
    WHERE mts.se_sale_id IS NULL
      AND mts.event_category = 'screen views'
) AS batch
WHERE target.event_hash = batch.event_hash
  AND target.se_sale_id IS NULL
  AND target.event_category = 'screen views';



SELECT sts.event_tstamp::DATE                 AS date,
       COUNT(*)                               AS spvs,
       SUM(IFF(sts.se_sale_id IS NULL, 1, 0)) AS null_se_sale_ids,
       null_se_sale_ids / spvs
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
WHERE sts.event_tstamp >= '2021-01-01'
  AND sts.event_category = 'screen views'
GROUP BY 1
;

DROP VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;

USE WAREHOUSE pipe_2xlarge;

UPDATE hygiene_vault_mvp_dev_robin.snowplow.event_stream AS target
SET target.se_sale_id = batch.se_sale_id
FROM (
    SELECT s.event_hash,
           s.se_sale_id
    FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs s
    WHERE s.event_category = 'screen views'
)
    AS batch
WHERE target.event_hash = batch.event_hash
  AND target.se_sale_id IS NULL
  AND target.device_platform LIKE 'native app%'
  AND target.event_hash IN (
    SELECT sub.event_hash
    FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs_20220322 sub
    WHERE sub.se_sale_id IS NULL
      AND sub.event_category = 'screen views'
)



SELECT sts.event_tstamp::DATE                 AS date,
       COUNT(*)                               AS spvs,
       SUM(IFF(sts.se_sale_id IS NULL, 1, 0)) AS null_se_sale_ids,
       null_se_sale_ids / spvs
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs_20220322 sts
WHERE sts.event_tstamp >= CURRENT_DATE - 30
GROUP BY 1
;

SELECT *
FROM latest_vault.iterable.campaign c
WHERE id = 3889732;

USE WAREHOUSE pipe_2xlarge;
SELECT contexts_com_secretescapes_secret_escapes_sale_context_1[0]['id']::VARCHAR,
       contexts_com_secretescapes_sale_page_context_1[0]['sale_id']::VARCHAR,
       NULLIF(contexts_com_secretescapes_sale_page_context_1[0]['sale_id']::VARCHAR, '0'),
       *
FROM hygiene_vault_mvp.snowplow.event_stream es
    INNER JOIN se.data.scv_touched_spvs sts ON es.event_hash = sts.event_hash
WHERE es.event_tstamp >= CURRENT_DATE - 1
  AND es.v_tracker LIKE 'ios-%';

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= CURRENT_DATE - 1
  AND es.contexts_com_secretescapes_secret_escapes_sale_context_1 IS NOT NULL;