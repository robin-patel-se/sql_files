SELECT *
FROM data_vault_mvp.bi.sale_date_spvs__step06__aggregate_datasets
WHERE se_sale_id IS NULL
   OR spv_date IS NULL
   OR posa_territory IS NULL;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.bi.sale_date_spvs__step06__aggregate_datasets_20221115 CLONE data_vault_mvp.bi.sale_date_spvs__step06__aggregate_datasets;


-- investigating sessions with null posa but have an spv
SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs sts
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON sts.touch_id = stmc.touch_id
WHERE stmc.touch_affiliate_territory IS NULL
  AND sts.event_tstamp::DATE = '2022-11-06'
  AND sts.se_sale_id = '114795';

SELECT
    FIRST_VALUE(ses.posa_territory) IGNORE NULLS OVER (PARTITION BY ssel.touch_id ORDER BY ssel.event_index_within_touch) AS touch_posa_territory,
    *
FROM se.data_pii.scv_event_stream ses
    INNER JOIN se.data_pii.scv_session_events_link ssel ON ses.event_hash = ssel.event_hash AND ssel.event_tstamp::DATE = '2022-11-06'
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel stmc ON ssel.touch_id = stmc.touch_id
    INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes stba ON ssel.touch_id = stba.touch_id
WHERE ses.event_tstamp::DATE = '2022-11-06'
  AND ssel.touch_id = '9f1ba9d6de29a32fcba01039d84763015562c0789ba8f31dc1ec344b47c463a4';

------------------------------------------------------------------------------------------------------------------------
--run basic touch attributes and marketing channel to assess why null territory

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;



self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2022-11-15 00:00:00' --end '2022-11-15 00:00:00'



DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification;
DROP TABLE data_vault_mvp_dev_robin.dwh.user_attributes;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_anomalous_tracker;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
WHERE mtba.touch_start_tstamp::DATE = '2022-11-06'
  AND mtba.touch_id = '9f1ba9d6de29a32fcba01039d84763015562c0789ba8f31dc1ec344b47c463a4';


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone__model_basic_attributes mtba
WHERE mtba.touch_start_tstamp::DATE = '2022-11-06'
  AND mtba.touch_id = '9f1ba9d6de29a32fcba01039d84763015562c0789ba8f31dc1ec344b47c463a4';


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
    INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
    LEFT JOIN  data_vault_mvp_dev_robin.dwh.user_attributes ua ON TRY_TO_NUMBER(t.attributed_user_id) = ua.shiro_user_id
WHERE t.updated_at >= TIMESTAMPADD('day', -1, '2022-11-14 03:00:00'::TIMESTAMP)
  AND t.touch_id = '9f1ba9d6de29a32fcba01039d84763015562c0789ba8f31dc1ec344b47c463a4';


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
WHERE mt.touch_id = '9f1ba9d6de29a32fcba01039d84763015562c0789ba8f31dc1ec344b47c463a4'
  AND mt.event_tstamp::DATE >= '2022-11-06'


------------------------------------------------------------------------------------------------------------------------
--check prod and dev

--row counts
SELECT
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba;
SELECT
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_20221116 CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

--hostname territory counts
SELECT
    mtba.touch_hostname_territory,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1;
SELECT
    mtba.touch_hostname_territory,
    COUNT(*)
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
GROUP BY 1;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes  mt
WHERE mt.touch_id = '9f1ba9d6de29a32fcba01039d84763015562c0789ba8f31dc1ec344b47c463a4'
  AND mt.touch_start_tstamp::DATE >= '2022-11-06';


SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes  mt
WHERE mt.touch_id = '9f1ba9d6de29a32fcba01039d84763015562c0789ba8f31dc1ec344b47c463a4'
  AND mt.touch_start_tstamp::DATE >= '2022-11-06';


SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touchification mt WHERE mt.updated_at::DATE = current_date;


------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM data_vault_mvp.bi.sale_date_spvs__step06__aggregate_datasets_20221117
WHERE se_sale_id IS NULL
   OR spv_date IS NULL
   OR posa_territory IS NULL;