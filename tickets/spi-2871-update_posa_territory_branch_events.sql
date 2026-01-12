SELECT
    COALESCE(es.unstruct_event_com_branch_secretescapes_install_1['install_activity']['data']['country_code']::VARCHAR, 'UK'),
    *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.unstruct_event_com_branch_secretescapes_install_1 IS NOT NULL
  AND es.posa_territory IS NULL;

SELECT
    MIN(es.event_tstamp)
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.unstruct_event_com_branch_secretescapes_install_1 IS NOT NULL
  AND es.posa_territory IS NULL;

-- update code in event stream
-- run sql update script on event stream
-- run sql update script on touch basic attributes for posa territory and hostname territory
-- run sql update script on touch marketing channel for hostname territory and affiliate territory


/*UPDATE hygiene_vault_mvp_dev_robin.snowplow.event_stream es
SET es.posa_territory = COALESCE(es.unstruct_event_com_branch_secretescapes_install_1['install_activity']['data']['country_code']::VARCHAR, 'UK')
WHERE es.unstruct_event_com_branch_secretescapes_install_1 IS NOT NULL
  AND es.posa_territory IS NULL
  AND es.event_tstamp >= '2020-01-29'
  AND es.device_platform IN ('native app android',
                             'native app ios'
    );*/

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;

USE WAREHOUSE pipe_xlarge;

CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.update_branch_events AS
SELECT
    es.event_hash,
    es.unstruct_event_com_branch_secretescapes_install_1
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream es
WHERE es.unstruct_event_com_branch_secretescapes_install_1 IS NOT NULL
  AND es.posa_territory IS NULL
  AND es.event_tstamp >= '2020-01-29'
  AND es.device_platform IN ('native app android',
                             'native app ios'
    );

USE WAREHOUSE pipe_4xlarge;

MERGE INTO hygiene_vault_mvp_dev_robin.snowplow.event_stream target
    USING scratch.robinpatel.update_branch_events batch
    ON target.event_hash = batch.event_hash
    WHEN MATCHED AND target.posa_territory IS NULL
        THEN UPDATE SET
        target.posa_territory = COALESCE(batch.unstruct_event_com_branch_secretescapes_install_1['install_activity']['data']['country_code']::VARCHAR, 'UK'),
        target.updated_at = CURRENT_TIMESTAMP::TIMESTAMP
;


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp.snowplow.event_stream_20220920 CLONE hygiene_vault_mvp.snowplow.event_stream;

CREATE OR REPLACE TABLE hygiene_vault_mvp.snowplow.event_stream CLONE hygiene_vault_mvp_dev_robin.snowplow.event_stream;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
-- list of touches that have a spv but don't have an affiliate territory
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.touch_ids_to_update AS (
    WITH touch_ids_without_affiliate_territory AS (
        SELECT
            mtmc.touch_id
        FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
        WHERE mtmc.touch_affiliate_territory IS NULL
    )
    SELECT DISTINCT
        mts.touch_id
    FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
        INNER JOIN touch_ids_without_affiliate_territory ti ON mts.touch_id = ti.touch_id --19 rows
);

SELECT * FROM scratch.robinpatel.touch_ids_to_update;

USE WAREHOUSE pipe_xlarge;
CREATE OR REPLACE TRANSIENT TABLE scratch.robinpatel.touch_affiliate_territory AS (
    SELECT DISTINCT
        t.touch_id,
        IFF(es.posa_territory = 'GB', 'UK', es.posa_territory) AS posa_territory
    FROM scratch.robinpatel.touch_ids_to_update t
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touchification mt ON t.touch_id = mt.touch_id
        INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mt.event_hash = es.event_hash
        QUALIFY ROW_NUMBER() OVER (PARTITION BY t.touch_id ORDER BY es.event_tstamp) = 1
);


MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel target
    USING scratch.robinpatel.touch_affiliate_territory batch
    ON target.touch_id = batch.touch_id AND target.touch_affiliate_territory IS NULL
    WHEN MATCHED THEN UPDATE SET
        target.touch_affiliate_territory = batch.posa_territory;

--check it returns no rows
WITH touch_ids_without_affiliate_territory AS (
    SELECT
        mtmc.touch_id
    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
    WHERE mtmc.touch_affiliate_territory IS NULL
)
SELECT
    mts.touch_id
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
    INNER JOIN touch_ids_without_affiliate_territory ti ON mts.touch_id = ti.touch_id --19 rows

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;


MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
    USING scratch.robinpatel.touch_affiliate_territory batch
    ON target.touch_id = batch.touch_id AND target.touch_hostname_territory IS NULL
    WHEN MATCHED THEN UPDATE SET
        target.touch_hostname_territory = batch.posa_territory;


MERGE INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes target
    USING scratch.robinpatel.touch_affiliate_territory batch
    ON target.touch_id = batch.touch_id AND target.touch_posa_territory IS NULL
    WHEN MATCHED THEN UPDATE SET
        target.touch_posa_territory = batch.posa_territory;


------------------------------------------------------------------------------------------------------------------------
-- run on pipeline runner:

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp.snowplow.event_stream_20220920 CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TABLE hygiene_vault_mvp.snowplow.event_stream CLONE hygiene_vault_mvp_dev_robin.snowplow.event_stream;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_20220920 CLONE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel;
CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_20220920 CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;