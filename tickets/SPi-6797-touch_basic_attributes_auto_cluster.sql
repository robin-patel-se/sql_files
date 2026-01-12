USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_mvp_dev_robin.snowplow
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream
	CLONE hygiene_vault_mvp.snowplow.event_stream
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification
	CLONE data_vault_mvp.single_customer_view_stg.module_touchification
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_anomalous_user_dates
	CLONE data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchifiable_events
	CLONE data_vault_mvp.single_customer_view_stg.module_touchifiable_events
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_events_of_interest
	CLONE data_vault_mvp.single_customer_view_stg.module_events_of_interest
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.page_screen_enrichment
	CLONE data_vault_mvp.single_customer_view_stg.page_screen_enrichment
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes
	CLONE data_vault_mvp.dwh.user_attributes
;

-- optional statement to create the module target table --
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
	CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.scv.05_touch_basic_attributes.01_module_touch_basic_attributes.py' \
    --method 'run' \
    --start '2024-12-05 00:00:00' \
    --end '2024-12-05 00:00:00'

------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

ALTER TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
	RENAME TO data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_2024_12_09
;


WITH stack_spvs AS (
                SELECT
                    'last non direct'                                               AS attribution_model,
                    COALESCE(
                        tmc.utm_campaign,
                        tba2.app_push_open_context:dataFields:campaignId::VARCHAR
                    )                                                               AS campaign_id,
                    COALESCE(
                        tmc.landing_page_parameters['messageId']::VARCHAR,
                        tba.app_push_open_context:dataFields:messageId::VARCHAR
                    )                                                               AS message_id_coalesce,

                    -- if there is a message id available, prioritise that however we have  found instances
                    -- where the message id is not populated on a given day but have seen events for the
                    -- same campaign with a message id using max partition on the campaign id and date to
                    -- remove nulls and use the message id from another spv for the same campaign on that
                    -- event date
                    COALESCE(
                        message_id_coalesce, -- note this is an aliased field
                        MAX(message_id_coalesce) OVER (
                            PARTITION BY
                                tba.attributed_user_id,
                                campaign_id, -- note this is an aliased field
                                spvs.event_tstamp::DATE
                        )
                    )                                                               AS message_id,
                    spvs.event_tstamp::DATE                                         AS event_date,
                    tba.attributed_user_id                                          AS shiro_user_id
                FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS spvs
                INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS tba
                    ON spvs.touch_id = tba.touch_id
                        AND tba.stitched_identity_type = 'se_user_id'
                INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution  attr
                    ON spvs.touch_id = attr.touch_id
                        AND attr.attribution_model = 'last non direct'
                INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel tmc
                    ON attr.attributed_touch_id = tmc.touch_id
                INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes tba2 ON attr.attributed_touch_id = tba2.touch_id
                        AND tba2.stitched_identity_type = 'se_user_id'
                WHERE
                    (tmc.utm_medium = 'email'
                    AND tmc.utm_campaign IS NOT NULL
                    AND spvs.event_tstamp::DATE >= '2021-11-03'
                )
                OR tba2.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL

                UNION ALL

                SELECT
                    'last click'                                                    AS attribution_model,
                    COALESCE(
                        tmc.utm_campaign,
                        tba.app_push_open_context:dataFields:campaignId::VARCHAR
                    )                                                               AS campaign_id,
                    COALESCE(
                        tmc.landing_page_parameters['messageId']::VARCHAR,
                        tba.app_push_open_context:dataFields:messageId::VARCHAR
                    )                                                               AS message_id_coalesce,

                    -- if there is a message id available, prioritise that however we have  found instances
                    -- where the message id is not populated on a given day but have seen events for the
                    -- same campaign with a message id using max partition on the campaign id and date to
                    -- remove nulls and use the message id from another spv for the same campaign on that
                    -- event date
                    COALESCE(
                        message_id_coalesce, -- note this is an aliased field
                        MAX(message_id_coalesce) OVER (
                            PARTITION BY
                                tba.attributed_user_id,
                                campaign_id, -- note this is an aliased field
                                spvs.event_tstamp::DATE
                        )
                    )                                                               AS message_id,
                    spvs.event_tstamp::DATE                                         AS event_date,
                    tba.attributed_user_id                                          AS shiro_user_id
                FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS spvs
                INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS tba
                    ON spvs.touch_id = tba.touch_id
                    AND tba.stitched_identity_type = 'se_user_id'
                INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel AS tmc
                    ON spvs.touch_id = tmc.touch_id
                WHERE (tmc.utm_medium = 'email'
                AND tmc.utm_campaign IS NOT NULL
                AND spvs.event_tstamp::DATE >= '2021-11-03'
                )
                OR tba.app_push_open_context:dataFields:campaignId::VARCHAR IS NOT NULL

                UNION ALL

                SELECT
                    'url params'                                                    AS attribution_model,
                    PARSE_URL(spvs.page_url)['parameters']:utm_campaign::VARCHAR    AS campaign_id,

                    -- if there is a message id available, prioritise that however we have  found instances
                    -- where the message id is not populated on a given day but have seen events for the
                    -- same campaign with a message id using max partition on the campaign id and date to
                    -- remove nulls and use the message id from another spv for the same campaign on that
                    -- event date
                    COALESCE(
                        PARSE_URL(spvs.page_url)['parameters']:messageId::VARCHAR,
                        MAX(PARSE_URL(spvs.page_url)['parameters']:messageId::VARCHAR) OVER (
                            PARTITION BY
                                campaign_id,
                                tba.attributed_user_id
                            )
                    )                                                               AS message_id,
                    spvs.event_tstamp::DATE                                         AS event_date,
                    tba.attributed_user_id                                          AS shiro_user_id
                FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs AS spvs
                INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS tba
                        ON spvs.touch_id = tba.touch_id
                        AND tba.stitched_identity_type = 'se_user_id'
                WHERE
                    PARSE_URL(spvs.page_url)['parameters']:utm_medium::VARCHAR = 'email'
                    AND PARSE_URL(spvs.page_url)['parameters']:utm_campaign::VARCHAR IS NOT NULL
                    AND spvs.event_tstamp::DATE >='2021-11-03'
            ), attach_send_data AS (
                SELECT
                    ss.attribution_model,
                    ss.campaign_id,
                    ss.message_id,
                    ss.event_date,
                    ss.shiro_user_id,
                    IFF(DATEDIFF(DAY, ms.send_start_date, ss.event_date) <= 1, 1, 0)  AS spvs_1d,
                    IFF(DATEDIFF(DAY, ms.send_start_date, ss.event_date) <= 7, 1, 0)  AS spvs_7d,
                    IFF(DATEDIFF(DAY, ms.send_start_date, ss.event_date) <= 14, 1, 0) AS spvs_14d
                FROM stack_spvs ss
                LEFT JOIN data_vault_mvp_dev_robin.dwh.iterable_crm_reporting__step02__model_sends ms
                    ON COALESCE(ss.message_id, ms.message_id) = ms.message_id
                    AND ss.campaign_id = ms.campaign_id::VARCHAR
                    AND ss.shiro_user_id = ms.shiro_user_id
                    AND ss.event_date BETWEEN ms.send_start_date AND ms.send_end_date
            )
            SELECT
                asd.attribution_model,
                asd.campaign_id,
                asd.message_id,
                asd.event_date,
                asd.shiro_user_id,
                COUNT(*)            AS spvs,
                SUM(asd.spvs_1d)    AS spvs_1d,
                SUM(asd.spvs_7d)    AS spvs_7d,
                SUM(asd.spvs_14d)   AS spvs_14d
            FROM attach_send_data asd
            GROUP BY 1, 2, 3, 4, 5;

-- checking table is still accessible during run
SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
-- it is :)


USE ROLE pipelinerunner;

ALTER TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes RENAME TO data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_2024_12_09;


ALTER TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes_2024_12_09 SWAP WITH data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;


ALTER TABLE  data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes RESUME RECLUSTER;


SELECT * FROM se.data.scv_touch_basic_attributes stba WHERE stba.touch_start_tstamp >= current_date -2