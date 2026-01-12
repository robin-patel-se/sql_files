CREATE SCHEMA data_vault_mvp_dev_robin.bi;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.bi.branch_purchase_events CLONE data_vault_mvp.bi.branch_purchase_events;


SELECT *
FROM data_vault_mvp.bi.branch_purchase_events;


SELECT
    event_hash,
    touch_id,
    event_category,
    event_subcategory,
    unstruct_event_com_branch_secretescapes_purchase_1,
    unstruct_event_com_branch_secretescapes_purchase_1:event_data:transaction_id::VARCHAR AS transaction_id,
    attributed,
    content_items,
    custom_data,        -- may contain PII
    customer_event_alias,
    days_from_last_attributed_touch_to_event,
    deep_linked,
    event_data,         -- may contain PII
    event_days_from_timestamp,
    DATEADD('ms', event_timestamp, '1970-01-01')::DATE                                    AS booking_event_date,
    existing_user,
    first_event_for_user,
    id,
    install_activity,
    last_attributed_touch_data,
    last_attributed_touch_timestamp,
    last_attributed_touch_type,
    last_cta_view_data, -- may contain PII
    campaign                                                                              AS last_cta_view_data_campaign,
    channel                                                                               AS last_cta_view_data_channel,
    last_cta_view_timestamp,
    name,
    origin,
    reengagement_activity,
    seconds_from_install_to_event,
    purchase_timestamp,
    user_data,          -- may contain PII
    fact_booking.margin_gross_of_toms_gbp_constant_currency                               AS branch_margin,
    advertising_partner_name                                                              AS last_attributed_advertising_partner_name,
    last_attributed_touch_data:"~campaign"                                                AS last_attributed_campaign,
    last_attributed_touch_data:"~channel"                                                 AS last_attributed_channel


FROM se.data_pii.scv_branch_purchase_events branch_purchase_events
    LEFT JOIN se.data.fact_booking fact_booking
              ON unstruct_event_com_branch_secretescapes_purchase_1:event_data:transaction_id::VARCHAR = fact_booking.transaction_id
WHERE branch_purchase_events.last_cta_view_data IS NOT NULL;


CREATE SCHEMA hygiene_vault_mvp_dev_robin.snowplow;
CREATE OR REPLACE VIEW hygiene_vault_mvp_dev_robin.snowplow.event_stream AS
SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream;

CREATE SCHEMA data_vault_mvp_dev_robin.dwh;
CREATE SCHEMA data_vault_mvp_dev_robin.single_customer_view_stg;

CREATE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions CLONE data_vault_mvp.single_customer_view_stg.module_touched_transactions;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;



self_describing_task --include 'biapp/task_catalogue/dv/bi/scv/branch_purchase_events.py'  --method 'run' --start '2022-10-12 00:00:00' --end '2022-10-12 00:00:00'

SELECT
    touch_id_match.event_hash                                                                                                 AS event_hash,
    touch_id_match.touch_id                                                                                                   AS touch_id,
    touch_id_match.event_tstamp                                                                                               AS event_tstamp,
    'purchase event'                                                                                                          AS event_category,
    'branch purchase event'                                                                                                   AS event_subcategory,
    events.unstruct_event_com_branch_secretescapes_purchase_1::OBJECT                                                         AS unstruct_event_com_branch_secretescapes_purchase_1,
    events.unstruct_event_com_branch_secretescapes_purchase_1:attributed::BOOLEAN                                             AS attributed,
    events.unstruct_event_com_branch_secretescapes_purchase_1:content_items::ARRAY                                            AS content_items,
    events.unstruct_event_com_branch_secretescapes_purchase_1:custom_data::OBJECT                                             AS custom_data,
    events.unstruct_event_com_branch_secretescapes_purchase_1:customer_event_alias::VARCHAR                                   AS customer_event_alias,
    events.unstruct_event_com_branch_secretescapes_purchase_1:days_from_last_attributed_touch_to_event::INTEGER               AS days_from_last_attributed_touch_to_event,
    events.unstruct_event_com_branch_secretescapes_purchase_1:deep_linked::BOOLEAN                                            AS deep_linked,
    events.unstruct_event_com_branch_secretescapes_purchase_1:event_data::OBJECT                                              AS event_data,
    events.unstruct_event_com_branch_secretescapes_purchase_1:event_data:transaction_id::VARCHAR                              AS transaction_id,
    events.unstruct_event_com_branch_secretescapes_purchase_1:event_days_from_timestamp::INTEGER                              AS event_days_from_timestamp,
    DATEADD('ms', events.unstruct_event_com_branch_secretescapes_purchase_1:event_timestamp::BIGINT, '1970-01-01')            AS event_timestamp,
    events.unstruct_event_com_branch_secretescapes_purchase_1:existing_user::BOOLEAN                                          AS existing_user,
    events.unstruct_event_com_branch_secretescapes_purchase_1:first_event_for_user::BOOLEAN                                   AS first_event_for_user,
    events.unstruct_event_com_branch_secretescapes_purchase_1:id::BIGINT                                                      AS id,
    events.unstruct_event_com_branch_secretescapes_purchase_1:install_activity::OBJECT                                        AS install_activity,
    events.unstruct_event_com_branch_secretescapes_purchase_1:last_attributed_touch_data::OBJECT                              AS last_attributed_touch_data,
    events.unstruct_event_com_branch_secretescapes_purchase_1:last_attributed_touch_data:"~advertising_partner_name"::VARCHAR AS last_attributed_advertising_partner_name,
    events.unstruct_event_com_branch_secretescapes_purchase_1:last_attributed_touch_timestamp::BIGINT                         AS last_attributed_touch_timestamp,
    events.unstruct_event_com_branch_secretescapes_purchase_1:last_attributed_touch_type::VARCHAR                             AS last_attributed_touch_type,
    events.unstruct_event_com_branch_secretescapes_purchase_1:last_attributed_touch_data:"~campaign"::VARCHAR                 AS last_attributed_campaign,
    events.unstruct_event_com_branch_secretescapes_purchase_1:last_attributed_touch_data:"~channel"::VARCHAR                  AS last_attributed_channel events.unstruct_event_com_branch_secretescapes_purchase_1:last_cta_view_data::OBJECT                            AS last_cta_view_data, events.unstruct_event_com_branch_secretescapes_purchase_1:last_cta_view_data:"~campaign"::VARCHAR AS last_cta_view_data_campaign,
    events.unstruct_event_com_branch_secretescapes_purchase_1:last_cta_view_data:"~channel"::VARCHAR                          AS last_cta_view_data_channel,
    events.unstruct_event_com_branch_secretescapes_purchase_1:last_cta_view_timestamp::BIGINT                                 AS last_cta_view_timestamp,
    events.unstruct_event_com_branch_secretescapes_purchase_1:name::VARCHAR                                                   AS name,
    events.unstruct_event_com_branch_secretescapes_purchase_1:origin::VARCHAR                                                 AS origin,
    events.unstruct_event_com_branch_secretescapes_purchase_1:reengagement_activity::OBJECT                                   AS reengagement_activity,
    events.unstruct_event_com_branch_secretescapes_purchase_1:seconds_from_install_to_event::BIGINT                           AS seconds_from_install_to_event,
    events.unstruct_event_com_branch_secretescapes_purchase_1:timestamp::BIGINT                                               AS purchase_timestamp,
    events.unstruct_event_com_branch_secretescapes_purchase_1:user_data::OBJECT                                               AS user_data


FROM data_vault_mvp_dev_robin.bi.branch_purchase_events__step01__get_touch_id_booking_transaction_match touch_id_match
    INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream events
               ON touch_id_match.transaction_id = events.unstruct_event_com_branch_secretescapes_purchase_1:event_data:transaction_id::VARCHAR

WHERE events.unstruct_event_com_branch_secretescapes_purchase_1 IS NOT NULL
  AND events.collector_tstamp >= TIMESTAMPADD('day', -1, '2022-10-11 03:00:00'::TIMESTAMP)

  -- branch purchase events can fire x2, for an example see event_hash(s) being the same event:
  -- 'e641b490ca1a70c743858f2668c4aad21c3b8d8cd199920bf1fa587bc58920af', '08f5657c5eace13e0ec967faefe72515416f190cefa7080e6c64369136c817df'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY touch_id_match.event_hash ORDER BY touch_id_match.event_hash) = 1



DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_transactions;
DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification;
DROP TABLE data_vault_mvp_dev_robin.bi.branch_purchase_events;


SELECT *
FROM data_vault_mvp_dev_robin.bi.branch_purchase_events;
USE WAREHOUSE pipe_xlarge;

SELECT
    MIN(event_tstamp)
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE unstruct_event_com_branch_secretescapes_purchase_1 IS NOT NULL;

-- backfill from 2021-10-26 10:29:01.700000000

SELECT
    sbpe.event_hash,
    sbpe.touch_id,
    sbpe.event_tstamp,
    sbpe.event_category,
    sbpe.event_subcategory,
    sbpe.unstruct_event_com_branch_secretescapes_purchase_1,
    sbpe.attributed,
    sbpe.content_items,
    sbpe.custom_data,
    sbpe.custom_data_margin,
    sbpe.custom_data_nights,
    sbpe.customer_event_alias,
    sbpe.days_from_last_attributed_touch_to_event,
    sbpe.deep_linked,
    sbpe.event_data,
    sbpe.transaction_id,
    sbpe.event_days_from_timestamp,
    sbpe.branch_event_timestamp,
    sbpe.existing_user,
    sbpe.first_event_for_user,
    sbpe.id,
    sbpe.install_activity,
    sbpe.last_attributed_touch_data,
    sbpe.last_attributed_advertising_partner_name,
    sbpe.last_attributed_touch_timestamp,
    sbpe.last_attributed_touch_type,
    sbpe.last_attributed_campaign,
    sbpe.last_attributed_channel,
    sbpe.last_cta_view_data,
    sbpe.last_cta_view_timestamp,
    sbpe.last_cta_view_data_campaign,
    sbpe.last_cta_view_data_channel,
    sbpe.name,
    sbpe.origin,
    sbpe.reengagement_activity,
    sbpe.seconds_from_install_to_event,
    sbpe.purchase_timestamp,
    sbpe.user_data
FROM se.data_pii.scv_branch_purchase_events sbpe;

USE WAREHOUSE dbt_pipe_medium;
USE ROLE dbt_production;
