SELECT
    column1,
    column1 REGEXP
    '(52\\.205\\.190\\.([1-9]|[1-9][0-9]|[1-2][0-5][0-4]))|(35\\.245\\.67\\.(225|226))|(34\\.145\\.238\\.([1-9]|[1-9][0-9]|[1-2][0-5][0-4]))' AS test,
    column2                                                                                                                                   AS valid
FROM
VALUES ('52.205.190.1', TRUE),
       ('52.205.190.100', TRUE),
       ('52.205.190.254', TRUE),
       ('52.205.190.256', FALSE),

       ('35.245.67.225', TRUE),
       ('35.245.67.226', TRUE),

       ('34.145.238.1', TRUE),
       ('34.145.238.100', TRUE),
       ('34.145.238.254', TRUE),
       ('34.145.238.256', FALSE)
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touched_spvs CLONE data_vault_mvp.single_customer_view_stg.module_touched_spvs;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2023-03-15 00:00:00' --end '2023-03-15 00:00:00'


-- post dep check

-- checking sessions that have ip addresses within synack ranges to see if their territory has been correctly identified
SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
WHERE user_ipaddress REGEXP
      '(52\\.205\\.190\\.([1-9]|[1-9][0-9]|[1-2][0-5][0-4]))|(35\\.245\\.67\\.(225|226))|(34\\.145\\.238\\.([1-9]|[1-9][0-9]|[1-2][0-5][0-4]))'


-- checking the user id of identified ip addresses, then cross referencing them in the cms to see if they are synack users
SELECT *
FROM snowplow.atomic.events e
WHERE e.user_ipaddress REGEXP
      '(52\\.205\\.190\\.([1-9]|[1-9][0-9]|[1-2][0-5][0-4]))|(35\\.245\\.67\\.(225|226))|(34\\.145\\.238\\.([1-9]|[1-9][0-9]|[1-2][0-5][0-4]))'
  AND e.collector_tstamp::DATE >= '2023-01-01';


-- checking ip addresses of a known synack user
SELECT *
FROM snowplow.atomic.events e
WHERE e.user_id = '79403262'
  AND e.collector_tstamp::DATE >= '2023-01-01';

