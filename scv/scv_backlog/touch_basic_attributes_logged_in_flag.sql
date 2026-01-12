CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;

self_describing_task --include 'dv/dwh/events/04_touch_basic_attributes/01_module_touch_basic_attributes.py'  --method 'run' --start '2020-07-24 00:00:00' --end '2020-07-24 00:00:00'

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp >= '2020-07-22'
LIMIT 100;

SELECT mt.touch_id,
       CASE WHEN MAX(es.se_user_id) IS NOT NULL THEN TRUE ELSE FALSE END AS touch_logged_in
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mt.event_hash = es.event_hash
GROUP BY 1
LIMIT 10;


DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification mt
WHERE mt.updated_at < CURRENT_DATE - 1;

DELETE
FROM hygiene_vault_mvp_dev_robin.snowplow.event_stream
WHERE updated_at < current_date - 2;
DELETE
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
WHERE updated_at < current_date - 2;




SELECT * FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba;


------------------------------------------------------------------------------------------------------------------------


USE WAREHOUSE pipe_xlarge;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_bkup clone data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.update_logged_in_status AS
SELECT mt.touch_id,
       CASE WHEN MAX(es.se_user_id) IS NOT NULL THEN TRUE ELSE FALSE END AS touch_logged_in
FROM data_vault_mvp.single_customer_view_stg.module_touchification mt
         INNER JOIN hygiene_vault_mvp.snowplow.event_stream es ON mt.event_hash = es.event_hash
GROUP BY 1
;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone
(
    -- (lineage) metadata for the current job
    schedule_tstamp TIMESTAMP,
    run_tstamp TIMESTAMP,
    operation_id VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,

    --touch metrics
    touch_id VARCHAR,
    attributed_user_id VARCHAR,
    stitched_identity_type VARCHAR,
    touch_logged_in BOOLEAN,
    touch_start_tstamp TIMESTAMP,
    touch_end_tstamp TIMESTAMP,
    touch_duration_seconds NUMBER,
    touch_posa_territory VARCHAR,
    touch_hostname_territory VARCHAR,
    touch_experience VARCHAR,
    touch_landing_page VARCHAR,
    touch_landing_pagepath VARCHAR,
    touch_hostname VARCHAR,
    touch_exit_pagepath VARCHAR,
    touch_referrer_url VARCHAR,
    touch_event_count NUMBER,
    touch_has_booking BOOLEAN,

    --user metics
    user_ipaddress VARCHAR,
    geo_country VARCHAR,
    geo_city VARCHAR,
    geo_zipcode VARCHAR,
    geo_latitude DOUBLE,
    geo_longitude DOUBLE,
    geo_region_name VARCHAR,
    useragent VARCHAR,
    br_name VARCHAR,
    br_family VARCHAR,
    os_name VARCHAR,
    os_family VARCHAR,
    os_manufacturer VARCHAR,
    dvce_screenwidth NUMBER,
    dvce_screenheight NUMBER
)
CLUSTER BY (TOUCH_START_TSTAMP::DATE, TOUCH_ID);

TRUNCATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;

INSERT INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
SELECT mtba.schedule_tstamp,
       mtba.run_tstamp,
       mtba.operation_id,
       mtba.created_at,
       mtba.updated_at,
       mtba.touch_id,
       mtba.attributed_user_id,
       mtba.stitched_identity_type,
       mtbal.touch_logged_in,
       mtba.touch_start_tstamp,
       mtba.touch_end_tstamp,
       mtba.touch_duration_seconds,
       mtba.touch_posa_territory,
       mtba.touch_hostname_territory,
       mtba.touch_experience,
       mtba.touch_landing_page,
       mtba.touch_landing_pagepath,
       mtba.touch_hostname,
       mtba.touch_exit_pagepath,
       mtba.touch_referrer_url,
       mtba.touch_event_count,
       mtba.touch_has_booking,
       mtba.user_ipaddress,
       mtba.geo_country,
       mtba.geo_city,
       mtba.geo_zipcode,
       mtba.geo_latitude,
       mtba.geo_longitude,
       mtba.geo_region_name,
       mtba.useragent,
       mtba.br_name,
       mtba.br_family,
       mtba.os_name,
       mtba.os_family,
       mtba.os_manufacturer,
       mtba.dvce_screenwidth,
       mtba.dvce_screenheight
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
LEFT JOIN data_vault_mvp_dev_robin.single_customer_view_stg.update_logged_in_status mtbal ON mtba.touch_id = mtbal.touch_id;

DROP TABLE data_vault_mvp.single_customer_view_stg.update_logged_in_status;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes clone data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_clone;

SELECT * FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba;