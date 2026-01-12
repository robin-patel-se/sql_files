SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream
LIMIT 15;

SELECT user_ipaddress,
       geo_country,
       geo_city,
       geo_zipcode,
       geo_latitude,
       geo_longitude,
       geo_region_name,
       useragent,
       br_name,
       br_family,
       os_name,
       os_family,
       os_manufacturer,
       dvce_screenwidth,
       dvce_screenheight
FROM hygiene_vault_mvp.snowplow.event_stream
;

--             user_ipaddress           VARCHAR,
--             geo_country              VARCHAR,
--             geo_city                 VARCHAR,
--             geo_zipcode              VARCHAR,
--             geo_latitude             DOUBLE,
--             geo_longitude            DOUBLE,
--             geo_region_name          VARCHAR,
--             useragent                VARCHAR,
--             br_name                  VARCHAR,
--             br_family                VARCHAR,
--             os_name                  VARCHAR,
--             os_family                VARCHAR,
--             os_manufacturer          VARCHAR,
--             dvce_screenwidth         NUMBER,
--             dvce_screenheight        NUMBER

------------------------------------------------------------------------------------------------------------------------
self_describing_task --include 'dv/dwh_rec/events/04_touch_basic_attributes/01_module_touch_basic_attributes'  --method 'run' --start '2020-04-29 00:00:00' --end '2020-04-29 00:00:00'

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN user_ipaddress VARCHAR;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN geo_country VARCHAR;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN geo_city VARCHAR;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN geo_zipcode VARCHAR;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN geo_latitude DOUBLE;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN geo_longitude DOUBLE;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN geo_region_name VARCHAR;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN useragent VARCHAR;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN br_name VARCHAR;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN br_family VARCHAR;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN os_name VARCHAR;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN os_family VARCHAR;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN os_manufacturer VARCHAR;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN dvce_screenwidth NUMBER;
ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes
    ADD COLUMN dvce_screenheight NUMBER;



------------------------------------------------------------------------------------------------------------------------
--to update historic data

SELECT min(updated_at)
FROM data_vault_mvp.single_customer_view_stg.module_touchification; --2020-02-28 17:06:45.849000000

DROP TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.snowplow.event_stream CLONE hygiene_vault_mvp.snowplow.event_stream;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification CLONE data_vault_mvp.single_customer_view_stg.module_touchification;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes_bkup CLONE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;



------------------------------------------------------------------------------------------------------------------------
--to update history
USE WAREHOUSE pipe_xlarge;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.single_customer_view_stg.update_touch_basic_attributes AS (
    SELECT DISTINCT t.touch_id,
                    --ignore nulls in window functions are to cater for future status where we might also have other
                    --events that might occur slightly before a landing page, eg. email events.
                    FIRST_VALUE(e.user_ipaddress)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS user_ipaddress,
                    FIRST_VALUE(e.geo_country)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS geo_country,
                    FIRST_VALUE(e.geo_city)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS geo_city,
                    FIRST_VALUE(e.geo_zipcode)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS geo_zipcode,
                    FIRST_VALUE(e.geo_latitude)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS geo_latitude,
                    FIRST_VALUE(e.geo_longitude)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS geo_longitude,
                    FIRST_VALUE(e.geo_region_name)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS geo_region_name,
                    FIRST_VALUE(e.useragent)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS useragent,
                    FIRST_VALUE(e.br_name)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS br_name,
                    FIRST_VALUE(e.br_family)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS br_family,
                    FIRST_VALUE(e.os_name)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS os_name,
                    FIRST_VALUE(e.os_family)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS os_family,
                    FIRST_VALUE(e.os_manufacturer)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS os_manufacturer,
                    FIRST_VALUE(e.dvce_screenwidth)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS dvce_screenwidth,
                    FIRST_VALUE(e.dvce_screenheight)
                                IGNORE NULLS OVER (PARTITION BY t.touch_id ORDER BY t.event_index_within_touch) AS dvce_screenheight

    FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touchification t
             INNER JOIN hygiene_vault_mvp_dev_robin.snowplow.event_stream e ON t.event_hash = e.event_hash
);

UPDATE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes AS target
SET target.user_ipaddress    = batch.user_ipaddress,
    target.geo_country       = batch.geo_country,
    target.geo_city          = batch.geo_city,
    target.geo_zipcode       = batch.geo_zipcode,
    target.geo_latitude      = batch.geo_latitude,
    target.geo_longitude     = batch.geo_longitude,
    target.geo_region_name   = batch.geo_region_name,
    target.useragent         = batch.useragent,
    target.br_name           = batch.br_name,
    target.br_family         = batch.br_family,
    target.os_name           = batch.os_name,
    target.os_family         = batch.os_family,
    target.os_manufacturer   = batch.os_manufacturer,
    target.dvce_screenwidth  = batch.dvce_screenwidth,
    target.dvce_screenheight = batch.dvce_screenheight

FROM data_vault_mvp_dev_robin.single_customer_view_stg.update_touch_basic_attributes AS batch
WHERE target.touch_id = batch.touch_id;

SELECT * FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;

--run on prod
CREATE OR REPLACE TABLE data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes CLONE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes;



SELECT c.touch_affiliate_territory,
       COUNT(*) AS sessions
FROM se.data.scv_touch_basic_attributes b
         INNER JOIN se.data.scv_touch_marketing_channel c ON b.touch_id = c.touch_id
WHERE b.touch_start_tstamp::DATE = '2020-05-04'
GROUP BY 1;

SELECT * FROM se.data.scv_touch_basic_attributes;


SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes;

