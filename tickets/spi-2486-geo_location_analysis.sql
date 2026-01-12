USE WAREHOUSE pipe_large;
ALTER SESSION SET QUERY_TAG = 'geo location analysis';

SELECT
    stba.geo_country,
    CASE
        WHEN stba.geo_country IN ('GB', 'IE') THEN 'UK'
        ELSE stba.geo_country
        END AS adjusted_geo_country,
    sua.current_affiliate_territory
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.se_user_attributes sua ON stba.attributed_user_id_hash = SHA2(sua.shiro_user_id)
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= '2022-06-01'
;

--sessions since june 2022: 12M (12002022)

WITH session_geo AS (
    SELECT
        stba.geo_country,
        CASE
            WHEN stba.geo_country IN ('GB', 'IE') THEN 'UK'
            ELSE stba.geo_country
            END AS adjusted_geo_country,
        sua.current_affiliate_territory
    FROM se.data.scv_touch_basic_attributes stba
        INNER JOIN se.data.se_user_attributes sua ON stba.attributed_user_id_hash = SHA2(sua.shiro_user_id)
    WHERE stba.stitched_identity_type = 'se_user_id'
      AND stba.touch_start_tstamp >= '2022-06-01'

)
SELECT
    sg.adjusted_geo_country,
    sg.current_affiliate_territory,
    COUNT(*)
FROM session_geo sg
WHERE sg.current_affiliate_territory != sg.adjusted_geo_country
GROUP BY 1, 2
;
--3,618,871


------------------------------------------------------------------------------------------------------------------------
-- since functionality went live

SELECT
    stba.geo_country,
    CASE
        WHEN stba.geo_country IN ('GB', 'IE') THEN 'UK'
        ELSE stba.geo_country
        END AS adjusted_geo_country,
    sua.current_affiliate_territory
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.se_user_attributes sua ON stba.attributed_user_id_hash = SHA2(sua.shiro_user_id)
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= '2022-07-07'
;
--1.7M (1750234)

-- eye ball sessions
SELECT
    sua.shiro_user_id,
    sua.original_affiliate_territory,
    sua.original_affiliate_name,
    stmc.touch_affiliate_territory,
    stba.touch_start_tstamp,
    stba.geo_country,
    stba.user_ipaddress,
    stba.useragent,
    CASE
        WHEN stba.geo_country IN ('GB', 'IE') THEN 'UK'
        ELSE stba.geo_country
        END AS adjusted_geo_country,
    sua.current_affiliate_territory,
    stba.touch_hostname,
    stba.stitched_identity_type
FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= '2022-07-07'
  AND stba.geo_country != sua.current_affiliate_territory
  AND stba.touch_hostname_territory NOT IN ('SE TECH');


-- break down by original affiliate
WITH session_geo AS (
    SELECT
        sua.shiro_user_id,
        sua.original_affiliate_territory,
        sua.original_affiliate_name,
        stba.geo_country,
        CASE
            WHEN stba.geo_country IN ('GB', 'IE') THEN 'UK'
            ELSE stba.geo_country
            END AS adjusted_geo_country,
        sua.current_affiliate_territory,
        stmc.touch_affiliate_territory,
        stba.touch_hostname,
        stba.stitched_identity_type
    FROM se.data.scv_touch_basic_attributes stba
        INNER JOIN se.data.se_user_attributes sua ON stba.attributed_user_id_hash = SHA2(sua.shiro_user_id)
        INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
    WHERE stba.stitched_identity_type = 'se_user_id'
      AND stba.touch_start_tstamp >= '2022-07-08'
      AND stba.geo_country != sua.current_affiliate_territory
      AND stba.touch_hostname_territory NOT IN ('SE TECH')
)
SELECT
    sg.current_affiliate_territory,
    sg.touch_affiliate_territory,
    sg.geo_country,
    COUNT(*)
FROM session_geo sg
WHERE sg.current_affiliate_territory != sg.adjusted_geo_country
GROUP BY 1, 2, 3
;

SELECT
    sua.shiro_user_id,
    sua.original_affiliate_territory,
    sua.original_affiliate_name,
    stmc.touch_affiliate_territory,
    stba.touch_start_tstamp,
    stba.geo_country,
    stba.user_ipaddress,
    stba.useragent,
    CASE
        WHEN stba.geo_country IN ('GB', 'IE') THEN 'UK'
        ELSE stba.geo_country
        END AS adjusted_geo_country,
    sua.current_affiliate_territory,
    stba.touch_hostname,
    stba.stitched_identity_type
FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= '2022-07-07'
  AND stba.geo_country != sua.current_affiliate_territory
  AND stba.geo_country = 'IE';



SELECT
    sua.original_affiliate_territory,
    sua.current_affiliate_territory,
    stmc.touch_affiliate_territory,
    stba.geo_country,
    stba.*
FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= '2022-07-07'
  AND stba.geo_country != sua.current_affiliate_territory
  AND stba.geo_country = 'IE'


SELECT
    stba.user_ipaddress,
    COUNT(*)
FROM se.data_pii.scv_touch_basic_attributes stba
    INNER JOIN se.data.se_user_attributes sua ON TRY_TO_NUMBER(stba.attributed_user_id) = sua.shiro_user_id
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.stitched_identity_type = 'se_user_id'
  AND stba.touch_start_tstamp >= '2022-07-07'
  AND stba.geo_country != sua.current_affiliate_territory
  AND stba.geo_country = 'IE'
GROUP BY 1
ORDER BY 2 DESC;








