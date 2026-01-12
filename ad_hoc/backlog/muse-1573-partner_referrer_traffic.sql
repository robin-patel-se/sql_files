USE WAREHOUSE pipe_xlarge;

SELECT *
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2021-12-01'
  AND stmc.affiliate = 'up_se_open';


SELECT *
FROM se.data.se_affiliate sa
WHERE sa.url_string = 'up_se_open';

SELECT stmc.affiliate,
       DATE_TRUNC(MONTH, stba.touch_start_tstamp) AS month,
       COUNT(DISTINCT stba.touch_id)              AS sessions
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2020-01-01' --open the start date paramter here
  AND stmc.affiliate IN
      ('up_se_open')                          --add additional affiliate url_strings here
GROUP BY 1, 2;


SELECT stmc.affiliate,
       DATE_TRUNC(YEAR, stba.touch_start_tstamp) AS year,
       COUNT(DISTINCT stba.touch_id)             AS sessions
FROM se.data.scv_touch_basic_attributes stba
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE stba.touch_start_tstamp >= '2020-01-01' --open the start date paramter here
  AND stmc.affiliate IN
      ('up_se_open')                          --add additional affiliate url_strings here
GROUP BY 1, 2;