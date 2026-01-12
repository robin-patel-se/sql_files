--look at sessions and user counts since jan 2020 to see if spike on 16th apr

ALTER SESSION SET week_start = 5;
SELECT date_trunc(WEEK, touch_start_tstamp) AS week,
       count(*)                             AS sessions,
       count(DISTINCT attributed_user_id)   AS users
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes
WHERE stitched_identity_type = 'se_user_id'
  AND touch_start_tstamp >= '2020-01-01'
  AND touch_hostname_territory = 'DE'
GROUP BY 1;

SELECT b.touch_hostname_territory,
       count(DISTINCT e.se_user_id) AS logged_in_users
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
         LEFT JOIN data_vault_mvp.single_customer_view_stg.module_touchification t ON b.touch_id = t.touch_id
         LEFT JOIN hygiene_vault_mvp.snowplow.event_stream e ON t.event_hash = e.event_hash
WHERE e.event_name IN ('page_view', 'screen_view')
  AND e.event_tstamp >= '2020-04-10'
  AND e.event_tstamp <= '2020-04-16'
  AND b.touch_hostname_territory IN ('UK', 'DE', 'IT')
  AND e.se_user_id IS NOT NULL
GROUP BY 1;


SELECT b.touch_hostname_territory,
       count(DISTINCT CASE WHEN b.stitched_identity_type = 'se_user_id' THEN b.attributed_user_id END)  AS logged_in_users,
       count(DISTINCT CASE WHEN b.stitched_identity_type != 'se_user_id' THEN b.attributed_user_id END) AS logged_out_users
FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes b
WHERE b.touch_hostname_territory IN ('UK', 'DE', 'IT')
  AND b.touch_start_tstamp >= '2020-04-10'
  AND b.touch_start_tstamp <= '2020-04-16'
GROUP BY 1;