USE WAREHOUSE pipe_large;

SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream;

SELECT touch_start_tstamp::DATE,
       touch_mkt_channel,
       COUNT(*)
FROM se.data.scv_touch_basic_attributes b
         INNER JOIN se.data.scv_touch_marketing_channel c ON b.touch_id = c.touch_id
WHERE touch_start_tstamp::DATE = '2020-04-01'
GROUP BY 1, 2;


SELECT *
FROM se.data.scv_touch_attribution;


SELECT touch_start_tstamp::DATE,
       touch_mkt_channel AS last_mkt_channel,
       COUNT(*)
FROM se.data.scv_touch_basic_attributes b
         INNER JOIN se.data.scv_touch_attribution a
                    ON b.touch_id = a.touch_id AND a.attribution_model = 'last non direct'
         INNER JOIN se.data.scv_touch_marketing_channel c ON a.attributed_touch_id = c.touch_id
WHERE touch_start_tstamp::DATE = '2020-04-01'
GROUP BY 1, 2;


SELECT s.sale_type,
       COUNT(*)
FROM se.data.scv_touched_spvs t
LEFT JOIN se.data.dim_sale s ON t.se_sale_id = s.sale_id
WHERE event_tstamp::DATE = '2020-04-01'
GROUP BY 1
;

SELECT