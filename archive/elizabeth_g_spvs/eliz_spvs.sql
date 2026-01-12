USE WAREHOUSE pipe_xlarge;

SELECT b.touch_start_tstamp::DATE                              AS date,
       b.touch_hostname,
       COUNT(DISTINCT b.touch_id)                              AS sessions,
       COUNT(DISTINCT b.attributed_user_id_hash)               AS users,
       SUM(CASE WHEN t.touch_id IS NOT NULL THEN 1 ELSE 0 END) AS spvs
FROM se.data.scv_touch_basic_attributes b
         INNER JOIN se.data.scv_touch_marketing_channel c
                    ON b.touch_id = c.touch_id
         LEFT JOIN se.data.scv_touched_spvs t ON b.touch_id = t.touch_id
         LEFT JOIN se.data.dim_sale ds ON ds.sale_id = t.se_sale_id
WHERE b.touch_start_tstamp::DATE = '2020-01-29'
  AND ds.product_line IS NULL
GROUP BY 1, 2;


SELECT b.touch_start_tstamp::DATE,
       c.touch_mkt_channel,
       COUNT(DISTINCT b.touch_id)                                AS sessions,
       COUNT(CASE WHEN s.touch_id IS NOT NULL THEN 1 ELSE 0 END) AS spvs
FROM se.data.scv_touch_basic_attributes b
         INNER JOIN se.data.scv_touch_marketing_channel c ON b.touch_id = c.touch_id
         LEFT JOIN se.data.scv_touched_spvs s ON b.touch_id = s.touch_id
         LEFT JOIN se.data.dim_sale ds ON s.se_sale_id = ds.sale_id
WHERE b.touch_start_tstamp::DATE = '2020-03-01'
  AND ds.product_line IS NULL
GROUP BY 1, 2;

SELECT b.touch_start_tstamp::DATE,
       c.touch_mkt_channel,
       ds.product_line,
       COUNT(DISTINCT b.touch_id)                                AS sessions,
       COUNT(CASE WHEN s.touch_id IS NOT NULL THEN 1 ELSE 0 END) AS spvs
FROM se.data.scv_touch_basic_attributes b
         INNER JOIN se.data.scv_touch_marketing_channel c ON b.touch_id = c.touch_id
         INNER JOIN se.data.scv_touched_spvs s ON b.touch_id = s.touch_id
         LEFT JOIN se.data.dim_sale ds ON s.se_sale_id = ds.sale_id
WHERE b.touch_start_tstamp::DATE = '2020-03-01'
GROUP BY 1, 2, 3;

SELECT *
FROM se.data.scv_touched_spvs
WHERE event_tstamp::DATE = '2020-01-29'
  AND se_sale_id IS NULL;



SELECT s.se_sale_id
FROM se.data.scv_touch_basic_attributes b
         INNER JOIN se.data.scv_touch_marketing_channel c ON b.touch_id = c.touch_id
         LEFT JOIN se.data.scv_touched_spvs s ON b.touch_id = s.touch_id
         LEFT JOIN se.data.dim_sale ds ON s.se_sale_id = ds.sale_id
WHERE b.touch_start_tstamp::DATE = '2020-03-01'
  AND s.se_sale_id IS NOT NULL
  AND ds.sale_id IS NULL;

SELECT *
FROM se.data.dim_sale
WHERE product_line = 'Catalogue';


------------------------------------------------------------------------------------------------------------------------

SELECT b.touch_start_tstamp::DATE                              AS date,
       ds.product_line,
       ds.product_type,
       c.touch_mkt_channel,
       b.touch_experience,
       c.touch_affiliate_territory,
       c.touch_hostname_territory,
       b.touch_hostname,
       COUNT(DISTINCT b.touch_id)                              AS sessions,
       COUNT(DISTINCT b.attributed_user_id_hash)                    AS users,
       SUM(CASE WHEN t.touch_id IS NOT NULL THEN 1 ELSE 0 END) AS SPVs
FROM se.data.SCV_touch_basic_attributes b
         INNER JOIN se.data.SCV_touch_marketing_channel c
                    ON b.touch_id = c.touch_id
         INNER JOIN se.data.SCV_touched_spvs t ON b.touch_id = t.touch_id
         LEFT JOIN se.data.dim_sale ds ON ds.sale_id=t.se_sale_id
WHERE b.touch_start_tstamp BETWEEN '2020-02-29' AND '2020-03-03'
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY 1, 2, 3, 4, 5, 6, 7;