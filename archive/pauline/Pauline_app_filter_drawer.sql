--pauline
SELECT es.event_tstamp,
       es.se_category,
       es.se_action,
       es.se_label,
       REGEXP_REPLACE(es.se_label, 'filter drawer ') as se_label_stripped
--        PARSE_JSON('{'|| RIGHT(LEFT(se_label_stripped, LENGTH(se_label_stripped) - 1), LENGTH(se_label_stripped) - 2) || '}') as parsed
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.device_platform LIKE 'native app%'
  AND es.event_tstamp >= current_date - 10
  AND es.se_category = 'filtering'
  AND es.se_label LIKE 'filter drawer%';

SET se_label = '["locations": "Paris, France", "types": [], "saleSatus": "CURRENT", "yearsMonths": [], "experiences": []]';

SELECT length($se_label) AS se_label_length,
       PARSE_JSON('{'|| RIGHT(LEFT($se_label, se_label_length - 1), se_label_length - 2) || '}');




