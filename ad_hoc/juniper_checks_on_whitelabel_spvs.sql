SELECT *
FROM snowplow.atomic.events
WHERE derived_tstamp::DATE = '2025-09-25'
  AND user_id = '67970160' -- robin who clicked a juniper deal
ORDER BY derived_tstamp
;


SELECT *
FROM snowplow.atomic.events
WHERE derived_tstamp::DATE = '2025-09-25'
  AND user_id = '31864730' -- random person who clicked a wrd deal
ORDER BY derived_tstamp
;



SELECT
	es.derived_tstamp,
	es.user_id
FROM hygiene_vault_mvp.snowplow.event_stream es



SELECT *
FROM snowplow.atomic.events
WHERE derived_tstamp::DATE = '2025-09-26'
  AND user_id = '67970160' -- robin who clicked a juniper deal
ORDER BY derived_tstamp
;

SELECT *
FROM data_vault_mvp.bi.dim_sale_territory dst
WHERE dst.pre_qualification_uk IS NOT NULL

SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE derived_tstamp::DATE = '2025-09-26'
  AND user_id = '67970160' -- robin who clicked a juniper deal
ORDER BY derived_tstamp
;


SELECT *
FROM se.data_pii.scv_event_stream ses
WHERE ses.se_category = 'JUNIPER_DEAL_CLICKED'
  AND ses.event_tstamp::DATE = '2025-12-09'


