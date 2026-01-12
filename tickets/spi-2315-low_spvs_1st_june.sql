SELECT
    sts.event_tstamp::DATE,
    COUNT(*)
FROM se.data.scv_touched_spvs sts
WHERE sts.event_tstamp >= CURRENT_DATE - 20
GROUP BY 1;


SELECT
    COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp::DATE = '2022-06-01';

SELECT
    COUNT(*)
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::DATE = '2022-06-01'; -- 7546754


SELECT
    COUNT(*)
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::DATE = '2022-06-02'; -- 7318776

SELECT *
FROM snowplow.atomic.events e
WHERE e.collector_tstamp::DATE = '2022-06-01'; -- 7546754

SELECT
    DATE_TRUNC(HOUR, ses.event_tstamp),
    COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.collector_tstamp::DATE BETWEEN '2022-05-30' AND '2022-06-02'
GROUP BY 1;


SELECT
    DATE_TRUNC(HOUR, ses.event_tstamp),
    COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp::DATE BETWEEN '2022-05-30' AND '2022-06-02'
GROUP BY 1;


SELECT
    DATE_TRUNC(HOUR, ses.etl_tstamp),
    COUNT(*)
FROM snowplow.atomic.events ses
WHERE ses.collector_tstamp::DATE BETWEEN '2022-05-30' AND '2022-06-02'
GROUP BY 1;

------------------------------------------------------------------------------------------------------------------------
--issue found in event stream on the 1st of June 2022

SELECT
    DATE_TRUNC(HOUR, ses.etl_tstamp) AS hour,
    COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.etl_tstamp::DATE = '2022-06-02'
GROUP BY 1;

SELECT
    DATE_TRUNC(HOUR, ses.updated_at) AS hour,
    COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream ses
WHERE ses.etl_tstamp::DATE = '2022-06-02'
GROUP BY 1;



SELECT
    DATE_TRUNC(HOUR, mts.updated_at) AS hour,
    COUNT(*)
FROM data_vault_mvp.single_customer_view_stg.module_touched_spvs mts
WHERE mts.event_tstamp::DATE = '2022-06-02'
GROUP BY 1;


