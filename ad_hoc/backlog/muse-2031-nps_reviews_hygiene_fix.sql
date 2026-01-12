CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.survey_sparrow.nps_responses CLONE raw_vault.survey_sparrow.nps_responses;

DROP TABLE hygiene_vault_dev_robin.survey_sparrow.nps_responses;
DROP TABLE latest_vault_dev_robin.survey_sparrow.nps_responses;

WITH booking_idd AS (
    SELECT nr.custom_properties_first_value,
           nr.custom_properties_second_value,
           CASE
               WHEN REGEXP_COUNT(nr.custom_properties_first_value, '-') = 2 THEN
                   IFF(LEFT(nr.custom_properties_first_value, 1) = 'A', 'A' || SPLIT_PART(nr.custom_properties_first_value, '-', -1), SPLIT_PART(nr.custom_properties_first_value, '-', -1))
               WHEN nr.custom_properties_second_value LIKE '%-%' THEN 'TB-' || SPLIT_PART(nr.custom_properties_second_value, '-', -1)
               ELSE nr.custom_properties_second_value
               END AS booking_id

    FROM latest_vault.survey_sparrow.nps_responses nr
)
SELECT bi.booking_id,
       bi.custom_properties_first_value,
       bi.custom_properties_second_value,
       fb.booking_status_type
FROM booking_idd bi
    LEFT JOIN se.data.fact_booking fb ON bi.booking_id = fb.booking_id;

dataset_task --include 'survey_sparrow.nps_responses' --operation LatestRecordsOperation --method 'run' --start '2021-11-02 00:30:00' --end '2021-11-02 00:30:00'

SELECT MIN(loaded_at)
FROM raw_vault.survey_sparrow.nps_responses nr; --2021-11-02 11:22:31.914000000

------------------------------------------------------------------------------------------------------------------------

SELECT nr.booking_id,
       fb.booking_status_type
FROM latest_vault_dev_robin.survey_sparrow.nps_responses nr
    LEFT JOIN se.data.fact_booking fb ON nr.booking_id = fb.booking_id;

SELECT COUNT(*),
       COUNT(fb.booking_id),
       SUM(IFF(fb.booking_status_type NOT IN ('live', 'cancelled'), 1, 0))
FROM latest_vault_dev_robin.survey_sparrow.nps_responses nr
    LEFT JOIN se.data.fact_booking fb ON nr.booking_id = fb.booking_id;



------------------------------------------------------------------------------------------------------------------------

SELECT DATE_TRUNC('month', ubr.review_date) AS review_month,
       COUNT(*)
FROM se.data.user_booking_review ubr
GROUP BY 1;

SELECT DATE_TRUNC('month', ubr.review_date) AS review_month,
       COUNT(*)
FROM se.data.user_booking_review ubr
WHERE ubr.booking_status_type = 'live'
GROUP BY 1;




SELECT *
FROM collab.finance.se_booking_cancellation_v sbcv;

SELECT *
FROM data_vault_mvp.dwh.user_booking_review ubr
WHERE ubr.review_tstamp >= CURRENT_DATE - 1;

SELECT *
FROM latest_vault.survey_sparrow.nps_responses nr
WHERE nr.booking_id = '71645280';

SELECT *
FROM latest_vault.survey_sparrow.nps_responses nr
WHERE nr.custom_properties[1]:value_string::VARCHAR = '37696393';

SELECT * FROM se.data.user_booking_review ubr WHERE ubr.review_tstamp>= current_date -30;