SELECT rn.score,
       rn.positive_experiences,
       rn.negative_experiences,
       CASE
           WHEN rn.positive_experiences IS NULL THEN rn.negative_experiences
           WHEN rn.negative_experiences IS NULL THEN rn.positive_experiences
           ELSE rn.positive_experiences || '\n' || rn.negative_experiences END AS follow_up_answer,
       OBJECT_CONSTRUCT(
               'positive_experiences', rn.positive_experiences,
               'negative_experiences', rn.negative_experiences
           )                                                                   AS follow_up_answer_object
FROM hygiene_snapshot_vault_mvp.travelbird_mysql.reviews_npsscore rn;


CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.survey_sparrow.nps_responses CLONE latest_vault.survey_sparrow.nps_responses;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.reviews_npsscore CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.reviews_npsscore;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfmc.net_promoter_score CLONE hygiene_snapshot_vault_mvp.sfmc.net_promoter_score;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking;


self_describing_task --include 'dv/dwh/reviews/user_booking_review.py'  --method 'run' --start '2021-11-21 00:00:00' --end '2021-11-21 00:00:00'

SELECT *
FROM data_vault_mvp.dwh.user_booking_review ubr
WHERE ubr.survey_source = 'travelbird';

SELECT *
FROM raw_vault_mvp.sfmc.net_promoter_score nps;