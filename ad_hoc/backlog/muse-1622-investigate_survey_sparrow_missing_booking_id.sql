SELECT nr.record,
       TO_JSON(nr.record::VARIANT)
FROM raw_vault.survey_sparrow.nps_responses nr;


SELECT nr.record,
       nr.booking_id,
       record['customProperties'][0]['value_string']
FROM hygiene_vault.survey_sparrow.nps_responses nr
WHERE nr.booking_id IS NULL;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.survey_sparrow.nps_responses CLONE raw_vault.survey_sparrow.nps_responses;

DROP TABLE hygiene_vault_dev_robin.survey_sparrow.nps_responses;


dataset_task --include 'survey_sparrow.nps_responses' --operation HygieneOperation --method 'run' --start '2020-07-15 00:30:00' --end '2020-07-15 00:30:00'


------------------------------------------------------------------------------------------------------------------------
-- post deployment steps:

-- backup table
CREATE OR REPLACE TRANSIENT TABLE hygiene_vault.survey_sparrow.nps_responses_20220301 CLONE hygiene_vault.survey_sparrow.nps_responses;
CREATE OR REPLACE TRANSIENT TABLE latest_vault.survey_sparrow.nps_responses_20220301 CLONE latest_vault.survey_sparrow.nps_responses;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.survey_sparrow.nps_responses CLONE hygiene_vault.survey_sparrow.nps_responses;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.survey_sparrow.nps_responses CLONE latest_vault.survey_sparrow.nps_responses;
-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.survey_sparrow.nps_responses CLONE hygiene_vault.survey_sparrow.nps_responses;
-- CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.survey_sparrow.nps_responses_20220301 CLONE hygiene_vault_dev_robin.survey_sparrow.nps_responses;

-- drop tables:
DROP TABLE hygiene_vault.survey_sparrow.nps_responses;
DROP TABLE latest_vault.survey_sparrow.nps_responses;

dataset_task --include 'survey_sparrow.nps_responses' --operation HygieneOperation --method 'run' --start '1970-01-01 00:30:00' --end '1970-01-01 00:30:00'
dataset_task --include 'survey_sparrow.nps_responses' --operation LatestRecordsOperation --method 'run' --start '1970-01-01 00:30:00' --end '1970-01-01 00:30:00'

-- rerun hygiene operation on the 1970-01-01
-- rerun latest records operation on the 1970-01-01

DROP TABLE hygiene_vault_dev_robin.survey_sparrow.nps_responses;
DROP TABLE latest_vault_dev_robin.survey_sparrow.nps_responses;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.survey_sparrow.nps_responses CLONE raw_vault.survey_sparrow.nps_responses;
--check row counts match
SELECT COUNT(*) FROM hygiene_vault.survey_sparrow.nps_responses;
SELECT COUNT(*) FROM hygiene_vault_dev_robin.survey_sparrow.nps_responses;

SELECT COUNT(*) FROM raw_vault.survey_sparrow.nps_responses nr;

SELECT COUNT(*) FROM latest_vault.survey_sparrow.nps_responses;
SELECT COUNT(*) FROM latest_vault_dev_robin.survey_sparrow.nps_responses;




