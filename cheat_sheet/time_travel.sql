SELECT
	COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream AT (OFFSET => -(60 * 60 * 11))
;


SELECT
	COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream
;


-- https://docs.snowflake.com/en/user-guide/data-time-travel

SELECT
	COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream AT (OFFSET => -(60 * 60 * 11))
;


/*
CREATE TABLE restored_table CLONE my_table
  AT(TIMESTAMP => 'Sat, 09 May 2015 01:01:00 +0300'::timestamp_tz);
 */

SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.tb_booking AT (TIMESTAMP => '2024-01-14 04:00:00.0000 +00:00'::timestamp_tz)
;

SELECT CURRENT_TIMESTAMP


CREATE OR REPLACE TABLE scratch.robinpatel.privileges CLONE hygiene_vault.snowflake_uac.privileges
	AT (TIMESTAMP => '2026-01-28 13:00:00'::timestamp_tz)
;

SELECT *
FROM scratch.robinpatel.privileges


SELECT *
FROM hygiene_vault.snowflake_uac.privileges
;

------------------------------------------------------------------------------------------------------------------------


SHOW TABLES IN SCHEMA hygiene_vault.snowflake_uac
;

USE ROLE pipelinerunner
;

ALTER TABLE hygiene_vault.snowflake_uac.privileges RENAME TO hygiene_vault.snowflake_uac.privileges_20260128_pre_uac_bug;


CREATE OR REPLACE TABLE hygiene_vault.snowflake_uac.privileges CLONE hygiene_vault.snowflake_uac.privileges_20260128_pre_uac_bug
	AT (TIMESTAMP => '2026-01-28 13:00:00'::timestamp_tz)
;

SELECT COUNT(*)
FROM hygiene_vault.snowflake_uac.privileges;

SELECT COUNT(*)
FROM hygiene_vault.snowflake_uac.privileges_20260128_pre_uac_bug;

