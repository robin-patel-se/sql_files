SELECT
	COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream AT (OFFSET => -(60 * 60 * 11));


SELECT
	COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream
;


-- https://docs.snowflake.com/en/user-guide/data-time-travel

SELECT
	COUNT(*)
FROM hygiene_vault_mvp.snowplow.event_stream AT (OFFSET => -(60 * 60 * 11));



CREATE TABLE restored_table CLONE my_table
  AT(TIMESTAMP => 'Sat, 09 May 2015 01:01:00 +0300'::timestamp_tz);
``

SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.tb_booking AT (TIMESTAMP => '2024-01-14 04:00:00.0000 +00:00'::timestamp_tz);

SELECT CURRENT_TIMESTAMP
