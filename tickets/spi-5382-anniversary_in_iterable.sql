DROP TABLE IF EXISTS data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
;

SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

-- Copied clone data
-- reran  model in development

self_describing_task --include 'biapp/task_catalogue/dv/dwh/iterable/user_profile_transaction_base_data.py'  --method 'run' --start '2024-07-12 00:00:00' --end '2024-07-12 00:00:00'


-- used your query to find a person

SELECT
	t.*
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data t,
	 LATERAL FLATTEN(INPUT => t.five_most_recent_stayed_booking_details) f
WHERE f.value:bookingDateAnniversary::BOOLEAN = TRUE
  AND f.value:depatureDateAnniversary::BOOLEAN = TRUE
;

-- shiro_user_id: 4225983 - has 5 bookings

-- using this user 80269432

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
WHERE shiro_user_id = 80269432
;

-- deleted all users except for this user in base data

DELETE
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
WHERE shiro_user_id IS DISTINCT FROM 80269432
;

-- data only contains one row
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data

-- drop any dev table incase it exists
DROP TABLE IF EXISTS data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction
;

-- run transaction data based on the one user
self_describing_task --include 'biapp/task_catalogue/dv/dwh/iterable/user_profile_transaction.py'  --method 'run' --start '2024-07-12 00:00:00' --end '2024-07-12 00:00:00'

-- table now only contains 1 row

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction
;

-- alter user id for this 1 row, found one user in iterable sandbox to update
UPDATE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction target
SET target.shiro_user_id = 12
WHERE target.shiro_user_id = 80269432
;

-- check the shiro user id has been updated to 12
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction
;

-- drop outgoing self describing
DROP TABLE IF EXISTS unload_vault_mvp_dev_robin.iterable.user_profile_transaction_sandbox
;

-- reran outgoing modelling for sandbox
self_describing_task --include 'biapp/task_catalogue/staging/outgoing/iterable/user_profile_transaction_sandbox/modelling.py'  --method 'run' --start '2024-07-12 00:00:00' --end '2024-07-12 00:00:00'

-- check outgoing

SELECT *
FROM unload_vault_mvp_dev_robin.iterable.user_profile_transaction_sandbox__20240711t030000__daily_at_03h00
;

-- run the unload operation
dataset_task --include 'outgoing.iterable.user_profile_transaction_sandbox' --operation UnloadOperation --method 'run' --start '2024-07-12 00:00:00' --end '2024-07-12 00:00:00'

-- run the distribute operation
dataset_task --include 'outgoing.iterable.user_profile_transaction_sandbox' --operation DistributeOperation --method 'run' --start '2024-07-12 00:00:00' --end '2024-07-12 00:00:00'


------------------------------------------------------------------------------------------------------------------------
-- tested again with a user who already has 3 most recent stayed bookings set: 39901485

-- alter user id for this 1 row, found one user in iterable sandbox to update

UPDATE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction target
SET target.shiro_user_id = 39901485
WHERE target.shiro_user_id = 12
;

-- check the shiro user id has been updated to 12
SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction
;

-- drop outgoing self describing
DROP TABLE IF EXISTS unload_vault_mvp_dev_robin.iterable.user_profile_transaction_sandbox
;

-- reran outgoing modelling for sandbox
self_describing_task --include 'biapp/task_catalogue/staging/outgoing/iterable/user_profile_transaction_sandbox/modelling.py'  --method 'run' --start '2024-07-12 00:00:00' --end '2024-07-12 00:00:00'

-- check outgoing

SELECT *
FROM unload_vault_mvp_dev_robin.iterable.user_profile_transaction_sandbox__20240711t030000__daily_at_03h00
;

-- run the unload operation
dataset_task --include 'outgoing.iterable.user_profile_transaction_sandbox' --operation UnloadOperation --method 'run' --start '2024-07-12 00:00:00' --end '2024-07-12 00:00:00'

-- run the distribute operation
dataset_task --include 'outgoing.iterable.user_profile_transaction_sandbox' --operation DistributeOperation --method 'run' --start '2024-07-12 00:00:00' --end '2024-07-12 00:00:00'

------------------------------------------------------------------------------------------------------------------------
-- found dodgey users with tvl bookings associated to them

SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_transaction iupt
WHERE iupt.shiro_user_id = '6465703'
;

SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_transaction_base_data iuptbd
WHERE iuptbd.shiro_user_id = 6465703
;



SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data iuptbd
WHERE iuptbd.shiro_user_id = 6465703
;

self_describing_task --include 'biapp/task_catalogue/dv/dwh/iterable/user_profile_transaction_base_data.py'  --method 'run' --start '2018-01-01 00:00:00' --end '2018-01-01 00:00:00'


SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.iterable__user_profile_transaction_base_data iuptbd
;

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data iuptbd
;

./
scripts/
mwaa-cli production "dags backfill --start-date '2018-01-01 00:00:00' --end-date '2018-01-02 00:00:00' dwh__iterable__user_profile_transaction__daily_at_03h00"

SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	shiro_user_id,
	booking_period,
	net_booking_data_set,
	gross_booking_data_set,
	three_most_recent_stayed_booking_details,
	booking_type_object,
	row_hash
FROM data_vault_mvp.dwh.iterable__user_profile_transaction_historical_base_data
;

SELECT GET_DDL('table', 'data_vault_mvp.dwh.iterable__user_profile_transaction_historical_base_data')
;

USE ROLE pipelinerunner
;

ALTER TABLE data_vault_mvp.dwh.iterable__user_profile_transaction_historical_base_data
	RENAME COLUMN three_most_recent_stayed_booking_details TO five_most_recent_stayed_booking_details

SELECT SHA2(CURRENT_TIME)


DROP TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data CLONE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data
;

USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data_20240717 CLONE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data
;

GRANT SELECT ON TABLE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data_20240717 TO ROLE data_team_basic
;

USE ROLE personal_role__robinpatel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data_20240717 CLONE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data
;

MERGE INTO data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data_20240717 target USING (
	WITH
		users_removed AS (
			SELECT
				iuptbd.shiro_user_id
			FROM data_vault_mvp.dwh.iterable__user_profile_transaction_base_data_20240717 iuptbd

			EXCEPT

			SELECT
				iuptbd.shiro_user_id
			FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data iuptbd
		)
	SELECT

		bd.shiro_user_id,
		NULL                    AS booking_period,
		NULL                    AS net_booking_data_set,
		NULL                    AS gross_booking_data_set,
		NULL                    AS five_most_recent_stayed_booking_details,
		NULL                    AS booking_type_object,
		SHA2(CURRENT_TIMESTAMP) AS row_hash
	FROM data_vault_mvp.dwh.iterable__user_profile_transaction_base_data_20240717 bd
		INNER JOIN users_removed ur ON bd.shiro_user_id = ur.shiro_user_id
	QUALIFY ROW_NUMBER() OVER (PARTITION BY bd.shiro_user_id ORDER BY schedule_tstamp) = 1
) AS batch
	ON target.shiro_user_id = batch.shiro_user_id
	WHEN MATCHED THEN UPDATE SET
		target.schedule_tstamp = CURRENT_TIMESTAMP()::TIMESTAMP,
		target.run_tstamp = CURRENT_TIMESTAMP()::TIMESTAMP,
		target.operation_id = 'purge of tvl data attached to se users',
		target.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,

		target.booking_period = batch.booking_period,
		target.net_booking_data_set = batch.net_booking_data_set,
		target.gross_booking_data_set = batch.gross_booking_data_set,
		target.five_most_recent_stayed_booking_details = batch.five_most_recent_stayed_booking_details,
		target.booking_type_object = batch.booking_type_object,
		target.row_hash = batch.row_hash
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data_20240717
WHERE booking_type_object IS NULL

SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_transaction_base_data iuptbd
WHERE iuptbd.shiro_user_id = 51885129
;


SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_transaction_base_data
WHERE five_most_recent_stayed_booking_details[0]['saleId']::VARCHAR LIKE 'TVL%'


------------------------------------------------------------------------------------------------------------------------
USE ROLE personal_role__robinpatel
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data_20240717 CLONE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data
;

GRANT SELECT ON TABLE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data_20240717 TO ROLE data_team_basic
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data_20240717 CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
;

DELETE
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data target
	USING (
		 WITH
			 remove_users AS (
				 SELECT
					 iuptbd.shiro_user_id
				 FROM data_vault_mvp.dwh.iterable__user_profile_transaction_base_data_20240717 iuptbd

				 EXCEPT

				 SELECT
					 iuptbd.shiro_user_id
				 FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data_20240717 iuptbd
			 )
		 SELECT *
		 FROM remove_users ru
	 ) batch
WHERE target.shiro_user_id = batch.shiro_user_id
;

SELECT
	COUNT(*)
FROM data_vault_mvp.dwh.iterable__user_profile_transaction_base_data iuptbd
;

SELECT
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data target
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction CLONE data_vault_mvp.dwh.iterable__user_profile_transaction
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data iuptbd
WHERE iuptbd.shiro_user_id = 2660409
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction
WHERE iterable__user_profile_transaction.shiro_user_id = 2660409
;

WITH
	user_bookings AS (
		SELECT
			fb.shiro_user_id,
			COUNT(*)                                               AS bookings,
			COUNT(IFF(fb.booking_status_type = 'live', 1, 0))      AS live_bookings,
			COUNT(IFF(fb.booking_status_type = 'cancelled', 1, 0)) AS cancelled_bookings
		FROM se.data.fact_booking fb
		WHERE fb.booking_status_type IN ('live', 'cancelled')
		GROUP BY 1
	)

SELECT
	pt.shiro_user_id,
	ub.bookings,
	ub.live_bookings,
	ub.cancelled_bookings
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction pt
	LEFT JOIN user_bookings ub ON pt.shiro_user_id = ub.shiro_user_id
WHERE pt.user_transaction_object['userTransaction']['lifetimeTransaction']['threeMostRecentStayedBookingDetails'] IS NOT NULL
  AND ub.bookings IS NOT NULL
;

USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable__user_profile_transaction_20240717 CLONE data_vault_mvp.dwh.iterable__user_profile_transaction
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data_20240717 CLONE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data
;


DROP TABLE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data
;


-- rerun the base data step of the user_transaction dag for current date
USE WAREHOUSE pipe_xlarge
;

UPDATE data_vault_mvp.dwh.iterable__user_profile_transaction AS target
SET target.run_tstamp              = CURRENT_TIMESTAMP()::TIMESTAMP,
	target.operation_id            = 'purge of tvl data attached to se users',
	target.updated_at              = CURRENT_TIMESTAMP()::TIMESTAMP,
	target.user_transaction_object = NULL,
	target.row_hash                = SHA2(CURRENT_TIMESTAMP())
FROM (
	WITH
		user_bookings AS (
			-- belt and braces to ensure these users have 0 bookings
			SELECT
				fb.shiro_user_id,
				COUNT(*)                                               AS bookings,
				COUNT(IFF(fb.booking_status_type = 'live', 1, 0))      AS live_bookings,
				COUNT(IFF(fb.booking_status_type = 'cancelled', 1, 0)) AS cancelled_bookings
			FROM se.data.fact_booking fb
			WHERE fb.booking_status_type IN ('live', 'cancelled')
			GROUP BY 1
		)
	SELECT
		pt.schedule_tstamp,
		pt.run_tstamp,
		pt.operation_id,
		pt.created_at,
		pt.updated_at,
		pt.shiro_user_id,
		pt.user_transaction_object,
		pt.row_hash
	FROM data_vault_mvp.dwh.iterable__user_profile_transaction pt
		LEFT JOIN user_bookings ub ON pt.shiro_user_id = ub.shiro_user_id
	WHERE pt.user_transaction_object['userTransaction']['lifetimeTransaction']['threeMostRecentStayedBookingDetails'] IS NOT NULL
	  AND ub.bookings IS NULL
) AS batch
WHERE target.shiro_user_id = batch.shiro_user_id
;

-- rerun the outgoing job for user transaction
USE ROLE pipelinerunner;
DELETE
FROM data_vault_mvp.dwh.iterable__user_profile_transaction
WHERE operation_id = 'purge of tvl data attached to se users'
;

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_transaction iuptbd
WHERE iuptbd.shiro_user_id = 3190465

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction CLONE data_vault_mvp.dwh.iterable__user_profile_transaction
;

SELECT
*
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction iuptbd
WHERE iuptbd.operation_id = 'purge of tvl data attached to se users';
-- AND iuptbd.shiro_user_id = 3190465

SELECT object_construct('userTransaction', object_construct('lifetimeTransaction', {}));

UPDATE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction iupt
SET iupt.updated_at = CURRENT_TIMESTAMP()::TIMESTAMP,
    iupt.user_transaction_object = object_construct('userTransaction', object_construct('lifetimeTransaction', {}))
WHERE iupt.operation_id = 'purge of tvl data attached to se users';


self_describing_task --include 'biapp/task_catalogue/staging/outgoing/iterable/user_profile_transaction_first_quartile/modelling.py'  --method 'run' --start '2024-07-18 00:00:00' --end '2024-07-18 00:00:00'


dataset_task --include 'outgoing.iterable.user_profile_transaction_first_quartile' --operation UnloadOperation --method 'run'  --start '2024-07-17 03:00:00' --end '2024-07-17 03:00:00'

dataset_task --include 'outgoing.iterable.user_profile_transaction_first_quartile' --operation DistributeOperation --method 'run'  --start '2024-07-17 03:00:00' --end '2024-07-17 03:00:00'