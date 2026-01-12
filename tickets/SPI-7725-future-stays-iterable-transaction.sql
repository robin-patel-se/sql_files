USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
	CLONE data_vault_mvp.dwh.iterable__user_profile_transaction_base_data
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
WHERE iterable__user_profile_transaction_base_data.shiro_user_id = 72868430
;


WITH
	most_recent_booking_stays AS (
		SELECT
			mbd.shiro_user_id,
			mbd.transaction_id,
			mbd.se_sale_id,
			mbd.booking_completed_date,
			DATEDIFF(DAY, mbd.check_in_date, CURRENT_DATE()) AS days_since_last_check_in,
			mbd.check_in_date <= CURRENT_DATE                AS is_past_stay,
			IFF(is_past_stay,
				ROW_NUMBER() OVER (PARTITION BY mbd.shiro_user_id, is_past_stay ORDER BY days_since_last_check_in ASC),
				NULL)
															 AS previous_stay_order,
			IFF(is_past_stay = FALSE,
				ROW_NUMBER() OVER (PARTITION BY mbd.shiro_user_id, is_past_stay ORDER BY days_since_last_check_in DESC),
				NULL)
															 AS future_stay_order,
			mbd.check_in_date,
			mbd.check_out_date,
			mbd.booking_date_anniversary,
			mbd.departure_date_anniversary,
			mbd.posu_city,
			mbd.posu_country,
			mbd.travel_type,
			mbd.sale_product,
			mbd.booking_includes_flight

		FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data__step01__model_base_dataset__lifetime mbd
		WHERE booking_status_type = 'live'
		  AND booking_type = 'net'
		  AND shiro_user_id = 72868430 -- TODO REMOVE
	),

	object_construct_data AS (
		SELECT
			mrbs.shiro_user_id,
			mrbs.is_past_stay,
			mrbs.previous_stay_order,
			mrbs.future_stay_order,
			OBJECT_CONSTRUCT(
					'saleId', mrbs.se_sale_id,
					'transactionId', mrbs.transaction_id,
					'bookingCompletedDate', mrbs.booking_completed_date,
					'checkInDate', mrbs.check_in_date,
					'checkOutDate', mrbs.check_out_date,
					'destinationCity', mrbs.posu_city,
					'destinationCountry', mrbs.posu_country,
					'travelType', mrbs.travel_type,
					'saleProduct', mrbs.sale_product,
					'hasFlights', mrbs.booking_includes_flight,
					'bookingDateAnniversary', mrbs.booking_date_anniversary,
					'depatureDateAnniversary', mrbs.departure_date_anniversary
			) AS booking_details,
		FROM most_recent_booking_stays mrbs
		WHERE (mrbs.previous_stay_order <= 5
			OR mrbs.future_stay_order <= 5)
	),
	list_of_users AS (
		SELECT DISTINCT
			shiro_user_id
		FROM most_recent_booking_stays
	)
SELECT
	booking_details.shiro_user_id,
	ARRAY_AGG(IFF(booking_details.is_past_stay, booking_details.booking_details, NULL))
			  WITHIN GROUP (ORDER BY booking_details.previous_stay_order ) AS five_most_recent_stays_details,
	ARRAY_AGG(IFF(booking_details.is_past_stay = FALSE, booking_details.booking_details, NULL))
			  WITHIN GROUP (ORDER BY booking_details.future_stay_order )   AS five_future_stays_details
FROM list_of_users users
LEFT JOIN object_construct_data booking_details
	ON users.shiro_user_id = booking_details.shiro_user_id
GROUP BY 1
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data__step06__five_most_recent_booking_stays_object__lifetime
WHERE
	iterable__user_profile_transaction_base_data__step06__five_most_recent_booking_stays_object__lifetime.shiro_user_id =
	72868430
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data__step10__create_net_gross_booking_object__lifetime
WHERE iterable__user_profile_transaction_base_data__step10__create_net_gross_booking_object__lifetime.shiro_user_id =
	  72868430
;



SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
WHERE iterable__user_profile_transaction_base_data.shiro_user_id = 72868430
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data_20250903 CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
;

USE WAREHOUSE pipe_xlarge
;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
	(

		-- (lineage) metadata for the current job
	 schedule_tstamp TIMESTAMP,
	 run_tstamp TIMESTAMP,
	 operation_id VARCHAR,
	 created_at TIMESTAMP,
	 updated_at TIMESTAMP,
	 shiro_user_id INT,
	 booking_period VARCHAR,
	 net_booking_data_set OBJECT,
	 gross_booking_data_set OBJECT,
	 five_most_recent_stayed_booking_details ARRAY,
	 five_future_stays_booking_details ARRAY,
	 booking_type_object OBJECT,
	 row_hash VARCHAR,
		CONSTRAINT pk_iterable__user_profile_transaction_base_data PRIMARY KEY (shiro_user_id, booking_period)
		)
AS
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
	five_most_recent_stayed_booking_details,
	NULL,
	booking_type_object,
	row_hash
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data_20250903
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
WHERE iterable__user_profile_transaction_base_data.shiro_user_id = 72868430
;

SELECT *
FROM data_vault_mvp.dwh.iterable__user_profile_transaction_base_data
WHERE iterable__user_profile_transaction_base_data.shiro_user_id = 72868430
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data__step06__five_most_recent_booking_stays_object__lifetime
WHERE shiro_user_id = 72868430
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data__step10__create_net_gross_booking_object__lifetime
WHERE shiro_user_id = 72868430
;

DROP TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_base_data
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction
	CLONE data_vault_mvp.dwh.iterable__user_profile_transaction
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction
WHERE iterable__user_profile_transaction.shiro_user_id = 72868430
;


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.se_calendar
AS
SELECT *
FROM data_vault_mvp.dwh.se_calendar
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.dim_sale
	CLONE data_vault_mvp.dwh.dim_sale
;

CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.dwh.fact_booking
AS
SELECT *
FROM data_vault_mvp.dwh.fact_booking
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_historical_base_data
	CLONE data_vault_mvp.dwh.iterable__user_profile_transaction_historical_base_data
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.iterable.user_profile_transaction_historical_base_data.py' \
    --method 'run' \
    --start '2025-09-03 00:00:00' \
    --end '2025-09-03 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_historical_base_data_20250903 CLONE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_historical_base_data
;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_historical_base_data
	(

		-- (lineage) metadata for the current job
	 schedule_tstamp TIMESTAMP,
	 run_tstamp TIMESTAMP,
	 operation_id VARCHAR,
	 created_at TIMESTAMP,
	 updated_at TIMESTAMP,
	 shiro_user_id INT,
	 booking_period VARCHAR,
	 net_booking_data_set OBJECT,
	 gross_booking_data_set OBJECT,
	 five_most_recent_stayed_booking_details ARRAY,
	 five_future_stays_booking_details ARRAY,
	 booking_type_object OBJECT,
	 row_hash VARCHAR,
		CONSTRAINT pk_iterable__user_profile_transaction_historical_base_data PRIMARY KEY (shiro_user_id, booking_period)
		)
AS
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
	five_most_recent_stayed_booking_details,
	NULL,
	booking_type_object,
	row_hash
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_historical_base_data_20250903
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_historical_base_data
WHERE shiro_user_id = 72868430
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction
WHERE shiro_user_id = 72868430
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction_historical
WHERE shiro_user_id = 72868430
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes
;


SELECT
	fcb.shiro_user_id,
	fcb.check_in_date,
	fcb.booking_id,
	fcb.booking_completed_date
FROM se.data.fact_complete_booking fcb
-- WHERE fcb.check_in_date > CURRENT_DATE
WHERE fcb.check_in_date <= CURRENT_DATE
QUALIFY COUNT(*) OVER (PARTITION BY fcb.shiro_user_id) > 3
;


SELECT
	user_transaction_object['userTransaction']['lifetimeTransaction']['fiveFutureStaysBookingDetails']      AS five_future_stays_array,
	user_transaction_object['userTransaction']['lifetimeTransaction']['fiveMostRecentStayedBookingDetails'] AS five_previous_stays_array,
FROM data_vault_mvp_dev_robin.dwh.iterable__user_profile_transaction
WHERE shiro_user_id = 65432249
;