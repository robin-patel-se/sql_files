module=/
biapp/
task_catalogue/
dv/
dwh/
transactional/
se_booking.py make clones

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.allocation
	CLONE latest_vault.cms_mysql.allocation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.amendment
	CLONE latest_vault.cms_mysql.amendment
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_offer
	CLONE latest_vault.cms_mysql.base_offer
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.booking
	CLONE latest_vault.cms_mysql.booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.booking_allocations
	CLONE latest_vault.cms_mysql.booking_allocations
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.booking_cancellation
	CLONE data_vault_mvp.dwh.booking_cancellation
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mongodb
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mongodb.booking_summary
	CLONE latest_vault.cms_mongodb.booking_summary
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.chiasma_external_booking
	CLONE data_vault_mvp.dwh.chiasma_external_booking
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.fpa_gsheets
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.constant_currency
	CLONE latest_vault.fpa_gsheets.constant_currency
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.days_before_policy
	CLONE latest_vault.cms_mysql.days_before_policy
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.offer
	CLONE latest_vault.cms_mysql.offer
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.offer_details
	CLONE latest_vault.cms_mysql.offer_details
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product
	CLONE latest_vault.cms_mysql.product
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product_reservation
	CLONE latest_vault.cms_mysql.product_reservation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.promo_code
	CLONE latest_vault.cms_mysql.promo_code
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.reservation
	CLONE latest_vault.cms_mysql.reservation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.reservation_base_offer
	CLONE latest_vault.cms_mysql.reservation_base_offer
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.reservation_exchange_rate
	CLONE latest_vault.cms_mysql.reservation_exchange_rate
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.sale_details
	CLONE latest_vault.cms_mysql.sale_details
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_offer_product
	CLONE latest_vault.cms_mysql.base_offer_product
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.product_provider
	CLONE latest_vault.cms_mysql.product_provider
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.flightservice__order_orderchange
	CLONE data_vault_mvp.dwh.flightservice__order_orderchange
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_credit
	CLONE data_vault_mvp.dwh.se_credit
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_booking
	CLONE data_vault_mvp.dwh.se_booking
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.transactional.se_booking.py' \
    --method 'run' \
    --start '2025-08-06 00:00:00' \
    --end '2025-08-06 00:00:00'


SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking
;


USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.chiasma_external_booking COPY GRANTS
	(
	 schedule_tstamp TIMESTAMP,
	 run_tstamp TIMESTAMP,
	 operation_id VARCHAR,
	 created_at TIMESTAMP,
	 updated_at TIMESTAMP,
	 booking_id VARCHAR PRIMARY KEY NOT NULL,
	 transaction_id VARCHAR,
	 external_reference_id VARCHAR,
	 booking_status VARCHAR,
	 booking_status_type VARCHAR,
	 booking_status_type_net_of_covid VARCHAR,
	 se_sale_id VARCHAR,
	 customer_identifier NUMBER,
	 check_in_date DATE,
	 check_out_date DATE,
	 booking_lead_time_days NUMBER,
	 booking_created_date DATE,
	 booking_completed_date DATE,
	 booking_completed_timestamp TIMESTAMP,
	 booking_cancelled_date DATE,
	 rate_to_gbp DECIMAL(19, 6),
	 customer_currency VARCHAR,
	 gross_revenue_cc DECIMAL(19, 6),
	 margin_gross_of_toms_cc DECIMAL(19, 6),
	 gross_revenue_gbp DECIMAL(19, 6),
	 gross_revenue_gbp_constant_currency DECIMAL(19, 6),
	 gross_revenue_eur_constant_currency DECIMAL(19, 6),
	 margin_gross_of_toms_gbp DECIMAL(19, 6),
	 margin_gross_of_toms_gbp_constant_currency DECIMAL(19, 6),
	 margin_gross_of_toms_eur_constant_currency DECIMAL(19, 6),
	 no_nights NUMBER,
	 adult_guests NUMBER,
	 child_guests NUMBER,
	 infant_guests NUMBER,
	 rooms NUMBER,
	 territory VARCHAR,
	 device_platform VARCHAR,
	 payment_type VARCHAR,
	 destination_type VARCHAR,
	 product_type VARCHAR,
	 posu_country VARCHAR,
	 posu_city VARCHAR,
	 booking_includes_flight BOOLEAN,
	 tech_platform VARCHAR,
	 price_per_night DECIMAL(19, 6),
	 price_per_person_per_night DECIMAL(19, 6)
		)
AS
SELECT
	ceb.schedule_tstamp,
	ceb.run_tstamp,
	ceb.operation_id,
	ceb.created_at,
	ceb.updated_at,
	ceb.booking_id,
	ceb.transaction_id,
	ceb.external_reference_id,
	ceb.booking_status,
	CASE
		WHEN UPPER(ceb.booking_status) = 'BOOKED' THEN 'live'
		WHEN (YEAR(ceb.booking_created_date) = '2019'
			AND ceb.booking_cancelled_date >= '2020-03-01'
			AND UPPER(ceb.booking_status) = 'CANCELLED') THEN 'live'
		WHEN UPPER(ceb.booking_status) = 'CANCELLED' THEN 'cancelled'
		ELSE 'other'
	END AS booking_status_type,
	CASE
		WHEN UPPER(ceb.booking_status) = 'BOOKED' THEN 'live'
		WHEN UPPER(ceb.booking_status) = 'CANCELLED' THEN 'cancelled'
		ELSE 'other'
	END AS booking_status_type_net_of_covid,
	ceb.se_sale_id,
	ceb.customer_identifier,
	ceb.check_in_date,
	ceb.check_out_date,
	ceb.booking_lead_time_days,
	ceb.booking_created_date,
	ceb.booking_completed_date,
	ceb.booking_completed_timestamp,
	ceb.booking_cancelled_date,
	ceb.rate_to_gbp,
	ceb.customer_currency,
	ceb.gross_revenue_cc,
	ceb.margin_gross_of_toms_cc,
	ceb.gross_revenue_gbp,
	ceb.gross_revenue_gbp_constant_currency,
	ceb.gross_revenue_eur_constant_currency,
	ceb.margin_gross_of_toms_gbp,
	ceb.margin_gross_of_toms_gbp_constant_currency,
	ceb.margin_gross_of_toms_eur_constant_currency,
	ceb.no_nights,
	ceb.adult_guests,
	ceb.child_guests,
	ceb.infant_guests,
	ceb.rooms,
	ceb.territory,
	ceb.device_platform,
	ceb.payment_type,
	ceb.destination_type,
	ceb.product_type,
	ceb.posu_country,
	ceb.posu_city,
	ceb.booking_includes_flight,
	ceb.tech_platform,
	ceb.price_per_night,
	ceb.price_per_person_per_night
FROM data_vault_mvp.dwh.chiasma_external_booking ceb
;


SELECT *
FROM data_vault_mvp.dwh.chiasma_external_booking


------------------------------------------------------------------------------------------------------------------------
	use role personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.cms_mysql
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_sale
	CLONE latest_vault.cms_mysql.base_sale
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.bedfinder
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.bedfinder.wrd_booking
	CLONE latest_vault.bedfinder.wrd_booking
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.fpa_gsheets
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.constant_currency
	CLONE latest_vault.fpa_gsheets.constant_currency
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.fx
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates
	CLONE data_vault_mvp.fx.rates
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory
	CLONE latest_vault.cms_mysql.territory
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.bigxtra_booking
	CLONE data_vault_mvp.dwh.bigxtra_booking
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.wrd_booking
	CLONE data_vault_mvp.dwh.wrd_booking
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.wrd.wrd_booking.py' \
    --method 'run' \
    --start '2025-08-06 00:00:00' \
    --end '2025-08-06 00:00:00'

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.airline_holidays
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.blue_bay_travel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.broadway_travel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.exoticca
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.jetline_travel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.neon_reisen
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.trading_gsheets
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.perfectstay
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.style_in_travel
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.travelcircus
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.airline_holidays.wrd_booking CLONE latest_vault.airline_holidays.wrd_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.wrd_booking__step03__filter_bigxtra CLONE data_vault_mvp.dwh.wrd_booking__step03__filter_bigxtra
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.blue_bay_travel.wrd_booking CLONE latest_vault.blue_bay_travel.wrd_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.broadway_travel.wrd_booking CLONE latest_vault.broadway_travel.wrd_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.exoticca.wrd_booking CLONE latest_vault.exoticca.wrd_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.jetline_travel.wrd_booking CLONE latest_vault.jetline_travel.wrd_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.journaway_booking CLONE data_vault_mvp.dwh.journaway_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.neon_reisen.wrd_booking CLONE latest_vault.neon_reisen.wrd_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.offline_3pp_uk_booking CLONE data_vault_mvp.dwh.offline_3pp_uk_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.trading_gsheets.offline_margin CLONE latest_vault.trading_gsheets.offline_margin
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.perfectstay.wrd_booking CLONE latest_vault.perfectstay.wrd_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.style_in_travel.wrd_booking CLONE latest_vault.style_in_travel.wrd_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelcircus.wrd_booking CLONE latest_vault.travelcircus.wrd_booking
;


SELECT *
FROM data_vault_mvp_dev_robin.dwh.wrd_booking
;

------------------------------------------------------------------------------------------------------------------------
USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.chiasma_sql_server_snapshots
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.business_units_snapshot
	CLONE data_vault_mvp.chiasma_sql_server_snapshots.business_units_snapshot
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.fpa_gsheets
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.fpa_gsheets.constant_currency
	CLONE latest_vault.fpa_gsheets.constant_currency
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_bookings_snapshot
	CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_bookings_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_customers_snapshot
	CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_customers_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_currencies_snapshot
	CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_currencies_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_payment_types_snapshot
	CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_payment_types_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_platforms_snapshot
	CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_platforms_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_sales_snapshot
	CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_sales_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.dim_status_snapshot
	CLONE data_vault_mvp.chiasma_sql_server_snapshots.dim_status_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.chiasma_sql_server_snapshots.fact_bookings_v_snapshot
	CLONE data_vault_mvp.chiasma_sql_server_snapshots.fact_bookings_v_snapshot
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tb_booking
	CLONE data_vault_mvp.dwh.tb_booking
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.travelist
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelist.booking_summary
	CLONE latest_vault.travelist.booking_summary
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.travelist.cancelled_bookings
	CLONE latest_vault.travelist.cancelled_bookings
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_booking
	CLONE data_vault_mvp.dwh.tvl_booking
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.dwh.travelist.tvl_booking.py' \
    --method 'run' \
    --start '2025-08-06 00:00:00' \
    --end '2025-08-06 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.tvl_booking COPY GRANTS (
	-- (lineage) metadata for the current job
																						schedule_tstamp TIMESTAMP,
																						run_tstamp TIMESTAMP,
																						operation_id VARCHAR,
																						created_at TIMESTAMP,
																						updated_at TIMESTAMP,

	-- data columns
																						booking_id VARCHAR PRIMARY KEY NOT NULL,
																						transaction_id VARCHAR NOT NULL,
																						booking_status VARCHAR,
																						booking_status_type VARCHAR,
																						booking_status_type_net_of_covid VARCHAR,
																						se_sale_id VARCHAR,
																						customer_identifier NUMBER,
																						check_in_date DATE,
																						check_out_date DATE,
																						no_nights INTEGER,
																						rooms INTEGER,
																						adult_guests INTEGER,
																						child_guests INTEGER,
																						infant_guests INTEGER,
																						booking_created_date_time TIMESTAMP,
																						booking_completed_date_time TIMESTAMP,
																						booking_cancellation_date DATE,
																						gross_revenue_customer_currency DECIMAL(16, 9),
																						margin_gross_of_toms_customer_currency DECIMAL(16, 9),
																						rate_to_gbp_from_cc DECIMAL(16, 9),
																						customer_currency VARCHAR,
																						vat_on_booking_fee_gbp DECIMAL(16, 9),
																						vat_on_commission_gbp DECIMAL(16, 9),
																						vat_on_payment_surcharge_gbp DECIMAL(16, 9),
																						rate_to_supplier_currency DECIMAL(16, 9),
																						supplier_currency VARCHAR,
																						gross_revenue_gbp_constant_currency DECIMAL(16, 9),
																						margin_gross_of_toms_gbp_constant_currency DECIMAL(16, 9),
																						gross_revenue_eur_constant_currency DECIMAL(16, 9),
																						margin_gross_of_toms_eur_constant_currency DECIMAL(16, 9),
																						territory VARCHAR,
																						device_platform VARCHAR,
																						travel_type VARCHAR,
																						payment_type VARCHAR,
																						product_type VARCHAR,
																						posu_country VARCHAR,
																						posu_city VARCHAR,
																						source_system VARCHAR,
																						last_updated_date_time TIMESTAMP,
	CONSTRAINT pk_tvl_booking
		PRIMARY KEY (
					 booking_id
			)
	)
AS
SELECT
	-- (lineage) metadata for the current job
	'2025-08-04 00:30:00',
	'2025-08-06 14:04:33',
	'ScriptOperator__/usr/local/one-data-pipeline/biapp/task_catalogue/dv/dwh/travelist/tvl_booking.py__20250804T003000__daily_at_00h30',
	CURRENT_TIMESTAMP()::TIMESTAMP,
	CURRENT_TIMESTAMP()::TIMESTAMP,

	-- data columns
	batch.booking_id,
	batch.transaction_id,
	batch.booking_status,
	batch.booking_status_type,
	batch.booking_status_type, -- booking_status_type_net_of_covid,
	batch.se_sale_id,
	batch.customer_identifier,
	batch.check_in_date,
	batch.check_out_date,
	batch.no_nights,
	batch.rooms,
	batch.adult_guests,
	batch.child_guests,
	batch.infant_guests,
	batch.booking_created_date_time,
	batch.booking_completed_date_time,
	batch.booking_cancellation_date,
	batch.gross_revenue_customer_currency,
	batch.margin_gross_of_toms_customer_currency,
	batch.rate_to_gbp_from_cc,
	batch.customer_currency,
	batch.vat_on_booking_fee_gbp,
	batch.vat_on_commission_gbp,
	vat_on_payment_surcharge_gbp,
	batch.rate_to_supplier_currency,
	batch.supplier_currency,
	batch.gross_revenue_gbp_constant_currency,
	batch.margin_gross_of_toms_gbp_constant_currency,
	batch.gross_revenue_eur_constant_currency,
	batch.margin_gross_of_toms_eur_constant_currency,
	batch.territory,
	batch.device_platform,
	batch.travel_type,
	batch.payment_type,
	batch.product_type,
	batch.posu_country,
	batch.posu_city,
	batch.source_system,
	batch.last_updated_date_time

FROM data_vault_mvp_dev_robin.dwh.tvl_booking__step01_model_tvl_bookings_chiasma batch

;

USE ROLE pipelinerunner
;

CREATE OR REPLACE TABLE data_vault_mvp.dwh.tvl_booking COPY GRANTS (
	-- data columns
																	booking_id VARCHAR PRIMARY KEY NOT NULL,
																	transaction_id VARCHAR NOT NULL,
																	booking_status VARCHAR,
																	booking_status_type VARCHAR,
																	booking_status_type_net_of_covid VARCHAR,
																	se_sale_id VARCHAR,
																	customer_identifier NUMBER,
																	check_in_date DATE,
																	check_out_date DATE,
																	no_nights INTEGER,
																	rooms INTEGER,
																	adult_guests INTEGER,
																	child_guests INTEGER,
																	infant_guests INTEGER,
																	booking_created_date_time TIMESTAMP,
																	booking_completed_date_time TIMESTAMP,
																	booking_cancellation_date DATE,
																	gross_revenue_customer_currency DECIMAL(16, 9),
																	margin_gross_of_toms_customer_currency DECIMAL(16, 9),
																	rate_to_gbp_from_cc DECIMAL(16, 9),
																	customer_currency VARCHAR,
																	vat_on_booking_fee_gbp DECIMAL(16, 9),
																	vat_on_commission_gbp DECIMAL(16, 9),
																	vat_on_payment_surcharge_gbp DECIMAL(16, 9),
																	rate_to_supplier_currency DECIMAL(16, 9),
																	supplier_currency VARCHAR,
																	gross_revenue_gbp_constant_currency DECIMAL(16, 9),
																	margin_gross_of_toms_gbp_constant_currency DECIMAL(16, 9),
																	gross_revenue_eur_constant_currency DECIMAL(16, 9),
																	margin_gross_of_toms_eur_constant_currency DECIMAL(16, 9),
																	territory VARCHAR,
																	device_platform VARCHAR,
																	travel_type VARCHAR,
																	payment_type VARCHAR,
																	product_type VARCHAR,
																	posu_country VARCHAR,
																	posu_city VARCHAR,
																	source_system VARCHAR,
																	last_updated_date_time TIMESTAMP
	)
AS
SELECT
	booking_id,
	transaction_id,
	booking_status,
	CASE
		WHEN UPPER(booking_status) = 'BOOKED' THEN 'live'
		WHEN UPPER(booking_status) = 'COMPLETE' THEN 'live'
		WHEN UPPER(booking_status) = 'CANCELLED' THEN 'cancelled'
		ELSE 'other'
	END AS booking_status_type,
	CASE
		WHEN UPPER(booking_status) = 'BOOKED' THEN 'live'
		WHEN UPPER(booking_status) = 'COMPLETE' THEN 'live'
		WHEN UPPER(booking_status) = 'CANCELLED' THEN 'cancelled'
		ELSE 'other'
	END AS booking_status_type_net_of_covid,
	se_sale_id,
	customer_identifier,
	check_in_date,
	check_out_date,
	no_nights,
	rooms,
	adult_guests,
	child_guests,
	infant_guests,
	booking_created_date_time,
	booking_completed_date_time,
	booking_cancellation_date,
	gross_revenue_customer_currency,
	margin_gross_of_toms_customer_currency,
	rate_to_gbp_from_cc,
	customer_currency,
	vat_on_booking_fee_gbp,
	vat_on_commission_gbp,
	vat_on_payment_surcharge_gbp,
	rate_to_supplier_currency,
	supplier_currency,
	gross_revenue_gbp_constant_currency,
	margin_gross_of_toms_gbp_constant_currency,
	gross_revenue_eur_constant_currency,
	margin_gross_of_toms_eur_constant_currency,
	territory,
	device_platform,
	travel_type,
	payment_type,
	product_type,
	posu_country,
	posu_city,
	source_system,
	last_updated_date_time
FROM data_vault_mvp.dwh.tvl_booking tb
;

SELECT *
FROM data_vault_mvp.dwh.tvl_booking tb
;

        CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.se_booking__step12__booking_status_type
        AS
        SELECT
            booking.*,
            CASE
                WHEN UPPER(booking.booking_status) = 'COMPLETE' THEN 'live'
                WHEN (YEAR(booking.booking_completed_date) = '2019'
                    AND booking.cancellation_date >= '2020-03-01'
                    AND UPPER(booking.booking_status) IN ('CANCELLED', 'REFUNDED')) THEN 'live'
                WHEN UPPER(booking.booking_status) IN ('CANCELLED', 'REFUNDED') THEN 'cancelled'
                WHEN UPPER(booking.booking_status) = 'ABANDONED' THEN 'abandoned'
                ELSE 'other'
            END AS booking_status_type,
            CASE
                WHEN UPPER(booking.booking_status) = 'COMPLETE' THEN 'live'
                WHEN UPPER(booking.booking_status) IN ('CANCELLED', 'REFUNDED') THEN 'cancelled'
                WHEN UPPER(booking.booking_status) = 'ABANDONED' THEN 'abandoned'
                ELSE 'other'
            END AS booking_status_type_net_of_covid,
        FROM data_vault_mvp.dwh.se_booking__step11__model_chiasma_bookings_flag booking
        ;

