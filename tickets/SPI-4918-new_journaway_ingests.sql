dataset_task --include 'journaway.wrd_booking_de' --operation LatestRecordsOperation --method 'run' --upstream --start '2024-03-01 00:00:00' --end '2024-03-02 00:00:00'

dataset_task --include 'journaway.wrd_booking_nl' --operation LatestRecordsOperation --method 'run' --upstream --start '2024-02-29 00:00:00' --end '2024-03-03 00:00:00'


CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_sale CLONE latest_vault.cms_mysql.base_sale
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.bedfinder.wrd_booking CLONE latest_vault.bedfinder.wrd_booking
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates CLONE data_vault_mvp.fx.rates
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory CLONE latest_vault.cms_mysql.territory
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.bigxtra_booking CLONE data_vault_mvp.dwh.bigxtra_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.airline_holidays.wrd_booking CLONE latest_vault.airline_holidays.wrd_booking
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.wrd_booking__step02__model_bedfinder_wrd CLONE data_vault_mvp.dwh.wrd_booking__step02__model_bedfinder_wrd
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

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.journaway.wrd_booking CLONE latest_vault.journaway.wrd_booking
;

-- CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.journaway.wrd_booking_de CLONE latest_vault.journaway.wrd_booking_de;
-- CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.journaway.wrd_booking_nl CLONE latest_vault.journaway.wrd_booking_nl;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.offline_3pp_uk_booking CLONE data_vault_mvp.dwh.offline_3pp_uk_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.trading_gsheets.offline_margin CLONE latest_vault.trading_gsheets.offline_margin
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.perfectstay.wrd_booking CLONE latest_vault.perfectstay.wrd_booking
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.style_in_travel.wrd_booking CLONE latest_vault.style_in_travel.wrd_booking
;

-- create tables as there are no rows in files and consolidated ingest doesn't create the tables otherwise

CREATE TABLE IF NOT EXISTS raw_vault_dev_robin.journaway.wrd_booking_nl
(
	-- (lineage) metadata for the current job
	dataset_name                           VARCHAR   NOT NULL,
	dataset_source                         VARCHAR   NOT NULL,
	schedule_interval                      VARCHAR   NOT NULL,
	schedule_tstamp                        TIMESTAMP NOT NULL,
	run_tstamp                             TIMESTAMP NOT NULL,
	loaded_at                              TIMESTAMP NOT NULL,
	filename                               VARCHAR   NOT NULL,
	file_row_number                        INT       NOT NULL,

	-- data columns
	extract_metadata                       VARIANT,
	booking_id                             VARCHAR,
	external_reference_id                  VARCHAR,
	booking_status                         VARCHAR,
	se_sale_id                             VARCHAR,
	customer_identifier                    VARCHAR,
	check_in_date                          VARCHAR,
	check_out_date                         VARCHAR,
	no_nights                              VARCHAR,
	rooms                                  VARCHAR,
	adult_guests                           VARCHAR,
	child_guests                           VARCHAR,
	infant_guests                          VARCHAR,
	booking_created_date_time              VARCHAR,
	booking_completed_date_time            VARCHAR,
	gross_revenue_customer_currency        VARCHAR,
	margin_gross_of_toms_customer_currency VARCHAR,
	rate_to_gbp_from_cc                    VARCHAR,
	customer_currency                      VARCHAR,
	rate_to_supplier_currency              VARCHAR,
	supplier_currency                      VARCHAR,
	territory                              VARCHAR,
	device_platform                        VARCHAR,
	travel_type                            VARCHAR,
	payment_type                           VARCHAR,
	product_type                           VARCHAR,
	posu_country                           VARCHAR,
	posu_city                              VARCHAR,
	last_updated_date_time                 VARCHAR
)
	CLUSTER BY (TO_DATE(schedule_tstamp))
;


CREATE TABLE IF NOT EXISTS hygiene_vault_dev_robin.journaway.wrd_booking_nl
(
	-- (lineage) metadata for the current job
	schedule_tstamp                                            TIMESTAMP NOT NULL,
	run_tstamp                                                 TIMESTAMP NOT NULL,
	operation_id                                               VARCHAR   NOT NULL,
	created_at                                                 TIMESTAMP NOT NULL,
	updated_at                                                 TIMESTAMP NOT NULL,

	-- (lineage) original metadata of row itself
	row_dataset_name                                           VARCHAR   NOT NULL,
	row_dataset_source                                         VARCHAR   NOT NULL,
	row_loaded_at                                              TIMESTAMP NOT NULL,
	row_schedule_tstamp                                        TIMESTAMP NOT NULL,
	row_run_tstamp                                             TIMESTAMP NOT NULL,
	row_filename                                               VARCHAR   NOT NULL,
	row_file_row_number                                        INT       NOT NULL,
	row_extract_metadata                                       VARIANT,


	-- transformed columns
	remote_filename                                            VARCHAR,
	remote_file_row_number                                     INT,
	booking_id                                                 VARCHAR,
	last_updated_date_time                                     TIMESTAMP,
	travel_type                                                VARCHAR,

	-- original columns
	booking_id__o                                              VARCHAR,
	external_reference_id                                      VARCHAR,
	booking_status                                             VARCHAR,
	se_sale_id                                                 VARCHAR,
	customer_identifier                                        VARCHAR,
	check_in_date                                              DATE,
	check_out_date                                             DATE,
	no_nights                                                  INT,
	rooms                                                      INT,
	adult_guests                                               INT,
	child_guests                                               INT,
	infant_guests                                              INT,
	booking_created_date_time                                  TIMESTAMP,
	booking_completed_date_time                                TIMESTAMP,
	gross_revenue_customer_currency                            NUMBER(16, 9),
	margin_gross_of_toms_customer_currency                     NUMBER(16, 9),
	rate_to_gbp_from_cc                                        NUMBER(16, 9),
	customer_currency                                          VARCHAR,
	rate_to_supplier_currency                                  NUMBER(16, 9),
	supplier_currency                                          VARCHAR,
	territory                                                  VARCHAR,
	device_platform                                            VARCHAR,
	travel_type__o                                             VARCHAR,
	payment_type                                               VARCHAR,
	product_type                                               VARCHAR,
	posu_country                                               VARCHAR,
	posu_city                                                  VARCHAR,
	last_updated_date_time__o                                  TIMESTAMP,

	-- validation columns
	failed_some_validation                                     INT,
	fails_validation__remote_filename__expected_nonnull        INT,
	fails_validation__remote_file_row_number__expected_nonnull INT,
	fails_validation__booking_id__expected_nonnull             INT,
	fails_validation__last_updated_date_time__expected_nonnull INT
)
;

CREATE TABLE IF NOT EXISTS latest_vault_dev_robin.journaway.wrd_booking_nl
(
	-- (lineage) metadata for the current job
	schedule_tstamp                        TIMESTAMP NOT NULL,
	run_tstamp                             TIMESTAMP NOT NULL,
	operation_id                           VARCHAR   NOT NULL,
	created_at                             TIMESTAMP NOT NULL,
	updated_at                             TIMESTAMP NOT NULL,

	-- (lineage) original metadata of row itself
	row_dataset_name                       VARCHAR   NOT NULL,
	row_dataset_source                     VARCHAR   NOT NULL,
	row_loaded_at                          TIMESTAMP NOT NULL,
	row_schedule_tstamp                    TIMESTAMP NOT NULL,
	row_run_tstamp                         TIMESTAMP NOT NULL,
	row_filename                           VARCHAR   NOT NULL,
	row_file_row_number                    INT       NOT NULL,
	row_extract_metadata                   VARIANT,

	-- transformed columns
	remote_filename                        VARCHAR,
	remote_file_row_number                 INT,
	booking_id                             VARCHAR,
	last_updated_date_time                 TIMESTAMP,
	travel_type                            VARCHAR,

	-- original columns
	booking_id__o                          VARCHAR,
	external_reference_id                  VARCHAR,
	booking_status                         VARCHAR,
	se_sale_id                             VARCHAR,
	customer_identifier                    VARCHAR,
	check_in_date                          DATE,
	check_out_date                         DATE,
	no_nights                              INT,
	rooms                                  INT,
	adult_guests                           INT,
	child_guests                           INT,
	infant_guests                          INT,
	booking_created_date_time              TIMESTAMP,
	booking_completed_date_time            TIMESTAMP,
	gross_revenue_customer_currency        NUMBER(16, 9),
	margin_gross_of_toms_customer_currency NUMBER(16, 9),
	rate_to_gbp_from_cc                    NUMBER(16, 9),
	customer_currency                      VARCHAR,
	rate_to_supplier_currency              NUMBER(16, 9),
	supplier_currency                      VARCHAR,
	territory                              VARCHAR,
	device_platform                        VARCHAR,
	travel_type__o                         VARCHAR,
	payment_type                           VARCHAR,
	product_type                           VARCHAR,
	posu_country                           VARCHAR,
	posu_city                              VARCHAR,
	last_updated_date_time__o              TIMESTAMP,
	CONSTRAINT pk_1 PRIMARY KEY (booking_id__o)
)
;


self_describing_task --include 'biapp/task_catalogue/dv/dwh/wrd/wrd_booking.py'  --method 'run' --start '2024-03-03 00:00:00' --end '2024-03-03 00:00:00'


------------------------------------------------------------------------------------------------------------------------
-- post dep steps
-- need to run the ddls for nl as there are no bookings as pipelinerunner



CREATE TABLE IF NOT EXISTS raw_vault.journaway.wrd_booking_nl
(
	-- (lineage) metadata for the current job
	dataset_name                           VARCHAR   NOT NULL,
	dataset_source                         VARCHAR   NOT NULL,
	schedule_interval                      VARCHAR   NOT NULL,
	schedule_tstamp                        TIMESTAMP NOT NULL,
	run_tstamp                             TIMESTAMP NOT NULL,
	loaded_at                              TIMESTAMP NOT NULL,
	filename                               VARCHAR   NOT NULL,
	file_row_number                        INT       NOT NULL,

	-- data columns
	extract_metadata                       VARIANT,
	booking_id                             VARCHAR,
	external_reference_id                  VARCHAR,
	booking_status                         VARCHAR,
	se_sale_id                             VARCHAR,
	customer_identifier                    VARCHAR,
	check_in_date                          VARCHAR,
	check_out_date                         VARCHAR,
	no_nights                              VARCHAR,
	rooms                                  VARCHAR,
	adult_guests                           VARCHAR,
	child_guests                           VARCHAR,
	infant_guests                          VARCHAR,
	booking_created_date_time              VARCHAR,
	booking_completed_date_time            VARCHAR,
	gross_revenue_customer_currency        VARCHAR,
	margin_gross_of_toms_customer_currency VARCHAR,
	rate_to_gbp_from_cc                    VARCHAR,
	customer_currency                      VARCHAR,
	rate_to_supplier_currency              VARCHAR,
	supplier_currency                      VARCHAR,
	territory                              VARCHAR,
	device_platform                        VARCHAR,
	travel_type                            VARCHAR,
	payment_type                           VARCHAR,
	product_type                           VARCHAR,
	posu_country                           VARCHAR,
	posu_city                              VARCHAR,
	last_updated_date_time                 VARCHAR
)
	CLUSTER BY (TO_DATE(schedule_tstamp))
;


CREATE TABLE IF NOT EXISTS hygiene_vault.journaway.wrd_booking_nl
(
	-- (lineage) metadata for the current job
	schedule_tstamp                                            TIMESTAMP NOT NULL,
	run_tstamp                                                 TIMESTAMP NOT NULL,
	operation_id                                               VARCHAR   NOT NULL,
	created_at                                                 TIMESTAMP NOT NULL,
	updated_at                                                 TIMESTAMP NOT NULL,

	-- (lineage) original metadata of row itself
	row_dataset_name                                           VARCHAR   NOT NULL,
	row_dataset_source                                         VARCHAR   NOT NULL,
	row_loaded_at                                              TIMESTAMP NOT NULL,
	row_schedule_tstamp                                        TIMESTAMP NOT NULL,
	row_run_tstamp                                             TIMESTAMP NOT NULL,
	row_filename                                               VARCHAR   NOT NULL,
	row_file_row_number                                        INT       NOT NULL,
	row_extract_metadata                                       VARIANT,


	-- transformed columns
	remote_filename                                            VARCHAR,
	remote_file_row_number                                     INT,
	booking_id                                                 VARCHAR,
	last_updated_date_time                                     TIMESTAMP,
	travel_type                                                VARCHAR,

	-- original columns
	booking_id__o                                              VARCHAR,
	external_reference_id                                      VARCHAR,
	booking_status                                             VARCHAR,
	se_sale_id                                                 VARCHAR,
	customer_identifier                                        VARCHAR,
	check_in_date                                              DATE,
	check_out_date                                             DATE,
	no_nights                                                  INT,
	rooms                                                      INT,
	adult_guests                                               INT,
	child_guests                                               INT,
	infant_guests                                              INT,
	booking_created_date_time                                  TIMESTAMP,
	booking_completed_date_time                                TIMESTAMP,
	gross_revenue_customer_currency                            NUMBER(16, 9),
	margin_gross_of_toms_customer_currency                     NUMBER(16, 9),
	rate_to_gbp_from_cc                                        NUMBER(16, 9),
	customer_currency                                          VARCHAR,
	rate_to_supplier_currency                                  NUMBER(16, 9),
	supplier_currency                                          VARCHAR,
	territory                                                  VARCHAR,
	device_platform                                            VARCHAR,
	travel_type__o                                             VARCHAR,
	payment_type                                               VARCHAR,
	product_type                                               VARCHAR,
	posu_country                                               VARCHAR,
	posu_city                                                  VARCHAR,
	last_updated_date_time__o                                  TIMESTAMP,

	-- validation columns
	failed_some_validation                                     INT,
	fails_validation__remote_filename__expected_nonnull        INT,
	fails_validation__remote_file_row_number__expected_nonnull INT,
	fails_validation__booking_id__expected_nonnull             INT,
	fails_validation__last_updated_date_time__expected_nonnull INT
)
;

CREATE TABLE IF NOT EXISTS latest_vault.journaway.wrd_booking_nl
(
	-- (lineage) metadata for the current job
	schedule_tstamp                        TIMESTAMP NOT NULL,
	run_tstamp                             TIMESTAMP NOT NULL,
	operation_id                           VARCHAR   NOT NULL,
	created_at                             TIMESTAMP NOT NULL,
	updated_at                             TIMESTAMP NOT NULL,

	-- (lineage) original metadata of row itself
	row_dataset_name                       VARCHAR   NOT NULL,
	row_dataset_source                     VARCHAR   NOT NULL,
	row_loaded_at                          TIMESTAMP NOT NULL,
	row_schedule_tstamp                    TIMESTAMP NOT NULL,
	row_run_tstamp                         TIMESTAMP NOT NULL,
	row_filename                           VARCHAR   NOT NULL,
	row_file_row_number                    INT       NOT NULL,
	row_extract_metadata                   VARIANT,

	-- transformed columns
	remote_filename                        VARCHAR,
	remote_file_row_number                 INT,
	booking_id                             VARCHAR,
	last_updated_date_time                 TIMESTAMP,
	travel_type                            VARCHAR,

	-- original columns
	booking_id__o                          VARCHAR,
	external_reference_id                  VARCHAR,
	booking_status                         VARCHAR,
	se_sale_id                             VARCHAR,
	customer_identifier                    VARCHAR,
	check_in_date                          DATE,
	check_out_date                         DATE,
	no_nights                              INT,
	rooms                                  INT,
	adult_guests                           INT,
	child_guests                           INT,
	infant_guests                          INT,
	booking_created_date_time              TIMESTAMP,
	booking_completed_date_time            TIMESTAMP,
	gross_revenue_customer_currency        NUMBER(16, 9),
	margin_gross_of_toms_customer_currency NUMBER(16, 9),
	rate_to_gbp_from_cc                    NUMBER(16, 9),
	customer_currency                      VARCHAR,
	rate_to_supplier_currency              NUMBER(16, 9),
	supplier_currency                      VARCHAR,
	territory                              VARCHAR,
	device_platform                        VARCHAR,
	travel_type__o                         VARCHAR,
	payment_type                           VARCHAR,
	product_type                           VARCHAR,
	posu_country                           VARCHAR,
	posu_city                              VARCHAR,
	last_updated_date_time__o              TIMESTAMP,
	CONSTRAINT pk_1 PRIMARY KEY (booking_id__o)
)
;

SELECT *
FROM latest_vault_dev_robin.journaway.wrd_booking_de
;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.wrd_booking
WHERE wrd_booking.booking_id = 'JNW-17786588510'
;

SELECT *
FROM dbt.bi_staging.base_tableau_gsheets__tableau_channel_costs
;


SELECT
	tcc.original_affiliate_territory,
	SUM(tcc.gbp_cost)
FROM dbt.bi_staging.base_tableau_gsheets__tableau_channel_costs tcc
GROUP BY 1
;

SELECT
	COUNT(*)
FROM dbt.bi_staging.base_tableau_gsheets__tableau_channel_costs tcc
;


SELECT min(ua.date) FROM data_vault_mvp.dwh.user_activity ua