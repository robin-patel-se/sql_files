-- test ingest with no table

DROP TABLE latest_vault_dev_robin.iterable.campaign
;

dataset_task --include 'iterable.campaign' --operation LatestRecordsOperation --method 'run' --upstream --start '2020-07-15 00:30:00' --end '2020-07-15 00:30:00'


SELECT *
FROM latest_vault_dev_robin.iterable.campaign
;

------------------------------------------------------------------------------------------------------------------------
-- test updates to prod table

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.iterable.campaign CLONE hygiene_vault.iterable.campaign
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.iterable.campaign_temp CLONE hygiene_vault.iterable.campaign
;


CREATE OR REPLACE TABLE hygiene_vault_dev_robin.iterable.campaign
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
	id                                     INT,
	campaign_created_at                    TIMESTAMP,
	campaign_updated_at                    TIMESTAMP,
	start_at                               TIMESTAMP,
	ended_at                               TIMESTAMP,
	name                                   VARCHAR,
	template_id                            INT,
	message_medium                         VARCHAR,
	created_by_user_id                     VARCHAR,
	updated_by_user_id                     VARCHAR,
	campaign_state                         VARCHAR,
	list_ids                               ARRAY,
	suppression_list_ids                   ARRAY,
	send_size                              INT,
	labels                                 ARRAY,
	type                                   VARCHAR,
	splittable_email_name                  VARCHAR,
	mapped_crm_date                        VARCHAR,
	mapped_territory                       VARCHAR,
	mapped_objective                       VARCHAR,
	mapped_platform                        VARCHAR,
	mapped_campaign_type                   VARCHAR,
	mapped_campaign                        VARCHAR,
	mapped_promo_type                      VARCHAR,
	mapped_theme                           VARCHAR,
	mapped_comment                         VARCHAR,
	mapped_segment                         VARCHAR,

	-- original columns
	record                                 VARIANT,

	-- validation columns
	failed_some_validation                 INT,
	fails_validation__id__expected_nonnull INT
)
;

INSERT INTO hygiene_vault_dev_robin.iterable.campaign
SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,

	row_dataset_name,
	row_dataset_source,
	row_loaded_at,
	row_schedule_tstamp,
	row_run_tstamp,
	row_filename,
	row_file_row_number,
	row_extract_metadata,

	id,
	campaign_created_at,
	campaign_updated_at,
	start_at,
	ended_at,
	name,
	template_id,
	message_medium,
	created_by_user_id,
	updated_by_user_id,
	campaign_state,
	list_ids,
	suppression_list_ids,
	send_size,
	labels,
	type,
	splittable_email_name,
	mapped_crm_date,
	mapped_territory,
	mapped_objective,
	mapped_platform,
	SPLIT_PART(splittable_email_name, '_', 4)::VARCHAR AS mapped_campaign_type,
	mapped_campaign,
	SPLIT_PART(splittable_email_name, '_', 5)::VARCHAR AS mapped_promo_type,
	SPLIT_PART(splittable_email_name, '_', 6)::VARCHAR AS mapped_theme,
	SPLIT_PART(splittable_email_name, '_', 7)::VARCHAR AS mapped_comment,
	SPLIT_PART(splittable_email_name, '_', 8)::VARCHAR AS mapped_segment,
	record,
	fails_validation__id__expected_nonnull,
	failed_some_validation
FROM hygiene_vault_dev_robin.iterable.campaign_temp
;

SELECT *
FROM hygiene_vault_dev_robin.iterable.campaign
;


dataset_task --include 'iterable.campaign' --operation HygieneOperation --method 'run' --start '2020-07-15 00:30:00' --end '2020-07-15 00:30:00'



CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.campaign CLONE latest_vault.iterable.campaign
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.iterable.campaign_temp CLONE latest_vault.iterable.campaign
;


CREATE OR REPLACE TABLE latest_vault_dev_robin.iterable.campaign
(
	-- (lineage) metadata for the current job
	schedule_tstamp       TIMESTAMP NOT NULL,
	run_tstamp            TIMESTAMP NOT NULL,
	operation_id          VARCHAR   NOT NULL,
	created_at            TIMESTAMP NOT NULL,
	updated_at            TIMESTAMP NOT NULL,

	-- (lineage) original metadata of row itself
	row_dataset_name      VARCHAR   NOT NULL,
	row_dataset_source    VARCHAR   NOT NULL,
	row_loaded_at         TIMESTAMP NOT NULL,
	row_schedule_tstamp   TIMESTAMP NOT NULL,
	row_run_tstamp        TIMESTAMP NOT NULL,
	row_filename          VARCHAR   NOT NULL,
	row_file_row_number   INT       NOT NULL,
	row_extract_metadata  VARIANT,

	-- transformed columns
	id                    INT,
	campaign_created_at   TIMESTAMP,
	campaign_updated_at   TIMESTAMP,
	start_at              TIMESTAMP,
	ended_at              TIMESTAMP,
	name                  VARCHAR,
	template_id           INT,
	message_medium        VARCHAR,
	created_by_user_id    VARCHAR,
	updated_by_user_id    VARCHAR,
	campaign_state        VARCHAR,
	list_ids              ARRAY,
	suppression_list_ids  ARRAY,
	send_size             INT,
	labels                ARRAY,
	type                  VARCHAR,
	splittable_email_name VARCHAR,
	mapped_crm_date       VARCHAR,
	mapped_territory      VARCHAR,
	mapped_objective      VARCHAR,
	mapped_platform       VARCHAR,
	mapped_campaign_type  VARCHAR,
	mapped_campaign       VARCHAR,
	mapped_promo_type     VARCHAR,
	mapped_theme          VARCHAR,
	mapped_comment        VARCHAR,
	mapped_segment        VARCHAR,

	-- original columns
	record                VARIANT,
	CONSTRAINT pk_1 PRIMARY KEY (id)
)
;


INSERT INTO latest_vault_dev_robin.iterable.campaign
SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	row_dataset_name,
	row_dataset_source,
	row_loaded_at,
	row_schedule_tstamp,
	row_run_tstamp,
	row_filename,
	row_file_row_number,
	row_extract_metadata,
	id,
	campaign_created_at,
	campaign_updated_at,
	start_at,
	ended_at,
	name,
	template_id,
	message_medium,
	created_by_user_id,
	updated_by_user_id,
	campaign_state,
	list_ids,
	suppression_list_ids,
	send_size,
	labels,
	type,
	splittable_email_name,
	mapped_crm_date,
	mapped_territory,
	mapped_objective,
	mapped_platform,
	SPLIT_PART(splittable_email_name, '_', 4)::VARCHAR AS mapped_campaign_type,
	mapped_campaign,
	SPLIT_PART(splittable_email_name, '_', 5)::VARCHAR AS mapped_promo_type,
	SPLIT_PART(splittable_email_name, '_', 6)::VARCHAR AS mapped_theme,
	SPLIT_PART(splittable_email_name, '_', 7)::VARCHAR AS mapped_comment,
	SPLIT_PART(splittable_email_name, '_', 8)::VARCHAR AS mapped_segment,
	record
FROM latest_vault_dev_robin.iterable.campaign_temp
;

SELECT *
FROM latest_vault_dev_robin.iterable.campaign
;

SELECT *
FROM latest_vault.iterable.campaign
;

dataset_task --include 'iterable.campaign' --operation LatestRecordsOperation --method 'run' --start '2020-07-15 00:30:00' --end '2020-07-15 00:30:00'

------------------------------------------------------------------------------------------------------------------------

-- post deployment steps

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault.iterable.campaign_20240219 CLONE hygiene_vault.iterable.campaign
;


CREATE OR REPLACE TABLE hygiene_vault.iterable.campaign
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
	id                                     INT,
	campaign_created_at                    TIMESTAMP,
	campaign_updated_at                    TIMESTAMP,
	start_at                               TIMESTAMP,
	ended_at                               TIMESTAMP,
	name                                   VARCHAR,
	template_id                            INT,
	message_medium                         VARCHAR,
	created_by_user_id                     VARCHAR,
	updated_by_user_id                     VARCHAR,
	campaign_state                         VARCHAR,
	list_ids                               ARRAY,
	suppression_list_ids                   ARRAY,
	send_size                              INT,
	labels                                 ARRAY,
	type                                   VARCHAR,
	splittable_email_name                  VARCHAR,
	mapped_crm_date                        VARCHAR,
	mapped_territory                       VARCHAR,
	mapped_objective                       VARCHAR,
	mapped_platform                        VARCHAR,
	mapped_campaign_type                   VARCHAR,
	mapped_campaign                        VARCHAR,
	mapped_promo_type                      VARCHAR,
	mapped_theme                           VARCHAR,
	mapped_comment                         VARCHAR,
	mapped_segment                         VARCHAR,

	-- original columns
	record                                 VARIANT,

	-- validation columns
	failed_some_validation                 INT,
	fails_validation__id__expected_nonnull INT
)
;

INSERT INTO hygiene_vault.iterable.campaign
SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,

	row_dataset_name,
	row_dataset_source,
	row_loaded_at,
	row_schedule_tstamp,
	row_run_tstamp,
	row_filename,
	row_file_row_number,
	row_extract_metadata,

	id,
	campaign_created_at,
	campaign_updated_at,
	start_at,
	ended_at,
	name,
	template_id,
	message_medium,
	created_by_user_id,
	updated_by_user_id,
	campaign_state,
	list_ids,
	suppression_list_ids,
	send_size,
	labels,
	type,
	splittable_email_name,
	mapped_crm_date,
	mapped_territory,
	mapped_objective,
	mapped_platform,
	SPLIT_PART(splittable_email_name, '_', 4)::VARCHAR AS mapped_campaign_type,
	mapped_campaign,
	SPLIT_PART(splittable_email_name, '_', 5)::VARCHAR AS mapped_promo_type,
	SPLIT_PART(splittable_email_name, '_', 6)::VARCHAR AS mapped_theme,
	SPLIT_PART(splittable_email_name, '_', 7)::VARCHAR AS mapped_comment,
	SPLIT_PART(splittable_email_name, '_', 8)::VARCHAR AS mapped_segment,
	record,
	fails_validation__id__expected_nonnull,
	failed_some_validation
FROM hygiene_vault.iterable.campaign_20240219
;


CREATE OR REPLACE TRANSIENT TABLE latest_vault.iterable.campaign_20240218 CLONE latest_vault.iterable.campaign
;


CREATE OR REPLACE TABLE latest_vault.iterable.campaign
(
	-- (lineage) metadata for the current job
	schedule_tstamp       TIMESTAMP NOT NULL,
	run_tstamp            TIMESTAMP NOT NULL,
	operation_id          VARCHAR   NOT NULL,
	created_at            TIMESTAMP NOT NULL,
	updated_at            TIMESTAMP NOT NULL,

	-- (lineage) original metadata of row itself
	row_dataset_name      VARCHAR   NOT NULL,
	row_dataset_source    VARCHAR   NOT NULL,
	row_loaded_at         TIMESTAMP NOT NULL,
	row_schedule_tstamp   TIMESTAMP NOT NULL,
	row_run_tstamp        TIMESTAMP NOT NULL,
	row_filename          VARCHAR   NOT NULL,
	row_file_row_number   INT       NOT NULL,
	row_extract_metadata  VARIANT,

	-- transformed columns
	id                    INT,
	campaign_created_at   TIMESTAMP,
	campaign_updated_at   TIMESTAMP,
	start_at              TIMESTAMP,
	ended_at              TIMESTAMP,
	name                  VARCHAR,
	template_id           INT,
	message_medium        VARCHAR,
	created_by_user_id    VARCHAR,
	updated_by_user_id    VARCHAR,
	campaign_state        VARCHAR,
	list_ids              ARRAY,
	suppression_list_ids  ARRAY,
	send_size             INT,
	labels                ARRAY,
	type                  VARCHAR,
	splittable_email_name VARCHAR,
	mapped_crm_date       VARCHAR,
	mapped_territory      VARCHAR,
	mapped_objective      VARCHAR,
	mapped_platform       VARCHAR,
	mapped_campaign_type  VARCHAR,
	mapped_campaign       VARCHAR,
	mapped_promo_type     VARCHAR,
	mapped_theme          VARCHAR,
	mapped_comment        VARCHAR,
	mapped_segment        VARCHAR,

	-- original columns
	record                VARIANT,
	CONSTRAINT pk_1 PRIMARY KEY (id)
)
;


INSERT INTO latest_vault.iterable.campaign
SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	row_dataset_name,
	row_dataset_source,
	row_loaded_at,
	row_schedule_tstamp,
	row_run_tstamp,
	row_filename,
	row_file_row_number,
	row_extract_metadata,
	id,
	campaign_created_at,
	campaign_updated_at,
	start_at,
	ended_at,
	name,
	template_id,
	message_medium,
	created_by_user_id,
	updated_by_user_id,
	campaign_state,
	list_ids,
	suppression_list_ids,
	send_size,
	labels,
	type,
	splittable_email_name,
	mapped_crm_date,
	mapped_territory,
	mapped_objective,
	mapped_platform,
	SPLIT_PART(splittable_email_name, '_', 4)::VARCHAR AS mapped_campaign_type,
	mapped_campaign,
	SPLIT_PART(splittable_email_name, '_', 5)::VARCHAR AS mapped_promo_type,
	SPLIT_PART(splittable_email_name, '_', 6)::VARCHAR AS mapped_theme,
	SPLIT_PART(splittable_email_name, '_', 7)::VARCHAR AS mapped_comment,
	SPLIT_PART(splittable_email_name, '_', 8)::VARCHAR AS mapped_segment,
	record
FROM latest_vault.iterable.campaign_20240218
;


SELECT *
FROM hygiene_vault.iterable.campaign
;

SELECT *
FROM hygiene_vault.iterable.campaign_20240219
;


SELECT *
FROM latest_vault.iterable.campaign
;


SELECT *
FROM latest_vault.iterable.campaign_20240219
;

-- Rerun iterable campaign for current run


SELECT
	name,
	COUNT(*)
FROM latest_vault.iterable.campaign c
WHERE LOWER(c.name) LIKE '%athena%'
  AND LOWER(c.name) NOT LIKE '%ame_athena%'
  AND LOWER(c.name) NOT LIKE '%core_athena%'
  AND LOWER(c.name) NOT LIKE '%partner_athena%'
  AND LOWER(c.name) NOT LIKE '%test_athena%'
GROUP BY 1
;



SELECT
	name,
	COUNT(*)
FROM latest_vault.iterable.campaign c
WHERE LOWER(c.name) LIKE '%%ame%' AND
	  CASE
		  WHEN LOWER(c.name) = 'ame_abandon_basket' THEN 'AbandonBasket'
		  WHEN LOWER(c.name) = 'ame_abandon_basket_bookinglink' THEN 'AbandonBasketbookingLink'
		  WHEN LOWER(c.name) = 'ame_abandon_browse_daily' THEN 'AbandonBrowseDaily'
		  WHEN LOWER(c.name) LIKE 'ame_abandon_browse_weekly_copy%' THEN 'AbandonBrowseWeekly'
		  WHEN LOWER(c.name) = 'ame_welcome_01_sign_up' THEN 'WelcomeSignUp'
		  WHEN LOWER(c.name) = 'ame_deal_improvement' THEN 'DealImprovement'
		  WHEN LOWER(c.name) = 'ame_keyword_search' THEN 'KeywordSearch'
		  WHEN LOWER(c.name) = 'ame_deal_spotlight' THEN 'DealSpotlight'
		  WHEN LOWER(c.name) = 'ame_welcome_back' THEN 'WelcomeBack'
		  WHEN LOWER(c.name) = 'ame_wishlist_specific_deal' THEN 'WishlistDeal'
		  WHEN LOWER(c.name) = 'ame_wishlist_destination' THEN 'WishlistDestination'
		  WHEN LOWER(c.name) = 'ame_destination_spotlight' THEN 'DestinationSpotlight'
		  WHEN LOWER(c.name) = 'ame_welcome_04_top_ten' THEN 'Welcome4Top10'
		  WHEN LOWER(c.name) = 'ame_welcome_02_inspiration' THEN 'Welcome2Inspiration'
		  WHEN LOWER(c.name) = 'ame_welcome_03_site_education' THEN 'Welcome3SiteEducation'
		  WHEN LOWER(c.name) = 'ame_welcome_05_trust' THEN 'Welcome5Trust'
		  WHEN LOWER(c.name) = 'ame_date_spotlight' THEN 'DateSpotlight'
		  WHEN LOWER(c.name) LIKE 'ame%' THEN 'OtherAME'
	  END IS NULL
GROUP BY 1
ORDER BY 2 DESC
;


SELECT
	icr.campaign_name,
	SUM(icr.email_sends)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icr
WHERE icr.ame_calculated_campaign_name IS NULL
  AND LOWER(icr.campaign_name) NOT LIKE '%_ame_%'
  AND LOWER(icr.campaign_name) NOT LIKE 'ame_%'
  AND LOWER(icr.campaign_name) LIKE '%ame%'
GROUP BY 1
ORDER BY 2 DESC;



SELECT
	icr.campaign_name,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.iterable_crm_reporting icr
WHERE icr.is_athena = FALSE
AND LOWER(icr.campaign_name) LIKE '%athena%'
GROUP BY 1
ORDER BY 2 DESC
;

------------------------------------------------------------------------------------------------------------------------
USE ROLE pipelinerunner;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault.iterable.campaign_20240219 CLONE hygiene_vault.iterable.campaign
;

CREATE OR REPLACE TABLE hygiene_vault.iterable.campaign
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
	id                                     INT,
	campaign_created_at                    TIMESTAMP,
	campaign_updated_at                    TIMESTAMP,
	start_at                               TIMESTAMP,
	ended_at                               TIMESTAMP,
	name                                   VARCHAR,
	template_id                            INT,
	message_medium                         VARCHAR,
	created_by_user_id                     VARCHAR,
	updated_by_user_id                     VARCHAR,
	campaign_state                         VARCHAR,
	list_ids                               ARRAY,
	suppression_list_ids                   ARRAY,
	send_size                              INT,
	labels                                 ARRAY,
	type                                   VARCHAR,
	splittable_email_name                  VARCHAR,
	mapped_crm_date                        VARCHAR,
	mapped_territory                       VARCHAR,
	mapped_objective                       VARCHAR,
	mapped_platform                        VARCHAR,
	mapped_campaign_type                   VARCHAR,
	mapped_campaign                        VARCHAR,
	mapped_promo_type                      VARCHAR,
	mapped_theme                           VARCHAR,
	mapped_comment                         VARCHAR,
	mapped_segment                         VARCHAR,

	-- original columns
	record                                 VARIANT,

	-- validation columns
	failed_some_validation                 INT,
	fails_validation__id__expected_nonnull INT
)
;

INSERT INTO hygiene_vault.iterable.campaign
SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,

	row_dataset_name,
	row_dataset_source,
	row_loaded_at,
	row_schedule_tstamp,
	row_run_tstamp,
	row_filename,
	row_file_row_number,
	row_extract_metadata,

	id,
	campaign_created_at,
	campaign_updated_at,
	start_at,
	ended_at,
	name,
	template_id,
	message_medium,
	created_by_user_id,
	updated_by_user_id,
	campaign_state,
	list_ids,
	suppression_list_ids,
	send_size,
	labels,
	type,
	splittable_email_name,
	mapped_crm_date,
	mapped_territory,
	mapped_objective,
	mapped_platform,
	SPLIT_PART(splittable_email_name, '_', 4)::VARCHAR AS mapped_campaign_type,
	mapped_campaign,
	SPLIT_PART(splittable_email_name, '_', 5)::VARCHAR AS mapped_promo_type,
	SPLIT_PART(splittable_email_name, '_', 6)::VARCHAR AS mapped_theme,
	SPLIT_PART(splittable_email_name, '_', 7)::VARCHAR AS mapped_comment,
	SPLIT_PART(splittable_email_name, '_', 8)::VARCHAR AS mapped_segment,
	record,
	fails_validation__id__expected_nonnull,
	failed_some_validation
FROM hygiene_vault.iterable.campaign_20240219
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault.iterable.campaign_20240218 CLONE latest_vault.iterable.campaign
;

CREATE OR REPLACE TABLE latest_vault.iterable.campaign
(
	-- (lineage) metadata for the current job
	schedule_tstamp       TIMESTAMP NOT NULL,
	run_tstamp            TIMESTAMP NOT NULL,
	operation_id          VARCHAR   NOT NULL,
	created_at            TIMESTAMP NOT NULL,
	updated_at            TIMESTAMP NOT NULL,

	-- (lineage) original metadata of row itself
	row_dataset_name      VARCHAR   NOT NULL,
	row_dataset_source    VARCHAR   NOT NULL,
	row_loaded_at         TIMESTAMP NOT NULL,
	row_schedule_tstamp   TIMESTAMP NOT NULL,
	row_run_tstamp        TIMESTAMP NOT NULL,
	row_filename          VARCHAR   NOT NULL,
	row_file_row_number   INT       NOT NULL,
	row_extract_metadata  VARIANT,

	-- transformed columns
	id                    INT,
	campaign_created_at   TIMESTAMP,
	campaign_updated_at   TIMESTAMP,
	start_at              TIMESTAMP,
	ended_at              TIMESTAMP,
	name                  VARCHAR,
	template_id           INT,
	message_medium        VARCHAR,
	created_by_user_id    VARCHAR,
	updated_by_user_id    VARCHAR,
	campaign_state        VARCHAR,
	list_ids              ARRAY,
	suppression_list_ids  ARRAY,
	send_size             INT,
	labels                ARRAY,
	type                  VARCHAR,
	splittable_email_name VARCHAR,
	mapped_crm_date       VARCHAR,
	mapped_territory      VARCHAR,
	mapped_objective      VARCHAR,
	mapped_platform       VARCHAR,
	mapped_campaign_type  VARCHAR,
	mapped_campaign       VARCHAR,
	mapped_promo_type     VARCHAR,
	mapped_theme          VARCHAR,
	mapped_comment        VARCHAR,
	mapped_segment        VARCHAR,

	-- original columns
	record                VARIANT,
	CONSTRAINT pk_1 PRIMARY KEY (id)
)
;

INSERT INTO latest_vault.iterable.campaign
SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	row_dataset_name,
	row_dataset_source,
	row_loaded_at,
	row_schedule_tstamp,
	row_run_tstamp,
	row_filename,
	row_file_row_number,
	row_extract_metadata,
	id,
	campaign_created_at,
	campaign_updated_at,
	start_at,
	ended_at,
	name,
	template_id,
	message_medium,
	created_by_user_id,
	updated_by_user_id,
	campaign_state,
	list_ids,
	suppression_list_ids,
	send_size,
	labels,
	type,
	splittable_email_name,
	mapped_crm_date,
	mapped_territory,
	mapped_objective,
	mapped_platform,
	SPLIT_PART(splittable_email_name, '_', 4)::VARCHAR AS mapped_campaign_type,
	mapped_campaign,
	SPLIT_PART(splittable_email_name, '_', 5)::VARCHAR AS mapped_promo_type,
	SPLIT_PART(splittable_email_name, '_', 6)::VARCHAR AS mapped_theme,
	SPLIT_PART(splittable_email_name, '_', 7)::VARCHAR AS mapped_comment,
	SPLIT_PART(splittable_email_name, '_', 8)::VARCHAR AS mapped_segment,
	record
FROM latest_vault.iterable.campaign_20240218
;


SELECT count(*)
FROM hygiene_vault.iterable.campaign
;

SELECT count(*)
FROM hygiene_vault.iterable.campaign_20240219
;


SELECT count(*)
FROM latest_vault.iterable.campaign
;


SELECT count(*)
FROM latest_vault.iterable.campaign_20240218
;