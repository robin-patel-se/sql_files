USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS raw_vault_dev_robin.impact_radius
;

CREATE OR REPLACE TRANSIENT TABLE raw_vault_dev_robin.impact_radius.partners
	CLONE raw_vault.impact_radius.partners
;

CREATE SCHEMA IF NOT EXISTS hygiene_vault_dev_robin.impact_radius
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.impact_radius.partners
	CLONE hygiene_vault.impact_radius.partners
;

CREATE SCHEMA IF NOT EXISTS latest_vault_dev_robin.impact_radius
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.impact_radius.partners
	CLONE latest_vault.impact_radius.partners
;

dataset_task
\
    --include 'incoming.impact_radius.partners' \
    --kind 'incoming' \
    --operation LatestRecordsOperation \
    --method 'run' \
    --upstream \
    --start '2025-01-14 00:00:00' \
    --end '2025-01-14 00:00:00'


SELECT *
FROM latest_vault.impact_radius.partners
;

DROP TABLE hygiene_vault_dev_robin.impact_radius.partners
;

DROP TABLE latest_vault_dev_robin.impact_radius.partners
;


------------------------------------------------------------------------------------------------------------------------


CREATE OR REPLACE TRANSIENT TABLE hygiene_vault_dev_robin.impact_radius.partners_20250114 CLONE hygiene_vault_dev_robin.impact_radius.partners
;


CREATE OR REPLACE TABLE hygiene_vault_dev_robin.impact_radius.partners
(
	-- (lineage) metadata for the current job
	schedule_tstamp                                   TIMESTAMP NOT NULL,
	run_tstamp                                        TIMESTAMP NOT NULL,
	operation_id                                      VARCHAR   NOT NULL,
	created_at                                        TIMESTAMP NOT NULL,
	updated_at                                        TIMESTAMP NOT NULL,

	-- (lineage) original metadata of row itself
	row_dataset_name                                  VARCHAR   NOT NULL,
	row_dataset_source                                VARCHAR   NOT NULL,
	row_loaded_at                                     TIMESTAMP NOT NULL,
	row_schedule_tstamp                               TIMESTAMP NOT NULL,
	row_run_tstamp                                    TIMESTAMP NOT NULL,
	row_filename                                      VARCHAR   NOT NULL,
	row_file_row_number                               INT       NOT NULL,
	row_extract_metadata                              VARIANT,


	-- transformed columns
	publisher_name                                    VARCHAR,
	publisher_id                                      INT,
	date_display                                      DATE,
	campaign_name                                     VARCHAR,
	aov                                               NUMBER(13, 2),
	actions                                           INT,
	actions_cost                                      NUMBER(15, 4),
	cpc                                               NUMBER(15, 4),
	clicks                                            INT,
	client_cost                                       NUMBER(15, 4),
	cr                                                NUMBER(25, 16),
	impressions                                       INT,
	lead_cpa                                          NUMBER(15, 4),
	lead_cr                                           NUMBER(25, 16),
	lead_cost                                         NUMBER(15, 4),
	lead_rr                                           NUMBER(19, 18),
	leads                                             INT,
	quantity                                          INT,
	rr                                                NUMBER(19, 18),
	raw_impressions                                   INT,
	revenue                                           NUMBER(13, 2),
	reversed_action_cost                              NUMBER(15, 4),
	reversed_lead_cost                                NUMBER(15, 4),
	reversed_leads                                    INT,
	reversed_revenue                                  NUMBER(13, 2),
	reversed_sale_cost                                NUMBER(15, 4),
	reversed_sales                                    INT,
	skus                                              INT,
	sale_cpa                                          NUMBER(15, 4),
	sale_cr                                           NUMBER(19, 18),
	sale_cost                                         NUMBER(15, 4),
	sale_rr                                           NUMBER(19, 18),
	sale_revenue                                      NUMBER(13, 2),
	sales                                             INT,
	total_clicks                                      INT,
	total_cost                                        NUMBER(15, 4),

	-- original columns
	record                                            VARIANT,
	clicks__o                                         INT,
	total_clicks__o                                   INT,

	-- validation columns
	failed_some_validation                            INT,
	fails_validation__campaign_name__expected_nonnull INT,
	fails_validation__publisher_id__expected_nonnull  INT,
	fails_validation__date_display__expected_nonnull  INT
)
;

INSERT INTO hygiene_vault_dev_robin.impact_radius.partners
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
	publisher_name,
	publisher_id,
	date_display,
	campaign_name,
	aov,
	actions,
	actions_cost,
	cpc,
	CASE
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'DE' AND publisher_name IN
																					   ('Admitad 269814', 'Adpump.',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'GLOPSS INTERNATIONAL PTE LTD',
																						'TakeAds Networks',
																						'Tradedoubler.',
																						'WESTPOINT Reise- & Business Service GmbH',
																						'admitad GmbH', 'cityads',
																						'xianfly') THEN NULL
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'UK' AND publisher_name IN
																					   ('Admitad 269814',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'DMS CD (Netherlands) B.V.',
																						'FlexOffers.com, LLC',
																						'Indoleads.', 'Linkbux',
																						'VIP Affiliate Network',
																						'admitad GmbH', 'cityads')
			THEN NULL
		ELSE clicks
	END                        AS clicks,
	client_cost,
	cr,
	impressions,
	lead_cpa,
	lead_cr,
	lead_cost,
	lead_rr,
	leads,
	quantity,
	rr,
	raw_impressions,
	revenue,
	reversed_action_cost,
	reversed_lead_cost,
	reversed_leads,
	reversed_revenue,
	reversed_sale_cost,
	reversed_sales,
	skus,
	sale_cpa,
	sale_cr,
	sale_cost,
	sale_rr,
	sale_revenue,
	sales,
	CASE
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'DE' AND publisher_name IN
																					   ('Admitad 269814', 'Adpump.',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'GLOPSS INTERNATIONAL PTE LTD',
																						'TakeAds Networks',
																						'Tradedoubler.',
																						'WESTPOINT Reise- & Business Service GmbH',
																						'admitad GmbH', 'cityads',
																						'xianfly') THEN NULL
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'UK' AND publisher_name IN
																					   ('Admitad 269814',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'DMS CD (Netherlands) B.V.',
																						'FlexOffers.com, LLC',
																						'Indoleads.', 'Linkbux',
																						'VIP Affiliate Network',
																						'admitad GmbH', 'cityads')
			THEN NULL
		ELSE total_clicks
	END                        AS total_clicks,
	total_cost,
	record,
	record['Clicks']::INT      AS clicks__o,
	record['TotalClicks']::INT AS total_clicks__o,
	failed_some_validation,
	fails_validation__campaign_name__expected_nonnull,
	fails_validation__publisher_id__expected_nonnull,
	fails_validation__date_display__expected_nonnull
FROM hygiene_vault_dev_robin.impact_radius.partners_20250114
;

SELECT *
FROM hygiene_vault_dev_robin.impact_radius.partners
WHERE partners.date_display <= '2023-01-01'
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.impact_radius.partners_20250114 CLONE latest_vault_dev_robin.impact_radius.partners
;


CREATE OR REPLACE TABLE latest_vault_dev_robin.impact_radius.partners
(
	-- (lineage) metadata for the current job
	schedule_tstamp      TIMESTAMP NOT NULL,
	run_tstamp           TIMESTAMP NOT NULL,
	operation_id         VARCHAR   NOT NULL,
	created_at           TIMESTAMP NOT NULL,
	updated_at           TIMESTAMP NOT NULL,

	-- (lineage) original metadata of row itself
	row_dataset_name     VARCHAR   NOT NULL,
	row_dataset_source   VARCHAR   NOT NULL,
	row_loaded_at        TIMESTAMP NOT NULL,
	row_schedule_tstamp  TIMESTAMP NOT NULL,
	row_run_tstamp       TIMESTAMP NOT NULL,
	row_filename         VARCHAR   NOT NULL,
	row_file_row_number  INT       NOT NULL,
	row_extract_metadata VARIANT,

	-- transformed columns
	publisher_name       VARCHAR,
	publisher_id         INT,
	date_display         DATE,
	campaign_name        VARCHAR,
	aov                  NUMBER(13, 2),
	actions              INT,
	actions_cost         NUMBER(15, 4),
	cpc                  NUMBER(15, 4),
	clicks               INT,
	client_cost          NUMBER(15, 4),
	cr                   NUMBER(25, 16),
	impressions          INT,
	lead_cpa             NUMBER(15, 4),
	lead_cr              NUMBER(25, 16),
	lead_cost            NUMBER(15, 4),
	lead_rr              NUMBER(19, 18),
	leads                INT,
	quantity             INT,
	rr                   NUMBER(19, 18),
	raw_impressions      INT,
	revenue              NUMBER(13, 2),
	reversed_action_cost NUMBER(15, 4),
	reversed_lead_cost   NUMBER(15, 4),
	reversed_leads       INT,
	reversed_revenue     NUMBER(13, 2),
	reversed_sale_cost   NUMBER(15, 4),
	reversed_sales       INT,
	skus                 INT,
	sale_cpa             NUMBER(15, 4),
	sale_cr              NUMBER(19, 18),
	sale_cost            NUMBER(15, 4),
	sale_rr              NUMBER(19, 18),
	sale_revenue         NUMBER(13, 2),
	sales                INT,
	total_clicks         INT,
	total_cost           NUMBER(15, 4),

	-- original columns
	record               VARIANT,
	clicks__o            INT,
	total_clicks__o      INT,
	CONSTRAINT pk_1 PRIMARY KEY (campaign_name, date_display, publisher_id)
)
;

INSERT INTO latest_vault_dev_robin.impact_radius.partners
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
	publisher_name,
	publisher_id,
	date_display,
	campaign_name,
	aov,
	actions,
	actions_cost,
	cpc,
	clicks,
	client_cost,
	cr,
	impressions,
	lead_cpa,
	lead_cr,
	lead_cost,
	lead_rr,
	leads,
	quantity,
	rr,
	raw_impressions,
	revenue,
	reversed_action_cost,
	reversed_lead_cost,
	reversed_leads,
	reversed_revenue,
	reversed_sale_cost,
	reversed_sales,
	skus,
	sale_cpa,
	sale_cr,
	sale_cost,
	sale_rr,
	sale_revenue,
	sales,
	total_clicks,
	total_cost,
	record,
	record['Clicks']::INT      AS clicks__o,
	record['TotalClicks']::INT AS total_clicks__o
FROM latest_vault_dev_robin.impact_radius.partners_20250114
;

SELECT
	COUNT(*)
FROM latest_vault.impact_radius.partners
;

SELECT
	COUNT(*)
FROM latest_vault_dev_robin.impact_radius.partners
;


------------------------------------------------------------------------------------------------------------------------
-- post deps
USE ROLE pipelinerunner
;

CREATE OR REPLACE TRANSIENT TABLE hygiene_vault.impact_radius.partners_20250114 CLONE hygiene_vault.impact_radius.partners
;


CREATE OR REPLACE TABLE hygiene_vault.impact_radius.partners
(
	-- (lineage) metadata for the current job
	schedule_tstamp                                   TIMESTAMP NOT NULL,
	run_tstamp                                        TIMESTAMP NOT NULL,
	operation_id                                      VARCHAR   NOT NULL,
	created_at                                        TIMESTAMP NOT NULL,
	updated_at                                        TIMESTAMP NOT NULL,

	-- (lineage) original metadata of row itself
	row_dataset_name                                  VARCHAR   NOT NULL,
	row_dataset_source                                VARCHAR   NOT NULL,
	row_loaded_at                                     TIMESTAMP NOT NULL,
	row_schedule_tstamp                               TIMESTAMP NOT NULL,
	row_run_tstamp                                    TIMESTAMP NOT NULL,
	row_filename                                      VARCHAR   NOT NULL,
	row_file_row_number                               INT       NOT NULL,
	row_extract_metadata                              VARIANT,


	-- transformed columns
	publisher_name                                    VARCHAR,
	publisher_id                                      INT,
	date_display                                      DATE,
	campaign_name                                     VARCHAR,
	aov                                               NUMBER(13, 2),
	actions                                           INT,
	actions_cost                                      NUMBER(15, 4),
	cpc                                               NUMBER(15, 4),
	clicks                                            INT,
	client_cost                                       NUMBER(15, 4),
	cr                                                NUMBER(25, 16),
	impressions                                       INT,
	lead_cpa                                          NUMBER(15, 4),
	lead_cr                                           NUMBER(25, 16),
	lead_cost                                         NUMBER(15, 4),
	lead_rr                                           NUMBER(19, 18),
	leads                                             INT,
	quantity                                          INT,
	rr                                                NUMBER(19, 18),
	raw_impressions                                   INT,
	revenue                                           NUMBER(13, 2),
	reversed_action_cost                              NUMBER(15, 4),
	reversed_lead_cost                                NUMBER(15, 4),
	reversed_leads                                    INT,
	reversed_revenue                                  NUMBER(13, 2),
	reversed_sale_cost                                NUMBER(15, 4),
	reversed_sales                                    INT,
	skus                                              INT,
	sale_cpa                                          NUMBER(15, 4),
	sale_cr                                           NUMBER(19, 18),
	sale_cost                                         NUMBER(15, 4),
	sale_rr                                           NUMBER(19, 18),
	sale_revenue                                      NUMBER(13, 2),
	sales                                             INT,
	total_clicks                                      INT,
	total_cost                                        NUMBER(15, 4),

	-- original columns
	record                                            VARIANT,
	clicks__o                                         INT,
	total_clicks__o                                   INT,

	-- validation columns
	failed_some_validation                            INT,
	fails_validation__campaign_name__expected_nonnull INT,
	fails_validation__publisher_id__expected_nonnull  INT,
	fails_validation__date_display__expected_nonnull  INT
)
;

INSERT INTO hygiene_vault.impact_radius.partners
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
	publisher_name,
	publisher_id,
	date_display,
	campaign_name,
	aov,
	actions,
	actions_cost,
	cpc,
	CASE
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'DE' AND publisher_name IN
																					   ('Admitad 269814', 'Adpump.',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'GLOPSS INTERNATIONAL PTE LTD',
																						'TakeAds Networks',
																						'Tradedoubler.',
																						'WESTPOINT Reise- & Business Service GmbH',
																						'admitad GmbH', 'cityads',
																						'xianfly') THEN NULL
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'UK' AND publisher_name IN
																					   ('Admitad 269814',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'DMS CD (Netherlands) B.V.',
																						'FlexOffers.com, LLC',
																						'Indoleads.', 'Linkbux',
																						'VIP Affiliate Network',
																						'admitad GmbH', 'cityads')
			THEN NULL
		ELSE clicks
	END                        AS clicks,
	client_cost,
	cr,
	impressions,
	lead_cpa,
	lead_cr,
	lead_cost,
	lead_rr,
	leads,
	quantity,
	rr,
	raw_impressions,
	revenue,
	reversed_action_cost,
	reversed_lead_cost,
	reversed_leads,
	reversed_revenue,
	reversed_sale_cost,
	reversed_sales,
	skus,
	sale_cpa,
	sale_cr,
	sale_cost,
	sale_rr,
	sale_revenue,
	sales,
	CASE
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'DE' AND publisher_name IN
																					   ('Admitad 269814', 'Adpump.',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'GLOPSS INTERNATIONAL PTE LTD',
																						'TakeAds Networks',
																						'Tradedoubler.',
																						'WESTPOINT Reise- & Business Service GmbH',
																						'admitad GmbH', 'cityads',
																						'xianfly') THEN NULL
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'UK' AND publisher_name IN
																					   ('Admitad 269814',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'DMS CD (Netherlands) B.V.',
																						'FlexOffers.com, LLC',
																						'Indoleads.', 'Linkbux',
																						'VIP Affiliate Network',
																						'admitad GmbH', 'cityads')
			THEN NULL
		ELSE total_clicks
	END                        AS total_clicks,
	total_cost,
	record,
	record['Clicks']::INT      AS clicks__o,
	record['TotalClicks']::INT AS total_clicks__o,
	failed_some_validation,
	fails_validation__campaign_name__expected_nonnull,
	fails_validation__publisher_id__expected_nonnull,
	fails_validation__date_display__expected_nonnull
FROM hygiene_vault.impact_radius.partners_20250114
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault.impact_radius.partners_20250114 CLONE latest_vault.impact_radius.partners
;


CREATE OR REPLACE TABLE latest_vault.impact_radius.partners
(
	-- (lineage) metadata for the current job
	schedule_tstamp      TIMESTAMP NOT NULL,
	run_tstamp           TIMESTAMP NOT NULL,
	operation_id         VARCHAR   NOT NULL,
	created_at           TIMESTAMP NOT NULL,
	updated_at           TIMESTAMP NOT NULL,

	-- (lineage) original metadata of row itself
	row_dataset_name     VARCHAR   NOT NULL,
	row_dataset_source   VARCHAR   NOT NULL,
	row_loaded_at        TIMESTAMP NOT NULL,
	row_schedule_tstamp  TIMESTAMP NOT NULL,
	row_run_tstamp       TIMESTAMP NOT NULL,
	row_filename         VARCHAR   NOT NULL,
	row_file_row_number  INT       NOT NULL,
	row_extract_metadata VARIANT,

	-- transformed columns
	publisher_name       VARCHAR,
	publisher_id         INT,
	date_display         DATE,
	campaign_name        VARCHAR,
	aov                  NUMBER(13, 2),
	actions              INT,
	actions_cost         NUMBER(15, 4),
	cpc                  NUMBER(15, 4),
	clicks               INT,
	client_cost          NUMBER(15, 4),
	cr                   NUMBER(25, 16),
	impressions          INT,
	lead_cpa             NUMBER(15, 4),
	lead_cr              NUMBER(25, 16),
	lead_cost            NUMBER(15, 4),
	lead_rr              NUMBER(19, 18),
	leads                INT,
	quantity             INT,
	rr                   NUMBER(19, 18),
	raw_impressions      INT,
	revenue              NUMBER(13, 2),
	reversed_action_cost NUMBER(15, 4),
	reversed_lead_cost   NUMBER(15, 4),
	reversed_leads       INT,
	reversed_revenue     NUMBER(13, 2),
	reversed_sale_cost   NUMBER(15, 4),
	reversed_sales       INT,
	skus                 INT,
	sale_cpa             NUMBER(15, 4),
	sale_cr              NUMBER(19, 18),
	sale_cost            NUMBER(15, 4),
	sale_rr              NUMBER(19, 18),
	sale_revenue         NUMBER(13, 2),
	sales                INT,
	total_clicks         INT,
	total_cost           NUMBER(15, 4),

	-- original columns
	record               VARIANT,
	clicks__o            INT,
	total_clicks__o      INT,
	CONSTRAINT pk_1 PRIMARY KEY (campaign_name, date_display, publisher_id)
)
;

INSERT INTO latest_vault.impact_radius.partners
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
	publisher_name,
	publisher_id,
	date_display,
	campaign_name,
	aov,
	actions,
	actions_cost,
	cpc,
	CASE
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'DE' AND publisher_name IN
																					   ('Admitad 269814', 'Adpump.',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'GLOPSS INTERNATIONAL PTE LTD',
																						'TakeAds Networks',
																						'Tradedoubler.',
																						'WESTPOINT Reise- & Business Service GmbH',
																						'admitad GmbH', 'cityads',
																						'xianfly') THEN NULL
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'UK' AND publisher_name IN
																					   ('Admitad 269814',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'DMS CD (Netherlands) B.V.',
																						'FlexOffers.com, LLC',
																						'Indoleads.', 'Linkbux',
																						'VIP Affiliate Network',
																						'admitad GmbH', 'cityads')
			THEN NULL
		ELSE clicks
	END                        AS clicks,
	client_cost,
	cr,
	impressions,
	lead_cpa,
	lead_cr,
	lead_cost,
	lead_rr,
	leads,
	quantity,
	rr,
	raw_impressions,
	revenue,
	reversed_action_cost,
	reversed_lead_cost,
	reversed_leads,
	reversed_revenue,
	reversed_sale_cost,
	reversed_sales,
	skus,
	sale_cpa,
	sale_cr,
	sale_cost,
	sale_rr,
	sale_revenue,
	sales,
	CASE
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'DE' AND publisher_name IN
																					   ('Admitad 269814', 'Adpump.',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'GLOPSS INTERNATIONAL PTE LTD',
																						'TakeAds Networks',
																						'Tradedoubler.',
																						'WESTPOINT Reise- & Business Service GmbH',
																						'admitad GmbH', 'cityads',
																						'xianfly') THEN NULL
		WHEN EXTRACT(year, date_display) = 2023 AND RIGHT(campaign_name, 2) = 'UK' AND publisher_name IN
																					   ('Admitad 269814',
																						'Cloudtraffic',
																						'Company_Cityads',
																						'DMS CD (Netherlands) B.V.',
																						'FlexOffers.com, LLC',
																						'Indoleads.', 'Linkbux',
																						'VIP Affiliate Network',
																						'admitad GmbH', 'cityads')
			THEN NULL
		ELSE total_clicks
	END                        AS total_clicks,
	total_cost,
	record,
	record['Clicks']::INT      AS clicks__o,
	record['TotalClicks']::INT AS total_clicks__o
FROM latest_vault.impact_radius.partners_20250114
;

SELECT
	SUM(partners.clicks)
FROM latest_vault.impact_radius.partners
WHERE date_display >= '2023-01-01'
  AND date_display <= '2023-12-31'
  AND RIGHT(campaign_name, 2) = 'UK'
  AND publisher_name IN
	  ('Admitad 269814', 'Cloudtraffic', 'Company_Cityads', 'DMS CD (Netherlands) B.V.', 'FlexOffers.com, LLC',
	   'Indoleads.', 'Linkbux', 'VIP Affiliate Network', 'admitad GmbH', 'cityads')

