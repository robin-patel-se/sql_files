
CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_marketing_channel_with_start_date
(
	-- (lineage) metadata for the current job
	schedule_tstamp           TIMESTAMP,
	run_tstamp                TIMESTAMP,
	operation_id              VARCHAR,
	created_at                TIMESTAMP,
	updated_at                TIMESTAMP,

	touch_id                  VARCHAR PRIMARY KEY NOT NULL,
	touch_start_tstamp        TIMESTAMP,
	touch_mkt_channel         VARCHAR,
	touch_landing_page        VARCHAR,
	touch_hostname            VARCHAR,
	touch_hostname_territory  VARCHAR,
	attributed_user_id        VARCHAR,
	utm_campaign              VARCHAR,
	utm_medium                VARCHAR,
	utm_source                VARCHAR,
	utm_term                  VARCHAR,
	utm_content               VARCHAR,
	click_id                  VARCHAR,
	sub_affiliate_name        VARCHAR,
	affiliate                 VARCHAR,
	touch_affiliate_territory VARCHAR,
	awadgroupid               VARCHAR,
	awcampaignid              VARCHAR,
	referrer_hostname         VARCHAR,
	referrer_medium           VARCHAR,
	landing_page_parameters   OBJECT,
	app_push_open_context     OBJECT
)
	CLUSTER BY (touch_mkt_channel)
;

INSERT INTO data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_marketing_channel_with_start_date
SELECT
	mtmc.schedule_tstamp,
	mtmc.run_tstamp,
	mtmc.operation_id,
	mtmc.created_at,
	mtmc.updated_at,
	mtmc.touch_id,
	mtba.touch_start_tstamp,
	mtmc.touch_mkt_channel,
	mtmc.touch_landing_page,
	mtmc.touch_hostname,
	mtmc.touch_hostname_territory,
	mtmc.attributed_user_id,
	mtmc.utm_campaign,
	mtmc.utm_medium,
	mtmc.utm_source,
	mtmc.utm_term,
	mtmc.utm_content,
	mtmc.click_id,
	mtmc.sub_affiliate_name,
	mtmc.affiliate,
	mtmc.touch_affiliate_territory,
	mtmc.awadgroupid,
	mtmc.awcampaignid,
	mtmc.referrer_hostname,
	mtmc.referrer_medium,
	mtmc.landing_page_parameters,
	mtmc.app_push_open_context
FROM data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_marketing_channel mtmc
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_basic_attributes mtba
			  ON mtmc.touch_id = mtba.touch_id
;



CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_with_start_date
(
	-- (lineage) metadata for the current job
	schedule_tstamp           TIMESTAMP,
	run_tstamp                TIMESTAMP,
	operation_id              VARCHAR,
	created_at                TIMESTAMP,
	updated_at                TIMESTAMP,

	touch_id                  VARCHAR PRIMARY KEY NOT NULL,
	touch_start_tstamp        TIMESTAMP,
	touch_mkt_channel         VARCHAR,
	touch_landing_page        VARCHAR,
	touch_hostname            VARCHAR,
	touch_hostname_territory  VARCHAR,
	attributed_user_id        VARCHAR,
	utm_campaign              VARCHAR,
	utm_medium                VARCHAR,
	utm_source                VARCHAR,
	utm_term                  VARCHAR,
	utm_content               VARCHAR,
	click_id                  VARCHAR,
	sub_affiliate_name        VARCHAR,
	affiliate                 VARCHAR,
	touch_affiliate_territory VARCHAR,
	awadgroupid               VARCHAR,
	awcampaignid              VARCHAR,
	referrer_hostname         VARCHAR,
	referrer_medium           VARCHAR,
	landing_page_parameters   OBJECT,
	app_push_open_context     OBJECT
)
	CLUSTER BY (touch_mkt_channel)
;

INSERT INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_with_start_date
SELECT
	mtmc.schedule_tstamp,
	mtmc.run_tstamp,
	mtmc.operation_id,
	mtmc.created_at,
	mtmc.updated_at,
	mtmc.touch_id,
	mtba.touch_start_tstamp,
	mtmc.touch_mkt_channel,
	mtmc.touch_landing_page,
	mtmc.touch_hostname,
	mtmc.touch_hostname_territory,
	mtmc.attributed_user_id,
	mtmc.utm_campaign,
	mtmc.utm_medium,
	mtmc.utm_source,
	mtmc.utm_term,
	mtmc.utm_content,
	mtmc.click_id,
	mtmc.sub_affiliate_name,
	mtmc.affiliate,
	mtmc.touch_affiliate_territory,
	mtmc.awadgroupid,
	mtmc.awcampaignid,
	mtmc.referrer_hostname,
	mtmc.referrer_medium,
	mtmc.landing_page_parameters,
	mtmc.app_push_open_context
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel mtmc
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
			  ON mtmc.touch_id = mtba.touch_id


ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_marketing_channel
	RENAME TO data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_marketing_channel_without_start_date
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_marketing_channel_with_start_date
	RENAME TO data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_marketing_channel
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
	RENAME TO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_without_start_date
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel_with_start_date
	RENAME TO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_marketing_channel
;

SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_attribution mta
;



CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_attribution_with_dates
(
	-- (lineage) metadata for the current job
	schedule_tstamp     TIMESTAMP,
	run_tstamp          TIMESTAMP,
	operation_id        VARCHAR,
	created_at          TIMESTAMP,
	updated_at          TIMESTAMP,

	touch_id            VARCHAR,
	touch_start_tstamp  TIMESTAMP,
	attributed_touch_id VARCHAR,
	attribution_model   VARCHAR,
	attributed_weight   FLOAT,
	CONSTRAINT pk_1 PRIMARY KEY (touch_id, attributed_touch_id, attribution_model)
)
	CLUSTER BY (attribution_model, touch_start_tstamp::DATE)
;


CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution_with_dates
(
	-- (lineage) metadata for the current job
	schedule_tstamp     TIMESTAMP,
	run_tstamp          TIMESTAMP,
	operation_id        VARCHAR,
	created_at          TIMESTAMP,
	updated_at          TIMESTAMP,

	touch_id            VARCHAR,
	touch_start_tstamp  TIMESTAMP,
	attributed_touch_id VARCHAR,
	attribution_model   VARCHAR,
	attributed_weight   FLOAT,
	CONSTRAINT pk_1 PRIMARY KEY (touch_id, attributed_touch_id, attribution_model)
)
	CLUSTER BY (attribution_model, touch_start_tstamp::DATE)
;

INSERT INTO data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_attribution_with_dates
SELECT
	mta.schedule_tstamp,
	mta.run_tstamp,
	mta.operation_id,
	mta.created_at,
	mta.updated_at,
	mta.touch_id,
	mtba.touch_start_tstamp,
	mta.attributed_touch_id,
	mta.attribution_model,
	mta.attributed_weight
FROM data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_attribution mta
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_basic_attributes mtba
			   ON mta.touch_id = mtba.touch_id


INSERT INTO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution_with_dates
SELECT
	mta.schedule_tstamp,
	mta.run_tstamp,
	mta.operation_id,
	mta.created_at,
	mta.updated_at,
	mta.touch_id,
	mtba.touch_start_tstamp,
	mta.attributed_touch_id,
	mta.attribution_model,
	mta.attributed_weight
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution mta
	INNER JOIN data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_basic_attributes mtba
			   ON mta.touch_id = mtba.touch_id

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_attribution
	RENAME TO data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_attribution_without_start_date
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_attribution_with_dates
	RENAME TO data_vault_mvp_dev_robin.single_customer_view_archive.module_touch_attribution
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
	RENAME TO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution_without_start_date
;

ALTER TABLE data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution_with_dates
	RENAME TO data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
;


SELECT *
FROM data_vault_mvp_dev_robin.single_customer_view_stg.module_touch_attribution
;


------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner
;

USE WAREHOUSE scv_pipe_2xlarge
;

CREATE TABLE IF NOT EXISTS data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_with_start_date
(
	-- (lineage) metadata for the current job
	schedule_tstamp           TIMESTAMP,
	run_tstamp                TIMESTAMP,
	operation_id              VARCHAR,
	created_at                TIMESTAMP,
	updated_at                TIMESTAMP,

	touch_id                  VARCHAR PRIMARY KEY NOT NULL,
	touch_start_tstamp        TIMESTAMP,
	touch_mkt_channel         VARCHAR,
	touch_landing_page        VARCHAR,
	touch_hostname            VARCHAR,
	touch_hostname_territory  VARCHAR,
	attributed_user_id        VARCHAR,
	utm_campaign              VARCHAR,
	utm_medium                VARCHAR,
	utm_source                VARCHAR,
	utm_term                  VARCHAR,
	utm_content               VARCHAR,
	click_id                  VARCHAR,
	sub_affiliate_name        VARCHAR,
	affiliate                 VARCHAR,
	touch_affiliate_territory VARCHAR,
	awadgroupid               VARCHAR,
	awcampaignid              VARCHAR,
	referrer_hostname         VARCHAR,
	referrer_medium           VARCHAR,
	landing_page_parameters   OBJECT,
	app_push_open_context     OBJECT
)
	CLUSTER BY (touch_mkt_channel)
;

INSERT INTO data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_with_start_date
SELECT
	mtmc.schedule_tstamp,
	mtmc.run_tstamp,
	mtmc.operation_id,
	mtmc.created_at,
	mtmc.updated_at,
	mtmc.touch_id,
	mtba.touch_start_tstamp,
	mtmc.touch_mkt_channel,
	mtmc.touch_landing_page,
	mtmc.touch_hostname,
	mtmc.touch_hostname_territory,
	mtmc.attributed_user_id,
	mtmc.utm_campaign,
	mtmc.utm_medium,
	mtmc.utm_source,
	mtmc.utm_term,
	mtmc.utm_content,
	mtmc.click_id,
	mtmc.sub_affiliate_name,
	mtmc.affiliate,
	mtmc.touch_affiliate_territory,
	mtmc.awadgroupid,
	mtmc.awcampaignid,
	mtmc.referrer_hostname,
	mtmc.referrer_medium,
	mtmc.landing_page_parameters,
	mtmc.app_push_open_context
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
			   ON mtmc.touch_id = mtba.touch_id
;



CREATE TABLE IF NOT EXISTS data_vault_mvp.single_customer_view_stg.module_touch_attribution_with_dates
(
	-- (lineage) metadata for the current job
	schedule_tstamp     TIMESTAMP,
	run_tstamp          TIMESTAMP,
	operation_id        VARCHAR,
	created_at          TIMESTAMP,
	updated_at          TIMESTAMP,

	touch_id            VARCHAR,
	touch_start_tstamp  TIMESTAMP,
	attributed_touch_id VARCHAR,
	attribution_model   VARCHAR,
	attributed_weight   FLOAT,
	CONSTRAINT pk_1 PRIMARY KEY (touch_id, attributed_touch_id, attribution_model)
)
	CLUSTER BY (attribution_model, touch_start_tstamp::DATE)
;

INSERT INTO data_vault_mvp.single_customer_view_stg.module_touch_attribution_with_dates
SELECT
	mta.schedule_tstamp,
	mta.run_tstamp,
	mta.operation_id,
	mta.created_at,
	mta.updated_at,
	mta.touch_id,
	mtba.touch_start_tstamp,
	mta.attributed_touch_id,
	mta.attribution_model,
	mta.attributed_weight
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution mta
	INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
			   ON mta.touch_id = mtba.touch_id
;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_with_start_date mtmcwsd
;

SELECT *
FROM data_vault_mvp.single_customer_view_stg.module_touch_attribution_with_dates
;


ALTER TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
	RENAME TO data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_without_start_date
;

ALTER TABLE data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel_with_start_date
	RENAME TO data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel
;


ALTER TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution
	RENAME TO data_vault_mvp.single_customer_view_stg.module_touch_attribution_without_start_date
;

ALTER TABLE data_vault_mvp.single_customer_view_stg.module_touch_attribution_with_dates
	RENAME TO data_vault_mvp.single_customer_view_stg.module_touch_attribution
;

USE role pipelinerunner;
SELECT *
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time >= CURRENT_DATE - 1 AND qh.query_id = '01bd492f-0206-d878-0002-dd0124c73ac7';


