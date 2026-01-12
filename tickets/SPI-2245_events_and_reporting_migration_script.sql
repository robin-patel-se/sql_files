-- donald gatfield
-- 31st May 2022
-- SPI-2245 - change primary keys for iterale_email_reporting + email_reporting + email_performanc

-- 1. email_reporting update primary key

-- row-count [34263083]
SELECT COUNT(*) FROM data_vault_mvp.dwh.iterable_email_reporting;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable_email_reporting_spi_2455_backup
CLONE data_vault_mvp.dwh.iterable_email_reporting;

ALTER TABLE data_vault_mvp.dwh.iterable_email_reporting RENAME TO data_vault_mvp_dev_robin.dwh.iterable_email_reporting_rename;


CREATE TABLE IF NOT EXISTS data_vault_mvp.dwh.iterable_email_reporting (
  schedule_tstamp TIMESTAMP,
  run_tstamp TIMESTAMP,
  operation_id VARCHAR,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  se_sale_id VARCHAR,
  email_id VARCHAR,
  campaign_id NUMBER,
  send_date DATE,
  email_name VARCHAR,
  mapped_territory VARCHAR,
  data_source_name VARCHAR,
  sale_position_group VARCHAR,
  sale_name VARCHAR,
  company_name VARCHAR,
  sale_start_date DATE,
  sale_end_date DATE,
  sale_type VARCHAR,
  sale_product VARCHAR,
  destination_type VARCHAR,
  posu_city VARCHAR,
  posu_country VARCHAR,
  posu_division VARCHAR,
  event_date DATE,
  unique_impressions NUMBER,
  impressions NUMBER,
  impressions_personalised_athena NUMBER,
  impressions_default_athena_recommendations_catalog NUMBER,
  impressions_not_got_athena NUMBER,
  unique_clicks NUMBER,
  clicks NUMBER,
  clicks_personalised_athena NUMBER,
  clicks_default_athena_recommendations_catalog NUMBER,
  clicks_not_got_athena NUMBER,
  crm_platform VARCHAR,

  PRIMARY KEY (se_sale_id, campaign_id, send_date, data_source_name, sale_position_group, event_date, mapped_territory)
)
CLUSTER BY (se_sale_id, campaign_id);
;


INSERT INTO data_vault_mvp.dwh.iterable_email_reporting
SELECT * FROM data_vault_mvp_dev_robin.dwh.iterable_email_reporting_rename;


-- row-count [34263083]
SELECT COUNT(*) FROM data_vault_mvp.dwh.iterable_email_reporting;



-- 2 email_reporting update primary key

-- row-count [119567591]
SELECT COUNT(*) FROM data_vault_mvp.dwh.email_reporting;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.email_reporting_spi_2445_backup
CLONE data_vault_mvp.dwh.email_reporting;

ALTER TABLE data_vault_mvp.dwh.email_reporting RENAME TO data_vault_mvp_dev_robin.dwh.email_reporting_rename;

CREATE OR REPLACE TABLE data_vault_mvp.dwh.email_reporting (
  schedule_tstamp TIMESTAMP,
  run_tstamp TIMESTAMP,
  operation_id VARCHAR,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  se_sale_id VARCHAR,
  email_id VARCHAR,
  send_id NUMBER,
  send_date DATE,
  email_name VARCHAR,
  mapped_territory VARCHAR,
  data_source_name VARCHAR,
  sale_position_group VARCHAR,
  sale_name VARCHAR,
  company_name VARCHAR,
  sale_start_date DATE,
  sale_end_date DATE,
  sale_type VARCHAR,
  sale_product VARCHAR,
  destination_type VARCHAR,
  posu_city VARCHAR,
  posu_country VARCHAR,
  posu_division VARCHAR,
  event_date DATE,
  unique_impressions NUMBER,
  impressions NUMBER,
  impressions_personalised_athena NUMBER,
  impressions_default_athena_recommendations_catalog NUMBER,
  impressions_not_got_athena NUMBER,
  unique_clicks NUMBER,
  clicks NUMBER,
  clicks_personalised_athena NUMBER,
  clicks_default_athena_recommendations_catalog NUMBER,
  clicks_not_got_athena NUMBER,
  crm_platform VARCHAR,
  PRIMARY KEY (se_sale_id, send_id, send_date, data_source_name, sale_position_group, event_date, mapped_territory)
)
;
USE WAREHOUSE pipe_xlarge;

INSERT INTO data_vault_mvp.dwh.email_reporting
SELECT
    schedule_tstamp ,
    run_tstamp ,
    operation_id ,
    created_at ,
    updated_at ,
    se_sale_id ,
    email_id ,
    send_id ,
    send_date ,
    email_name ,
    COALESCE(IFF(mapped_territory IS NULL, split_part(email_name,'_', 2), mapped_territory),'TESTING') AS mapped_territory, -- 'TESTING' is a legit terriory.. there are still ~70K NULL that don't have either
    data_source_name ,
    sale_position_group ,
    sale_name ,
    company_name ,
    sale_start_date ,
    sale_end_date ,
    sale_type ,
    sale_product ,
    destination_type ,
    posu_city ,
    posu_country ,
    posu_division ,
    event_date ,
    unique_impressions ,
    impressions ,
    impressions_personalised_athena ,
    impressions_default_athena_recommendations_catalog ,
    impressions_not_got_athena ,
    unique_clicks ,
    clicks ,
    clicks_personalised_athena ,
    clicks_default_athena_recommendations_catalog ,
    clicks_not_got_athena ,
  crm_platform
FROM data_vault_mvp_dev_robin.dwh.email_reporting_rename
;

-- row-count [119567591]
SELECT COUNT(*) FROM data_vault_mvp.dwh.email_reporting;

-- 3 email_performance update primary key

-- row-count [767700]
SELECT COUNT(*) FROM data_vault_mvp.dwh.email_performance;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.email_performance_spi_2445_backup
CLONE data_vault_mvp.dwh.email_performance;

ALTER TABLE data_vault_mvp.dwh.email_performance RENAME TO data_vault_mvp_dev_robin.dwh.email_performance_rename;

CREATE TABLE IF NOT EXISTS data_vault_mvp.dwh.email_performance(
  schedule_tstamp TIMESTAMP,
  run_tstamp TIMESTAMP,
  operation_id VARCHAR,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  email_id VARCHAR,
  send_id NUMBER,
  campaign_id NUMBER,
  scheduled_date DATE,
  scheduled_tstmap TIMESTAMP_NTZ,
  email_name VARCHAR,
  mapped_crm_date VARCHAR,
  mapped_territory VARCHAR,
  mapped_objective VARCHAR,
  mapped_platform VARCHAR,
  mapped_campaign VARCHAR,
  sent_date DATE,
  sent_tstamp TIMESTAMP_NTZ,
  is_email_name_remapped BOOLEAN,
  client_id NUMBER,
  from_name VARCHAR,
  from_email VARCHAR,
  subject VARCHAR,
  triggered_send_external_key VARCHAR,
  send_definition_external_key VARCHAR,
  job_status VARCHAR,
  preview_url VARCHAR,
  is_multipart VARCHAR,
  additional VARCHAR,
  campaign_created_at TIMESTAMP_NTZ,
  campaign_updated_at TIMESTAMP_NTZ,
  ended_at TIMESTAMP_NTZ,
  template_id NUMBER,
  message_medium VARCHAR,
  created_by_user_id VARCHAR,
  updated_by_user_id VARCHAR,
  campaign_state VARCHAR,
  list_ids ARRAY,
  suppression_list_ids ARRAY,
  send_size NUMBER,
  labels ARRAY,
  type VARCHAR,
  crm_platform VARCHAR,
  is_athena_email BOOLEAN,
  email_sends NUMBER,
  unique_email_opens NUMBER,
  email_opens NUMBER,
  unique_email_clicks NUMBER,
  email_clicks NUMBER,
  email_unsubs NUMBER,
  sessions NUMBER,
  spvs NUMBER,
  bookings NUMBER,
  domestic_bookings NUMBER,
  international_bookings NUMBER,
  margin DECIMAL(13, 4),
  gross_revenue DECIMAL(13, 4),

  PRIMARY KEY (email_id, sent_date)
);
;

INSERT INTO data_vault_mvp.dwh.email_performance
SELECT * FROM data_vault_mvp_dev_robin.dwh.email_performance_rename;


-- row-count [767700]
SELECT COUNT(*) FROM data_vault_mvp.dwh.email_performance;