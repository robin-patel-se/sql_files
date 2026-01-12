CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags CLONE data_vault_mvp.dwh.se_sale_tags
;

-- run on empty table to check code
self_describing_task --include 'biapp/task_catalogue/dv/dwh/transactional/se_sale_tags_snapshot.py'  --method 'run' --start '2023-10-29 00:00:00' --end '2023-10-29 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_sale_tags_snapshot
;


-- create post dep steps
DROP TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags_snapshot
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.se_sale_tags_snapshot_20231029 CLONE data_vault_mvp.dwh.se_sale_tags_snapshot
;

-- create table using new ddl (got from logs of testing code)

CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.dwh.se_sale_tags_snapshot
(
	-- (lineage) metadata for the current job
	schedule_tstamp          TIMESTAMP,
	run_tstamp               TIMESTAMP,
	operation_id             VARCHAR,
	created_at               TIMESTAMP,
	updated_at               TIMESTAMP,
	view_date                DATE,
	se_sale_id               VARCHAR,
	tag_array                ARRAY,
	number_of_tags           NUMBER,
	number_of_campaign_tags  NUMBER,
	number_of_permanent_tags NUMBER,
	has_flash_tag            BOOLEAN,
	has_no_athena_tag        BOOLEAN,
	has_hotel_only_tag       BOOLEAN,
	has_refundable_rates_tag BOOLEAN,
	has_auto_promo_tag       BOOLEAN,

	CONSTRAINT pk_1 PRIMARY KEY (view_date, se_sale_id)
)
;

-- create insert script

INSERT INTO data_vault_mvp_dev_robin.dwh.se_sale_tags_snapshot
SELECT
	schedule_tstamp,
	run_tstamp,
	operation_id,
	created_at,
	updated_at,
	view_date,
	se_sale_id,
	tag_array,
	number_of_tags,
	number_of_campaign_tags,
	number_of_permanent_tags,
	has_flash_tag,
	has_no_athena_tag,
	has_hotel_only_tag,
	has_refundable_rates_tag,
	FALSE AS has_auto_promo_tag -- promo tag didn't exist
FROM data_vault_mvp_dev_robin.dwh.se_sale_tags_snapshot_20231029;


SELECT * FROM data_vault_mvp.dwh.iterable__user_profile_activity iupa;


