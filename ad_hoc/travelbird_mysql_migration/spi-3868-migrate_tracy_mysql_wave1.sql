------------------------------------------------------------------------------------------------------------------------


dag_id: incoming__travelbird_mysql__allocations_allocationboard__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
allocations_allocationboard.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE
migrated: TRUE

dag_id: incoming__travelbird_mysql__allocations_allocationunit__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
allocations_allocationunit.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE
migrated: TRUE

dag_id: incoming__travelbird_mysql__allocations_chargeblock__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
allocations_chargeblock.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE
migrated: TRUE

dag_id: incoming__travelbird_mysql__carrentals_carextra__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
carrentals_carextra.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE
migrated: TRUE

dag_id: incoming__travelbird_mysql__hotel_integrations_hirateplan__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
hotel_integrations_hirateplan.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__hotel_integrations_marihotel__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
hotel_integrations_marihotel.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__hotel_integrations_marirateplan__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
hotel_integrations_marirateplan.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__image_pool_imagesource__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
image_pool_imagesource.json
hygiene_snapshot: biapp/
task_catalogue/
staging/
hygiene_snapshots/
travelbird_mysql/
image_pool_imagesource.py
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__offers_image__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
offers_image.json
hygiene_snapshot: biapp/
task_catalogue/
staging/
hygiene_snapshots/
travelbird_mysql/
offers_image.py
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__offers_leisureproductlink__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
offers_leisureproductlink.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__orders_leisureorderitem__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
orders_leisureorderitem.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__orders_transferorderitem__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
orders_transferorderitem.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__packaging_tourpackageconfiguration_weekdays__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
packaging_tourpackageconfiguration_weekdays.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__packaging_transferpackageconfiguration_transfer_properties__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
packaging_transferpackageconfiguration_transfer_properties.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__partners_partneremail__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
partners_partneremail.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__pricing_commissionrate__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
pricing_commissionrate.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE

dag_id: incoming__travelbird_mysql__pricing_conditionand__daily_at_00h30
manifest: biapp/
manifests/
incoming/
travelbird_mysql/
pricing_conditionand.json
hygiene_snapshot: FALSE
dependencies: FALSE
bulk_snapshot: FALSE

------------------------------------------------------------------------------------------------------------------------

- allocations_allocationboard -- already migrated
- allocations_allocationunit -- already migrated
- allocations_chargeblock -- already migrated
- carrentals_carextra -- already migrated
- hotel_integrations_hirateplan -- already migrated
- hotel_integrations_marihotel -- already migrated
- hotel_integrations_marirateplan -- already migrated
- image_pool_imagesource -- created new ingest
- offers_image -- migrated from generic hygiene
- offers_leisureproductlink -- already migrated
- orders_leisureorderitem -- already migrated
- orders_transferorderitem -- already migrated
- packaging_tourpackageconfiguration_weekdays -- created new ingest
- packaging_transferpackageconfiguration_transfer_properties -- already done
- partners_partneremail -- already done
- pricing_commissionrate -- already done
- pricing_conditionand -- already done

------------------------------------------------------------------------------------------------------------------------
/*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.allocations_allocationboard
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.allocations_allocationunit
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.allocations_chargeblock
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.carrentals_carextra
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.hotel_integrations_hirateplan
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.hotel_integrations_marihotel
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.hotel_integrations_marirateplan

python biapp/bau/manifests/generate_manifest_from_sql_table.py \
    --connector 'travelbird_mysql' \
    --table_names 'image_pool_imagesource' \
    --mode 'regenerative' \
    --start_date '2022-01-01 00:00:00'

python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.offers_image
python biapp/adhoc/migrate_manifests/convert_generic_hygiene.py --dataset_source 'travelbird_mysql' --dataset_name 'offers_image'

python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.offers_leisureproductlink
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.orders_leisureorderitem
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.orders_transferorderitem

python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.packaging_tourpackageconfiguration_weekdays
python biapp/bau/manifests/generate_manifest_from_sql_table.py \
    --connector 'travelbird_mysql' \
    --table_names 'packaging_tourpackageconfiguration_weekdays' \
    --mode 'incremental' \
    --start_date '2022-01-01 00:00:00'

python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.packaging_transferpackageconfiguration_transfer_properties
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.partners_partneremail
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_commissionrate
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionand


*/

dataset_task --include 'travelbird_mysql.allocations_allocationboard' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.allocations_chargeblock' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_commissionrate' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'

dataset_task --include 'travelbird_mysql.allocations_allocationunit' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.carrentals_carextra' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.hotel_integrations_hirateplan' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.hotel_integrations_marihotel' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.hotel_integrations_marirateplan' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.image_pool_imagesource' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.offers_image' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.offers_leisureproductlink' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.orders_leisureorderitem' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.orders_transferorderitem' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'

dataset_task --include 'travelbird_mysql.packaging_tourpackageconfiguration_weekdays' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00' -- has 0 rows in prod table
dataset_task --include 'travelbird_mysql.packaging_transferpackageconfiguration_transfer_properties' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.partners_partneremail' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditionand' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-10-12 00:30:00' --end '2023-10-12 00:30:00' -- has 0 rows in prod table

*/


DROP TABLE raw_vault_dev_robin.travelbird_mysql.allocations_allocationboard
;

DROP TABLE hygiene_vault_mvp_dev_robin.travelbird_mysql.allocations_allocationboard
;

------------------------------------------------------------------------------------------------------------------------
-- IDENTIFY VIEW REFERENCES TO RAW_VAULT_MVP TABLES


USE ROLE pipelinerunner
;

USE WAREHOUSE pipe_xlarge
;

CALL data_vault_mvp.dwh.table_reference_in_view(
		'scratch.robinpatel.table_reference_in_view',
		'raw_vault_mvp.travelbird_mysql.image_pool_imagesource, raw_vault_mvp.travelbird_mysql.offers_image, raw_vault_mvp.travelbird_mysql.packaging_tourpackageconfiguration_weekdays',
		'collab, data_vault_mvp, se'
	)
;
-- result: []
SELECT *
FROM scratch.robinpatel.table_reference_in_view
;

USE ROLE pipelinerunner
;

CALL data_vault_mvp.dwh.table_reference_in_view(
		'scratch.robinpatel.table_reference_in_view',
		'hygiene_snapshot_vault_mvp.travelbird_mysql.image_pool_imagesource, hygiene_snapshot_vault_mvp.travelbird_mysql.offers_image, hygiene_snapshot_vault_mvp.travelbird_mysql.packaging_tourpackageconfiguration_weekdays',
		'collab, data_vault_mvp, se'
	)
;
-- result: []
SELECT *
FROM scratch.robinpatel.table_reference_in_view
;



USE ROLE pipelinerunner
;

CALL data_vault_mvp.dwh.table_usage('scratch.robinpatel.table_usage',
									'hygiene_snapshot_vault_mvp.travelbird_mysql.image_pool_imagesource, hygiene_snapshot_vault_mvp.travelbird_mysql.offers_image, hygiene_snapshot_vault_mvp.travelbird_mysql.packaging_tourpackageconfiguration_weekdays')
;

SELECT *
FROM scratch.robinpatel.table_usage
;

-- result: []

------------------------------------------------------------------------------------------------------------------------

image_pool_imagesource -- created new ingest
offers_image -- migrated from generic hygiene
packaging_tourpackageconfiguration_weekdays -- created new ingest


SELECT
	COUNT(*)
FROM image_pool_imagesource
;

SELECT
	COUNT(*)
FROM latest_vault.travelbird_mysql.image_pool_imagesource
;

SELECT
	COUNT(*)
FROM offers_image
;

SELECT
	COUNT(*)
FROM latest_vault.travelbird_mysql.offers_image
;

SELECT
	COUNT(*)
FROM packaging_tourpackageconfiguration_weekdays
;

SELECT
	COUNT(*)
FROM latest_vault.travelbird_mysql.packaging_tourpackageconfiguration_weekdays
;

SELECT *
FROM raw_vault_mvp.travelbird_mysql.image_pool_imagesource
;



------------------------------------------------------------------------------------------------------------------------
-- Source system:


SELECT MIN(updated_at_dts)
FROm travelbird.image_pool_imagesource;
-- 2019-01-08 13:24:48.927523

SELECT count(*)
FROm travelbird.image_pool_imagesource;
-- 155,234 rows


SELECT count(*)
FROm travelbird.offers_image;
-- 127,203 rows

SELECT count(*)
FROm travelbird.packaging_tourpackageconfiguration_weekdays;
-- 0 rows

airflow dags backfill \
incoming__travelbird_mysql__image_pool_imagesource__daily_at_00h30 \
--reset-dagruns \
--start-date "2019-01-08 00:30:00" \
--end-date "2019-01-08 00:30:00"


incoming__travelbird_mysql__image_pool_imagesource__daily_at_00h30
incoming__travelbird_mysql__offers_image__daily_at_00h30
incoming__travelbird_mysql__packaging_tourpackageconfiguration_weekdays__daily_at_00h30


    ------------------------------------------------------------------------------------------------------------------------


DROP TABLE raw_vault_mvp.travelbird_mysql.image_pool_imagesource;
DROP TABLE hygiene_vault_mvp.travelbird_mysql.image_pool_imagesource;
DROP TABLE hygiene_snapshot_vault_mvp.travelbird_mysql.image_pool_imagesource;


DROP TABLE raw_vault_mvp.travelbird_mysql.offers_image;
DROP TABLE hygiene_vault_mvp.travelbird_mysql.offers_image;
DROP TABLE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_image;

------------------------------------------------------------------------------------------------------------------------

SELECT * FROM DATA_VAULT_MVP.TRAVELBIRD_MYSQL_SNAPSHOTS.packaging_tourpackageconfiguration_weekdays;
