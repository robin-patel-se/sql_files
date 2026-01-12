dag_id: incoming__travelbird_mysql__pricing_conditionbookingdatetime__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditionbookingdatetime.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditionbookingoffset__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditionbookingoffset.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditiondate__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditiondate.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditionday__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditionday.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditionduration__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditionduration.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditioneventdate__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditioneventdate.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditioneventweekday__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditioneventweekday.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditionmarketcountry__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditionmarketcountry.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditionnot__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditionnot.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditionoccupancy__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditionoccupancy.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditionor__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditionor.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditionpersonage__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditionpersonage.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditionpersontypecount__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditionpersontypecount.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_conditionrequiredobject__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_conditionrequiredobject.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_rate__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_rate.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_ratecondition__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_ratecondition.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

dag_id: incoming__travelbird_mysql__pricing_rateplan__daily_at_00h30
manifest: biapp/manifests/incoming/travelbird_mysql/pricing_rateplan.json
hygiene_snapshot: False
dependencies: False
bulk_snapshot: False

------------------------------------------------------------------------------------------------------------------------
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionbookingdatetime - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionbookingoffset - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditiondate - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionday - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionduration - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditioneventdate - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditioneventweekday - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionmarketcountry - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionnot - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionoccupancy - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionor - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionpersonage - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionpersontypecount - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_conditionrequiredobject - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_rate - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_ratecondition - *already migrated*
python biapp/adhoc/migrate_manifests/check_dataset_status.py travelbird_mysql.pricing_rateplan - *already migrated*


------------------------------------------------------------------------------------------------------------------------

dataset_task --include 'travelbird_mysql.pricing_conditionbookingdatetime' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditionbookingoffset' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditiondate' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditionday' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditionduration' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditioneventdate' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditioneventweekday' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditionmarketcountry' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditionnot' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditionoccupancy' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditionor' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditionpersonage' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditionpersontypecount' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_conditionrequiredobject' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_rate' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_ratecondition' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'
dataset_task --include 'travelbird_mysql.pricing_rateplan' --operation LatestRecordsOperation --method 'run' --upstream --start '2023-11-16 00:30:00' --end '2023-11-16 00:30:00'


------------------------------------------------------------------------------------------------------------------------

http://localhost:8080/dags/incoming__travelbird_mysql__offers_image__daily_at_00h30/grid


http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditionbookingdatetime__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditionbookingoffset__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditiondate__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditionday__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditionduration__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditioneventdate__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditioneventweekday__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditionmarketcountry__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditionnot__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditionoccupancy__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditionor__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditionpersonage__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditionpersontypecount__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_conditionrequiredobject__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_rate__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_ratecondition__daily_at_00h30/grid
http://localhost:8080/dags/incoming__travelbird_mysql__pricing_rateplan__daily_at_00h30/grid