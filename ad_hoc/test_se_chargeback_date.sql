--extract gsheet to s3
dataset_task \
--include 'finance_gsheets.chargebacks_se*' \
--operation ExtractOperation \
--method 'run' \
--start '2020-11-08 00:00:00' \
--end '2020-11-08 00:00:00'

--load s3 to transient table
dataset_task \
--include 'finance_gsheets.chargebacks_se*' \
--operation RawIngestOperation \
--method 'run' \
--start '2020-11-08 00:00:00' \
--end '2020-11-08 00:00:00'

--load transient table to raw_vault table
dataset_task \
--include 'finance_gsheets.chargebacks_se*' \
--operation ProductionIngestOperation \
--method 'run' \
--start '2020-11-08 00:00:00' \
--end '2020-11-08 00:00:00'
DROP TABLE raw_vault_mvp_dev_robin.finance_gsheets.chargebacks_se;

CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.finance_gsheets.chargebacks_se clone raw_vault_mvp.finance_gsheets.chargebacks_se;

ALTER TABLE raw_vault_mvp_dev_robin.finance_gsheets.chargebacks_se RENAME COLUMN date to cb_date;

self_describing_task --include 'hygiene/finance_gsheets/chargebacks_se.py'  --method 'run' --start '2020-11-08 00:00:00' --end '2020-11-08 00:00:00'
self_describing_task --include 'hygiene_snapshots/finance_gsheets/chargebacks_se.py'  --method 'run' --start '2020-11-08 00:00:00' --end '2020-11-08 00:00:00'
self_describing_task --include 'se/data/payments/se_chargebacks.py'  --method 'run' --start '2020-11-08 00:00:00' --end '2020-11-08 00:00:00';
