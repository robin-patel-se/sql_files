/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/se/data/dwh/fact_complete_booking.py

self_describing_task --include 'se/data/dwh/fact_complete_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking clone data_vault_mvp.dwh.se_booking;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_booking clone data_vault_mvp.dwh.tb_booking;

SELECT * FROM se_dev_robin.data.fact_complete_booking fcb
WHERE booking_transaction_complete = FALSE;