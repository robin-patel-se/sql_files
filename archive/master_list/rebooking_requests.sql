SELECT rr.unique_transaction_id
FROM hygiene_vault_mvp.sfsc.rebooking_requests rr
WHERE rr.rank = 1
GROUP BY 1
HAVING count(*) > 1;

SELECT rr.unique_transaction_id
FROM hygiene_snapshot_vault_mvp.sfsc.rebooking_requests rr
GROUP BY 1
HAVING count(*) > 1;

CREATE OR REPLACE TABLE hygiene_vault_mvp_dev_robin.sfsc.rebooking_requests CLONE hygiene_vault_mvp.sfsc.rebooking_requests;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfsc.rebooking_requests CLONE raw_vault_mvp.sfsc.rebooking_request_cases_ho;
CREATE OR REPLACE TABLE raw_vault_mvp_dev_robin.sfsc.rebooking_requests CLONE raw_vault_mvp.sfsc.rebooking_request_cases_pkg;
self_describing_task --include 'staging/hygiene/sfsc/rebooking_requests'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/sfsc/rebooking_requests'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'



SELECT rr.transaction_id
FROM hygiene_vault_mvp.sfsc.rebooking_requests rr
GROUP BY 1
HAVING count(*) > 1;


SELECT rr.transaction_id
FROM hygiene_snapshot_vault_mvp_dev_robin.sfsc.rebooking_requests rr
GROUP BY 1
HAVING count(*) > 1;

SELECT unique_transaction_id,
       count(*)
FROM (
         SELECT *
         FROM hygiene_vault_mvp_dev_robin.sfsc.rebooking_requests
         WHERE rank = 1
     )
GROUP BY 1
HAVING count(*) > 1;

SELECT transaction_id, booking_id
FROM hygiene_snapshot_vault_mvp.sfsc.rebooking_requests rr
WHERE LEFT(rr.booking_id, 3) = '218';