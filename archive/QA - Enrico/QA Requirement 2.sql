--child policy
--surface how its shown in the cms


--enrich the global sale attributes with live on site from the opportunity
SELECT *
FROM se.data.global_sale_attributes gsa;
SELECT o.id,
       o.name,
       o.status__c,
       o.stagename
FROM hygiene_snapshot_vault_mvp.sfsc.opportunity o;

SELECT gsa.opportunity_id_full,
       gsa.account_name,
       o.name,
       o.stagename,
       o.status__c,
       gsa.proposed_start_date
FROM se.data.global_sale_attributes gsa
         LEFT JOIN hygiene_snapshot_vault_mvp.sfsc.opportunity o ON gsa.opportunity_id_full = o.id
WHERE gsa.proposed_start_date >= CURRENT_DATE - 30
  AND o.stagename IS DISTINCT FROM o.status__c;

SELECT gsa.owner,
       gsa.owner_role
FROM se.data.global_sale_attributes gsa;

SELECT a.ownerid,
       se.data.salesforce_user_name(a.ownerid)
FROM hygiene_snapshot_vault_mvp.sfsc.account a
WHERE a.ownerid = '005w0000004DwmTAAS';

CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.opportunity CLONE hygiene_snapshot_vault_mvp.sfsc.opportunity;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.sfsc.account CLONE hygiene_snapshot_vault_mvp.sfsc.account;

self_describing_task --include 'dv/dwh/salesforce/salesforce_sale_opportunity.py'  --method 'run' --start '2021-03-31 00:00:00' --end '2021-03-31 00:00:00'



SELECT o.allocation_loaded_by__c
FROM hygiene_snapshot_vault_mvp.sfsc.opportunity o
WHERE LEFT(o.id, 15) = '0066900001NlU6v';

SELECT *
FROM se.data.se_offer
WHERE children_free > 0
  AND child_rate > 0
ORDER BY sale_id DESC

self_describing_task --include 'se/data_pii/dwh/global_sale_attributes.py'  --method 'run' --start '2021-03-31 00:00:00' --end '2021-03-31 00:00:00'
self_describing_task --include 'se/data/dwh/global_sale_attributes.py'  --method 'run' --start '2021-03-31 00:00:00' --end '2021-03-31 00:00:00'

/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/se/data_pii/dwh/global_sale_attributes.py
/Users/robin/myrepos/one-data-pipeline/biapp/task_catalogue/se/data/dwh/global_sale_attributes.py