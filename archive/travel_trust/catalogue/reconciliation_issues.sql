SELECT * FROM se.data.se_voucher sv WHERE sv.voucher_id = 32252;

self_describing_task --include 'dv/dwh/transactional/se_voucher.py'  --method 'run' --start '2021-03-15 00:00:00' --end '2021-03-15 00:00:00'
self_describing_task --include 'se/data_pii/finance_models/se_voucher.py'  --method 'run' --start '2021-03-15 00:00:00' --end '2021-03-15 00:00:00'
self_describing_task --include 'se/data/finance_models/se_voucher.py'  --method 'run' --start '2021-03-15 00:00:00' --end '2021-03-15 00:00:00'
self_describing_task --include 'se/finance/finance_models/se_voucher.py'  --method 'run' --start '2021-03-15 00:00:00' --end '2021-03-15 00:00:00'

SELECT * FROm se.data.se_voucher sv WHERE sv.voucher_id = 32250;

SELECT * FROM data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot aas
INNER JOIN data_vault_mvp.cms_mysql_snapshots.territory_snapshot ts ON aas.territory_id = ts.id
WHERE ts.name = 'TL';
SELECT * FROM data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc  WHERE mtmc.touch_affiliate_territory= 'TL';


SELECT * FROM se.data.tb_booking tb WHERE tb.booking_id = 'TB-21904768';

SELECT * FROM se.data.tb_offer t WHERE t.tb_offer_id = 116985;

airflow backfill --start_date '2021-03-16 07:00:00' --end_date '2021-03-16 07:00:00' --task_regex '.*' se_finance_object_creation__daily_at_07h00