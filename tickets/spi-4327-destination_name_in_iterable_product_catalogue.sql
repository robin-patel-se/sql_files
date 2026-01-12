CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.affiliate CLONE latest_vault.cms_mysql.affiliate
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.base_offer_translation CLONE latest_vault.cms_mysql.base_offer_translation
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.hotel_sale_offer CLONE latest_vault.cms_mysql.hotel_sale_offer
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.se_api.sales_kingfisher CLONE latest_vault.se_api.sales_kingfisher
;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.cms_mysql.territory CLONE latest_vault.cms_mysql.territory
;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_booking_review CLONE data_vault_mvp.dwh.user_booking_review
;

SELECT *
FROM latest_vault_dev_robin.se_api.sales_kingfisher
;

SELECT *
FROM se.data.sales_kingfisher
WHERE id = 'A11871'
;

USE ROLE personal_role__robinpatel
;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__catalogue_product CLONE data_vault_mvp.dwh.iterable__catalogue_product
;

WITH
	lat_flat AS (
		SELECT
			stt.*
		FROM data_vault_mvp_dev_robin.dwh.iterable__catalogue_product u,
			 LATERAL SPLIT_TO_TABLE(u.destination_name, ', ') stt
	)
SELECT
	index,
	COUNT(*)
FROM lat_flat lf
GROUP BY 1

;


SELECT
	u.destination_name,
	SPLIT_PART(u.destination_name, ', ', 1) AS destination_name_split_one,
	SPLIT_PART(u.destination_name, ', ', 2) AS destination_name_split_two,
	SPLIT_PART(u.destination_name, ', ', 3) AS destination_name_split_three,
	SPLIT_PART(u.destination_name, ', ', 4) AS destination_name_split_four,
	SPLIT_PART(u.destination_name, ', ', 5) AS destination_name_split_five
FROM data_vault_mvp_dev_robin.dwh.iterable__catalogue_product u;



self_describing_task --include 'biapp/task_catalogue/staging/outgoing/iterable/catalogue_product_sandbox/modelling.py'  --method 'run' --start '2023-10-19 00:00:00' --end '2023-10-19 00:00:00'

dataset_task --include 'outgoing.iterable.catalogue_product_sandbox' --operation UnloadOperation --method 'run' --start '2023-10-19 00:30:00' --end '2023-10-19 00:30:00'

dataset_task --include 'outgoing.iterable.catalogue_product_sandbox' --operation DistributeOperation --method 'run' --start '2023-10-19 00:30:00' --end '2023-10-19 00:30:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__catalogue_product_20231020 CLONE data_vault_mvp_dev_robin.dwh.iterable__catalogue_product;

UPDATE data_vault_mvp_dev_robin.dwh.iterable__catalogue_product target
SET target.updated_at = CURRENT_TIMESTAMP::TIMESTAMP
WHERE target.sale_active;

SELECT * FROM data_vault_mvp_dev_robin.dwh.iterable__catalogue_product icp WHERE icp.sale_active


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.iterable__user_profile_activity CLONE data_vault_mvp.dwh.iterable__user_profile_activity;

self_describing_task --include 'biapp/task_catalogue/staging/outgoing/iterable/user_profile_activity_first_quartile/modelling.py'  --method 'run' --start '2023-10-19 00:00:00' --end '2023-10-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/staging/outgoing/iterable/user_profile_activity_second_quartile/modelling.py'  --method 'run' --start '2023-10-19 00:00:00' --end '2023-10-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/staging/outgoing/iterable/user_profile_activity_third_quartile/modelling.py'  --method 'run' --start '2023-10-19 00:00:00' --end '2023-10-19 00:00:00'
self_describing_task --include 'biapp/task_catalogue/staging/outgoing/iterable/user_profile_activity_fourth_quartile/modelling.py'  --method 'run' --start '2023-10-19 00:00:00' --end '2023-10-19 00:00:00'

------------------------------------------------------------------------------------------------------------------------

USE ROLE pipelinerunner;

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp.dwh.iterable__catalogue_product_20231023 CLONE data_vault_mvp.dwh.iterable__catalogue_product;

UPDATE data_vault_mvp.dwh.iterable__catalogue_product target
SET target.updated_at = CURRENT_TIMESTAMP::TIMESTAMP
WHERE target.sale_active;

