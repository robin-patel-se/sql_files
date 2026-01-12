USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_science_dev_robin.predictive_modeling
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.predictive_modeling.deal_latent_als_factors
	CLONE data_science.predictive_modeling.deal_latent_als_factors
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.data_science__deal_latent_als_factors
	CLONE data_vault_mvp.dwh.data_science__deal_latent_als_factors
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.data_science.predictive_modelling.deal_latent_als_factors.py' \
    --method 'run' \
    --start '2024-11-18 00:00:00' \
    --end '2024-11-18 00:00:00'

-- check there are IT sales
SELECT *
FROM data_vault_mvp_dev_robin.dwh.data_science__deal_latent_als_factors
WHERE data_science__deal_latent_als_factors.territory = 'IT'

-- compare in prod
SELECT
	data_science__deal_latent_als_factors.territory,
	COUNT(*)
FROM data_vault_mvp.dwh.data_science__deal_latent_als_factors
GROUP BY 1
;

-- compare in dev
SELECT
	data_science__deal_latent_als_factors.territory,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.data_science__deal_latent_als_factors
GROUP BY 1
;



USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_vault_mvp_dev_robin.dwh
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.data_science__deal_latent_als_factors_snapshot
	CLONE data_vault_mvp.dwh.data_science__deal_latent_als_factors_snapshot
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.data_science.predictive_modelling.deal_latent_als_factors_snapshot.py' \
    --method 'run' \
    --start '2024-11-18 00:00:00' \
    --end '2024-11-18 00:00:00'


-- check that the new territory made it in today's snapshot
SELECT
	view_date,
	territory,
	COUNT(*)
FROM data_vault_mvp_dev_robin.dwh.data_science__deal_latent_als_factors_snapshot
WHERE view_date >= CURRENT_DATE - 2
GROUP BY 1, 2;

SELECT * FROM se.data.se_territory st;



SELECT
	view_date,
	territory,
	COUNT(*)
FROM data_vault_mvp.dwh.data_science__deal_latent_als_factors_snapshot
WHERE view_date >= CURRENT_DATE - 2
GROUP BY 1, 2;


SELECT * FROm se.data.scv_touch_basic_attributes stba;