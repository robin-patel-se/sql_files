USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_science_dev_robin.nextoken_prod
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.nextoken_prod.ncf_user_embeddings
	CLONE data_science.nextoken_prod.ncf_user_embeddings
;

SELECT GET_DDL('table', 'data_science.nextoken_prod.ncf_user_embeddings')
;

CREATE OR REPLACE TABLE ncf_user_embeddings
(
	territory_id  NUMBER(38, 0),
	user_id       NUMBER(38, 0),
	feature_key   VARCHAR(30),
	feature_value FLOAT
)
;

SELECT *
FROM data_science.nextoken_prod.ncf_user_embeddings
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.data_science__ncf_user_embeddings_snapshot
	CLONE data_vault_mvp.dwh.data_science__ncf_user_embeddings_snapshot
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.data_science.nextoken.ncf_user_embeddings_snapshot.py' \
    --method 'run' \
    --start '2024-12-03 00:00:00' \
    --end '2024-12-03 00:00:00'


data_science.nextoken_prod.ncf_user_embeddings



------------------------------------------------------------------------------------------------------------------------

SELECT GET_DDL('table', 'data_science.nextoken_prod.ncf_deal_embeddings')
;


CREATE OR REPLACE TABLE ncf_deal_embeddings
(
	territory_id  NUMBER(38, 0),
	deal_id       VARCHAR(16777216),
	feature_key   VARCHAR(30),
	feature_value FLOAT
)
;


USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_science_dev_robin.nextoken_prod
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.nextoken_prod.ncf_deal_embeddings
	CLONE data_science.nextoken_prod.ncf_deal_embeddings
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.data_science__ncf_deal_embeddings_snapshot
	CLONE data_vault_mvp.dwh.data_science__ncf_deal_embeddings_snapshot
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.data_science.nextoken.ncf_deal_embeddings_snapshot.py' \
    --method 'run' \
    --start '2024-12-03 00:00:00' \
    --end '2024-12-03 00:00:00'


CREATE SCHEMA IF NOT EXISTS se_dev_robin.data_science
;


------------------------------------------------------------------------------------------------------------------------


SELECT
	COUNT(DISTINCT user_id)
FROM data_science.nextoken_prod.ncf_user_embeddings
-- WHERE user_id = 28
-- AND territory_id = 11
-- AND feature_key = 0
;



WITH
	final AS (
		SELECT
			view_date,
			shiro_user_id,
			user_object:userObject:als:feature_1   AS feature_1,
			user_object:userObject:als:feature_2   AS feature_2,
			user_object:userObject:als:feature_3   AS feature_3,
			user_object:userObject:als:feature_4   AS feature_4,
			user_object:userObject:als:feature_5   AS feature_5,
			user_object:userObject:als:feature_6   AS feature_6,
			user_object:userObject:als:feature_7   AS feature_7,
			user_object:userObject:als:feature_8   AS feature_8,
			user_object:userObject:als:feature_9   AS feature_9,
			user_object:userObject:als:feature_10  AS feature_10,
			user_object:userObject:als:feature_11  AS feature_11,
			user_object:userObject:als:feature_12  AS feature_12,
			user_object:userObject:als:feature_13  AS feature_13,
			user_object:userObject:als:feature_14  AS feature_14,
			user_object:userObject:als:feature_15  AS feature_15,
			user_object:userObject:als:feature_16  AS feature_16,
			user_object:userObject:als:feature_17  AS feature_17,
			user_object:userObject:als:feature_18  AS feature_18,
			user_object:userObject:als:feature_19  AS feature_19,
			user_object:userObject:als:feature_20  AS feature_20,
			user_object:userObject:als:feature_21  AS feature_21,
			user_object:userObject:als:feature_22  AS feature_22,
			user_object:userObject:als:feature_23  AS feature_23,
			user_object:userObject:als:feature_24  AS feature_24,
			user_object:userObject:als:feature_25  AS feature_25,
			user_object:userObject:als:feature_26  AS feature_26,
			user_object:userObject:als:feature_27  AS feature_27,
			user_object:userObject:als:feature_28  AS feature_28,
			user_object:userObject:als:feature_29  AS feature_29,
			user_object:userObject:als:feature_30  AS feature_30,
			user_object:userObject:als:feature_31  AS feature_31,
			user_object:userObject:als:feature_32  AS feature_32,
			user_object:userObject:als:feature_33  AS feature_33,
			user_object:userObject:als:feature_34  AS feature_34,
			user_object:userObject:als:feature_35  AS feature_35,
			user_object:userObject:als:feature_36  AS feature_36,
			user_object:userObject:als:feature_37  AS feature_37,
			user_object:userObject:als:feature_38  AS feature_38,
			user_object:userObject:als:feature_39  AS feature_39,
			user_object:userObject:als:feature_40  AS feature_40,
			user_object:userObject:als:feature_41  AS feature_41,
			user_object:userObject:als:feature_42  AS feature_42,
			user_object:userObject:als:feature_43  AS feature_43,
			user_object:userObject:als:feature_44  AS feature_44,
			user_object:userObject:als:feature_45  AS feature_45,
			user_object:userObject:als:feature_46  AS feature_46,
			user_object:userObject:als:feature_47  AS feature_47,
			user_object:userObject:als:feature_48  AS feature_48,
			user_object:userObject:als:feature_49  AS feature_49,
			user_object:userObject:als:feature_50  AS feature_50,
			user_object:userObject:als:feature_51  AS feature_51,
			user_object:userObject:als:feature_52  AS feature_52,
			user_object:userObject:als:feature_53  AS feature_53,
			user_object:userObject:als:feature_54  AS feature_54,
			user_object:userObject:als:feature_55  AS feature_55,
			user_object:userObject:als:feature_56  AS feature_56,
			user_object:userObject:als:feature_57  AS feature_57,
			user_object:userObject:als:feature_58  AS feature_58,
			user_object:userObject:als:feature_59  AS feature_59,
			user_object:userObject:als:feature_60  AS feature_60,
			user_object:userObject:als:feature_61  AS feature_61,
			user_object:userObject:als:feature_62  AS feature_62,
			user_object:userObject:als:feature_63  AS feature_63,
			user_object:userObject:als:feature_64  AS feature_64,
			user_object:userObject:als:feature_65  AS feature_65,
			user_object:userObject:als:feature_66  AS feature_66,
			user_object:userObject:als:feature_67  AS feature_67,
			user_object:userObject:als:feature_68  AS feature_68,
			user_object:userObject:als:feature_69  AS feature_69,
			user_object:userObject:als:feature_70  AS feature_70,
			user_object:userObject:als:feature_71  AS feature_71,
			user_object:userObject:als:feature_72  AS feature_72,
			user_object:userObject:als:feature_73  AS feature_73,
			user_object:userObject:als:feature_74  AS feature_74,
			user_object:userObject:als:feature_75  AS feature_75,
			user_object:userObject:als:feature_76  AS feature_76,
			user_object:userObject:als:feature_77  AS feature_77,
			user_object:userObject:als:feature_78  AS feature_78,
			user_object:userObject:als:feature_79  AS feature_79,
			user_object:userObject:als:feature_80  AS feature_80,
			user_object:userObject:als:feature_81  AS feature_81,
			user_object:userObject:als:feature_82  AS feature_82,
			user_object:userObject:als:feature_83  AS feature_83,
			user_object:userObject:als:feature_84  AS feature_84,
			user_object:userObject:als:feature_85  AS feature_85,
			user_object:userObject:als:feature_86  AS feature_86,
			user_object:userObject:als:feature_87  AS feature_87,
			user_object:userObject:als:feature_88  AS feature_88,
			user_object:userObject:als:feature_89  AS feature_89,
			user_object:userObject:als:feature_90  AS feature_90,
			user_object:userObject:als:feature_91  AS feature_91,
			user_object:userObject:als:feature_92  AS feature_92,
			user_object:userObject:als:feature_93  AS feature_93,
			user_object:userObject:als:feature_94  AS feature_94,
			user_object:userObject:als:feature_95  AS feature_95,
			user_object:userObject:als:feature_96  AS feature_96,
			user_object:userObject:als:feature_97  AS feature_97,
			user_object:userObject:als:feature_98  AS feature_98,
			user_object:userObject:als:feature_99  AS feature_99,
			user_object:userObject:als:feature_100 AS feature_100
		FROM se.data_science.data_science_user_latent_als_factors_snapshot
	)

SELECT *
FROM final
;

SELECT *
FROM se.data_science.data_science_user_latent_als_factors_snapshot
;

WITH
	territory_agg AS (
		SELECT
			user_id,
			territory_id,
			se.data.territory_name_from_territory_id(territory_id) AS territory_name,
			OBJECT_AGG(feature_key, feature_value)                 AS feature_list
		FROM data_science.nextoken_prod.ncf_user_embeddings nue
			INNER JOIN se.data_pii.se_user_attributes sua
					   ON nue.user_id = sua.shiro_user_id AND nue.territory_id = sua.current_affiliate_territory_id
		WHERE user_id = 5981071 -- todo remove
		GROUP BY 1, 2, 3
	),
	feature_construct AS (
		SELECT
			user_id,
			territory_id,
			territory_name,
			OBJECT_CONSTRUCT(
					'ncf', ta.feature_list,
					'territory_id', ta.territory_id,
					'territory_name', ta.territory_name
			) AS territory_object
		FROM territory_agg ta
	),
	final AS (
		SELECT
			user_id,
			OBJECT_CONSTRUCT(
					'user_id', user_id,
					'userObject', fc.territory_object
			) AS user_object
		FROM feature_construct fc
	)
SELECT *
FROM final
;


SELECT *
FROM se.data.se_user_attributes sua
WHERE sua.shiro_user_id = 5981071
;


WITH
	territory_count AS (
		SELECT
			user_id,
			COUNT(DISTINCT territory_id) AS territories
		FROM data_science.nextoken_prod.ncf_user_embeddings
		GROUP BY 1
	)
SELECT
	territories,
	COUNT(DISTINCT user_id) AS users
FROM territory_count
GROUP BY 1
;



SELECT *
FROM data_vault_mvp.dwh.data_science__user_latent_als_factors_snapshot dsulafs
WHERE dsulafs.shiro_user_id = 10527537 AND dsulafs.view_date = CURRENT_DATE - 1
;

------------------------------------------------------------------------------------------------------------------------

USE ROLE personal_role__robinpatel
;

CREATE SCHEMA IF NOT EXISTS data_science_dev_robin.nextoken_prod
;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.nextoken_prod.ncf_user_embeddings
	CLONE data_science.nextoken_prod.ncf_user_embeddings
;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.data_science__ncf_user_embeddings_snapshot
	CLONE data_vault_mvp.dwh.data_science__ncf_user_embeddings_snapshot
;

self_describing_task
\
    --include 'biapp.task_catalogue.dv.data_science.nextoken.ncf_user_embeddings.py' \
    --method 'run' \
    --start '2024-12-13 00:00:00' \
    --end '2024-12-13 00:00:00'

	user_id,
	CURRENT_DATE() AS view_date,
	user_object

SELECT *
FROM data_science_dev_robin.nextoken_prod.ncf_user_embeddings


;


------------------------------------------------------------------------------------------------------------------------

-- deal embeddings

SELECT GET_DDL('table', 'data_science.nextoken_prod.ncf_deal_embeddings')
;

SELECT *
FROM data_science.nextoken_prod.ncf_deal_embeddings
;

WITH
	feature_agg AS (
		SELECT
			nde.deal_id,
			nde.territory_id,
			se.data.territory_name_from_territory_id(territory_id) AS territory_name,
			OBJECT_AGG(feature_key, feature_value)                 AS feature_list
		FROM data_science.nextoken_prod.ncf_deal_embeddings nde
		GROUP BY 1, 2
	)
SELECT
	fa.deal_id,
	fa.territory_id,
	OBJECT_CONSTRUCT(
			'territoryId', fa.territory_id,
			'territoryName', territory_name,
			'dealId', fa.deal_id,
			'ncf', feature_list
	) AS deal_object,
	SHA2(
			COALESCE(fa.deal_id::VARCHAR, '') ||
			COALESCE(fa.territory_id::VARCHAR, '') ||
			COALESCE(deal_object::VARCHAR, '')
	) AS row_hash
FROM feature_agg fa
;


SELECT *
FROM data_vault_mvp.dwh.data_science__deal_latent_als_factors_snapshot dsdlafs



CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.nextoken_prod.ncf_deal_embeddings
	CLONE data_science.nextoken_prod.ncf_deal_embeddings
;

SELECT * FROm data_vault_mvp_dev_robin.dwh.data_science__ncf_deal_embeddings__step02__modelling;
DROP TABLE  data_vault_mvp_dev_robin.dwh.data_science__ncf_deal_embeddings;

SELECT * FROm data_vault_mvp_dev_robin.dwh.data_science__ncf_deal_embeddings;


DROP TABLE data_vault_mvp_dev_robin.dwh.data_science__ncf_deal_embeddings_snapshot;

SELECT * FROm data_science_dev_robin.nextoken_prod.ncf_deal_embeddings;

SELECT * FROm data_vault_mvp_dev_robin.dwh.data_science__ncf_deal_embeddings_snapshot;


SELECT * FROm data_science.predictive_modeling.user_deal_events;


SELECT * FROm data_vault_mvp.dwh.data_science__user_latent_als_factors dsulaf;

DROP TABLE data_vault_mvp_dev_robin.dwh.data_science__ncf_deal_embeddings_snapshot;
SELECT * FROM data_vault_mvp_dev_robin.dwh.data_science__ncf_user_embeddings_snapshot;



------------------------------------------------------------------------------------------------------------------------


USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_science_dev_robin.nextoken_prod;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.nextoken_prod.ncf_user_embeddings
CLONE data_science.nextoken_prod.ncf_user_embeddings;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.data_science__ncf_user_embeddings
CLONE data_vault_mvp.dwh.data_science__ncf_user_embeddings;

self_describing_task \
    --include 'biapp.task_catalogue.dv.data_science.nextoken.ncf_user_embeddings.py' \
    --method 'run' \
    --start '2024-12-17 00:00:00' \
    --end '2024-12-17 00:00:00'

self_describing_task \
	--include 'biapp/task_catalogue/dv/data_science/nextoken/ncf_user_embeddings_snapshot.py' \
	--method 'run' \
	--start '2024-12-16 00:00:00' \
 	--end '2024-12-16 00:00:00'

------------------------------------------------------------------------------------------------------------------------

USE ROLE PERSONAL_ROLE__ROBINPATEL;

CREATE SCHEMA IF NOT EXISTS data_science_dev_robin.nextoken_prod;

CREATE OR REPLACE TRANSIENT TABLE data_science_dev_robin.nextoken_prod.ncf_deal_embeddings
CLONE data_science.nextoken_prod.ncf_deal_embeddings;

-- optional statement to create the module target table --
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.data_science__ncf_deal_embeddings
CLONE data_vault_mvp.dwh.data_science__ncf_deal_embeddings;

self_describing_task \
    --include 'biapp.task_catalogue.dv.data_science.nextoken.ncf_deal_embeddings.py' \
    --method 'run' \
    --start '2024-12-17 00:00:00' \
    --end '2024-12-17 00:00:00'

self_describing_task \
	--include 'biapp/task_catalogue/dv/data_science/nextoken/ncf_deal_embeddings_snapshot.py' \
 	--method 'run' \
 	--start '2024-12-16 00:00:00' \
 	--end '2024-12-16 00:00:00'

------------------------------------------------------------------------------------------------------------------------

self_describing_task --include 'biapp/task_catalogue/se/data_science/ncf_deal_embeddings_snapshot.py'  --method 'run' --start '2024-12-16 00:00:00' --end '2024-12-16 00:00:00'
self_describing_task --include 'biapp/task_catalogue/se/data_science/ncf_deal_embeddings.py'  --method 'run' --start '2024-12-16 00:00:00' --end '2024-12-16 00:00:00'
self_describing_task --include 'biapp/task_catalogue/se/data_science/ncf_user_embeddings_snapshot.py'  --method 'run' --start '2024-12-16 00:00:00' --end '2024-12-16 00:00:00'
self_describing_task --include 'biapp/task_catalogue/se/data_science/ncf_user_embeddings.py'  --method 'run' --start '2024-12-16 00:00:00' --end '2024-12-16 00:00:00'

------------------------------------------------------------------------------------------------------------------------

SELECT * FROM data_vault_mvp_dev_robin.dwh.data_science__ncf_user_embeddings;
SELECT * FROM data_vault_mvp_dev_robin.dwh.data_science__ncf_user_embeddings_snapshot;
SELECT * FROM data_vault_mvp_dev_robin.dwh.data_science__ncf_deal_embeddings;
SELECT * FROM data_vault_mvp_dev_robin.dwh.data_science__ncf_deal_embeddings_snapshot;


SELECT * FROM se.data_science.data_science__ncf_user_embeddings;
SELECT * FROM se.data_science.data_science__ncf_user_embeddings_snapshot;
SELECT * FROM se.data_science.data_science__ncf_deal_embeddings;
SELECT * FROM se.data_science.data_science__ncf_deal_embeddings_snapshot;