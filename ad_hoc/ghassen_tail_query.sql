WITH
	users_with_no_tail AS (
		SELECT
			dds.territory_id,
			dds.user_id,
			LISTAGG(dds.deal_id, '|')
					WITHIN GROUP (ORDER BY dds.planning_position) AS athena_deals,
			MAX(planning_position)                                AS max_pos
		FROM data_science.operational_output.daily_deals_selections dds
		WHERE dds.planning_date = '2023-09-28'
		  AND dds.user_id > 0
		GROUP BY 1, 2
		HAVING max_pos < 10
	),
	generic_tail_deals AS (
		SELECT
			dds.territory_id,
			user_id,
			LISTAGG(dds.deal_id, '|') WITHIN GROUP (ORDER BY dds.planning_position) AS tail
		FROM data_science.operational_output.daily_deals_selections dds
		WHERE dds.planning_date = '2023-09-28'
		  AND dds.user_id = -1
		  AND planning_position > 9
		GROUP BY 1, 2
	)

SELECT
	t.territory_id,
	t.user_id,
	CONCAT(t.athena_deals, '|', gt.tail) AS athena_deals
FROM users_with_no_tail t
	LEFT JOIN generic_tail_deals gt
			  ON t.territory_id = gt.territory_id

WHERE t.user_id = 80610655


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge
;

WITH
	users_with_no_tail AS (
		SELECT
			dds.territory_id,
			dds.user_id,
			dds.deal_id,
			dds.planning_position
		FROM data_science.operational_output.daily_deals_selections dds
		WHERE dds.planning_date = '2023-09-28'
		  AND dds.user_id > 0
		  AND dds.planning_position < 10
	),
	generic_tail_deals AS (
		SELECT
			dds.territory_id,
			user_id,
			dds.deal_id,
			dds.planning_position
		FROM data_science.stg.daily_deals_tail_selections dds
		WHERE dds.planning_date = '2023-09-28'
		  AND dds.user_id = -1
		  AND territory_id = 4
		  AND planning_position > 9
	),
	distinct_users AS (
		-- get a distinct list of users to synthetically create a list of tail
		SELECT DISTINCT
			uwnt.user_id,
			uwnt.territory_id
		FROM users_with_no_tail uwnt
	),
	explode_tail_deals AS (
		-- create a synthetic user tail by exploding out tail deals for each user
		SELECT
			uwnt.territory_id,
			uwnt.user_id,
			gtd.deal_id,
			gtd.planning_position
		FROM distinct_users uwnt
			INNER JOIN generic_tail_deals gtd ON uwnt.territory_id = gtd.territory_id
	),
	stack AS (
		-- stack the user deals
		SELECT *
		FROM users_with_no_tail
		UNION ALL
		SELECT *
		FROM explode_tail_deals
	),
	dedupe AS (
		-- return the highest planning position for each sale only
		SELECT *
		FROM stack
		QUALIFY ROW_NUMBER() OVER (PARTITION BY stack.user_id, stack.deal_id ORDER BY stack.planning_position) = 1
	)
SELECT
	d.territory_id,
	d.user_id,
	LISTAGG(d.deal_id, '|') WITHIN GROUP (ORDER BY d.planning_position) AS athena_deals
FROM dedupe d
WHERE d.user_id = 80610655 -- TODO test user
GROUP BY 1, 2
;