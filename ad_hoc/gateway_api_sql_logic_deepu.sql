WITH
	/*Athena*/
	/* Aggregate the deal IDs for each user in each territory from the daily_deals_selections table */
	athena AS (
		SELECT
			dds.territory_id,
			dds.user_id,
			LISTAGG(dds.deal_id, ',') WITHIN GROUP (ORDER BY dds.planning_position) AS athena_deals
		FROM data_science.operational_output.daily_deals_selections dds
		WHERE dds.planning_date = CURRENT_DATE
		GROUP BY 1,
				 2
	),
	/*Artemis*/
	/* Get the most recent timestamp for each territory_id */
	most_recent_ts AS (
		SELECT
			territory_id,
			MAX(last_modified_ts) AS max_last_modified_ts
		FROM data_science.operational_output.selections_conversion
		GROUP BY territory_id
	),
	/* Join the most recent selections_conversion data with the most recent timestamp */
	cte_selections_conversion AS (
		SELECT
			sc.territory_id,
			user_id,
			rec_deal_id AS deal_id,
			planning_position
		FROM data_science.operational_output.selections_conversion sc
			INNER JOIN most_recent_ts mrt ON
					sc.territory_id = mrt.territory_id
				AND sc.last_modified_ts >= mrt.max_last_modified_ts
	),
	/* Aggregate the deal IDs for each user in each territory from the selections_conversion data */
	artemis AS (
		SELECT
			cvr.territory_id,
			cvr.user_id,
			LISTAGG(cvr.deal_id, ',') WITHIN GROUP (ORDER BY cvr.planning_position) AS artemis_deals
		FROM cte_selections_conversion cvr
		GROUP BY 1,
				 2
	),
	/*Tail*/
	/* Aggregate the tail deals for the user with ID -2 */
	cte_tail_deals AS (
		SELECT
			territory_id,
			user_id,
			LISTAGG(dds.deal_id, ',') WITHIN GROUP (ORDER BY dds.planning_position) AS tail
		FROM data_science.operational_output.daily_deals_selections dds
		WHERE user_id = -2
		  AND planning_date = IFF(HOUR(CURRENT_TIMESTAMP) < 17, CURRENT_DATE, CURRENT_DATE + 1)
		GROUP BY 1,
				 2
	),
	/* Get the most recent timestamp for each territory id */
	most_recent_ts_apollo AS (
		SELECT
			territory_id,
			MAX(inference_ts) AS max_inference_ts
		FROM data_science.operational_output.booking_intent_prediction
		WHERE HOUR(inference_ts) IN (5, 6)
		GROUP BY territory_id
	),
	/*Apollo*/
	/* Get the recommended model from the booking_intent_prediction table */
	apollo AS (
		SELECT
			bip.territory_id,
			bip.user_id,
			bip.recommended_model
		FROM data_science.operational_output.booking_intent_prediction bip
			INNER JOIN most_recent_ts_apollo mrta ON
					bip.territory_id = mrta.territory_id
				AND bip.inference_ts = mrta.max_inference_ts
	),
	/*AB Test Groups*/
	/* Get ab test user group table */
	ab_test AS (
		SELECT
			territory_id,
			user_id,
			segment
		FROM data_science.tmp.experiment_user_groups_gateway_api
	)
/* Combine the data from the three tables into one result set, with the recommended_deals column determined by the recommended_model */
SELECT
	athena.territory_id,
	athena.user_id,
	CONCAT(
			IFF(apollo.recommended_model = 'Artemis' AND ab_test.segment = 'B',
				IFNULL(artemis.artemis_deals, athena.athena_deals), athena.athena_deals
				), ',',
			td.tail) AS recommended_deals
FROM athena
	LEFT JOIN
artemis ON artemis.territory_id = athena.territory_id AND artemis.user_id = athena.user_id
	LEFT JOIN
cte_tail_deals td ON athena.territory_id = td.territory_id
	LEFT JOIN
apollo ON apollo.territory_id = athena.territory_id AND apollo.user_id = athena.user_id
	LEFT JOIN
ab_test ON ab_test.territory_id = athena.territory_id AND ab_test.user_id = athena.user_id
WHERE recommended_deals IS NOT NULL
ORDER BY 1, 2

------------------------------------------------------------------------------------------------------------------------


WITH
	/*Athena*/
	/* Aggregate the deal IDs for each user in each territory from the daily_deals_selections table */
	athena AS (
		SELECT
			dds.territory_id,
			dds.user_id,
			dds.deal_id,
			dds.planning_position,
			'athena' AS source
		FROM data_science.operational_output.daily_deals_selections dds
		WHERE dds.planning_date = CURRENT_DATE
	),
	/*Artemis*/
	/* Get the most recent timestamp for each territory_id */
	most_recent_ts AS (
		SELECT
			territory_id,
			MAX(last_modified_ts) AS max_last_modified_ts
		FROM data_science.operational_output.selections_conversion
		GROUP BY territory_id
	),
	/* Join the most recent selections_conversion data with the most recent timestamp */
	cte_selections_conversion AS (
		SELECT
			sc.territory_id,
			user_id,
			rec_deal_id AS deal_id,
			planning_position
		FROM data_science.operational_output.selections_conversion sc
			INNER JOIN most_recent_ts mrt ON
					sc.territory_id = mrt.territory_id
				AND sc.last_modified_ts >= mrt.max_last_modified_ts
	),
	/* Aggregate the deal IDs for each user in each territory from the selections_conversion data */
	artemis AS (
		SELECT
			cvr.territory_id,
			cvr.user_id,
			cvr.deal_id,
			cvr.planning_position,
			'artemis' AS source
		FROM cte_selections_conversion cvr
	),

	/*Tail*/
	/* Get a list of deals in the tail ID -2 */
	cte_tail_deals AS (
		SELECT
			dds.territory_id,
			dds.user_id,
			dds.deal_id,
			dds.planning_position,
			'tail' AS source
		FROM data_science.operational_output.daily_deals_selections dds
		WHERE user_id = -2
		  AND planning_date = IFF(HOUR(CURRENT_TIMESTAMP) < 17, CURRENT_DATE, CURRENT_DATE + 1)
	),
	athena_users AS (
		-- get a distinct list of athena users to explode tail
		SELECT DISTINCT
			a.user_id,
			a.territory_id
		FROM athena a
	),

	cte_tail_deals_athena AS (
		-- explode out tail for athena users
		SELECT
			ctd.territory_id,
			au.user_id,
			ctd.deal_id,
			ctd.planning_position,
			ctd.source
		FROM athena_users au
			LEFT JOIN cte_tail_deals ctd ON au.territory_id = ctd.territory_id

	),
	athena_list AS (
		-- combine athena deals with tail
		SELECT *
		FROM athena a
		UNION ALL
		SELECT *
		FROM cte_tail_deals_athena
	),
	athena_dedupe AS (
		-- dedupe to remove duplication in tail
		SELECT *
		FROM athena_list al
		QUALIFY ROW_NUMBER() OVER (PARTITION BY al.user_id, al.deal_id ORDER BY al.source, al.planning_position) = 1
	),
	athena_sale_list AS (
		-- distinct list of athena recommended and tail deals
		SELECT
			ad.territory_id,
			ad.user_id,
			LISTAGG(ad.deal_id, ',') WITHIN GROUP (ORDER BY ad.source,ad.planning_position) AS athena_deals
		FROM athena_dedupe ad
		GROUP BY 1, 2
	),

	artemis_users AS (
		-- get a distinct list of artemis users to explode tail
		SELECT DISTINCT
			a.user_id,
			a.territory_id
		FROM artemis a
	),

	cte_tail_deals_artemis AS (
		-- explode out tail for artemis users
		SELECT
			ctd.territory_id,
			au.user_id,
			ctd.deal_id,
			ctd.planning_position,
			ctd.source
		FROM artemis_users au
			LEFT JOIN cte_tail_deals ctd ON au.territory_id = ctd.territory_id

	),
	artemis_list AS (
		-- combine artemis deals with tail
		SELECT *
		FROM artemis a
		UNION ALL
		SELECT *
		FROM cte_tail_deals_artemis
	),
	artemis_dedupe AS (
		-- dedupe to remove duplication in tail
		SELECT *
		FROM artemis_list al
		QUALIFY ROW_NUMBER() OVER (PARTITION BY al.user_id, al.deal_id ORDER BY al.source, al.planning_position) = 1
	),
	artemis_sale_list AS (
		-- distinct list of artemis recommended and tail deals
		SELECT
			ad.territory_id,
			ad.user_id,
			LISTAGG(ad.deal_id, ',') WITHIN GROUP (ORDER BY ad.source,ad.planning_position) AS artemis_deals
		FROM artemis_dedupe ad
		GROUP BY 1, 2
	),
	/* Get the most recent timestamp for each territory id */
	most_recent_ts_apollo AS (
		SELECT
			territory_id,
			MAX(inference_ts) AS max_inference_ts
		FROM data_science.operational_output.booking_intent_prediction
		WHERE HOUR(inference_ts) IN (5, 6)
		GROUP BY territory_id
	),
	/*Apollo*/
	/* Get the recommended model from the booking_intent_prediction table */
	apollo AS (
		SELECT
			bip.territory_id,
			bip.user_id,
			bip.recommended_model
		FROM data_science.operational_output.booking_intent_prediction bip
			INNER JOIN most_recent_ts_apollo mrta ON
					bip.territory_id = mrta.territory_id
				AND bip.inference_ts = mrta.max_inference_ts
	),
	/*AB Test Groups*/
	/* Get ab test user group table */
	ab_test AS (
		SELECT
			territory_id,
			user_id,
			segment
		FROM data_science.tmp.experiment_user_groups_gateway_api
	)
SELECT
	asl.territory_id,
	asl.user_id,
	ap.recommended_model                                                         AS apollo_recommended_model,
	ab.segment                                                                   AS ab_test_segment,
	asl.athena_deals,
	asl2.artemis_deals,
	CASE
		WHEN ap.recommended_model IS NOT DISTINCT FROM 'Artemis'
			AND ab.segment IS NOT DISTINCT FROM 'B'
			AND asl2.artemis_deals IS NOT NULL
			THEN 'Artemis'
		ELSE 'Athena'
	END                                                                          AS recommended_deal_flag,
	IFF(recommended_deal_flag = 'Artemis', asl2.artemis_deals, asl.athena_deals) AS recommended_deals
FROM athena_sale_list asl
	LEFT JOIN artemis_sale_list asl2 ON asl.user_id = asl2.user_id
	LEFT JOIN apollo ap ON asl.user_id = ap.user_id
	LEFT JOIN ab_test ab ON asl.user_id = ab.user_id
;





