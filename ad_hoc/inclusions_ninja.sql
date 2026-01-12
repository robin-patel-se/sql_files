SELECT *
FROM data_vault_mvp.dwh.offer_inclusion oil
;

-- create a payload
-- model to offer level
-- shortlist best inclusion
-- translate to right locale
-- share what structured data from salesforce

SELECT *
FROM se.data.dim_sale ds
WHERE ds.se_sale_id = 'A60882'
;

SELECT *
FROM se.data.se_sale_attributes ssa
WHERE ssa.se_sale_id = 'A60882'
;

SELECT *
FROM latest_vault.cms_mysql.hotel_sale_offer hso
WHERE hso.sale_id = 'A60882'
;

-- offers on the sale
OFFER_ID
33204
36728
30160


SELECT *
FROM data_vault_mvp.dwh.offer_inclusion oil
WHERE oil.se_offer_id IN (
						  'A33204',
						  'A36728',
						  'A30160'
	)
;


SELECT *
FROM se.data.se_sale_attributes ssa
INNER JOIN latest_vault.cms_mysql.hotel_sale_offer hso
	ON ssa.se_sale_id = hso.sale_id
INNER JOIN se.data.se_offer_attributes soa
	ON hso.offer_id = soa.offer_id
WHERE ssa.sale_active
  AND ssa.posa_territory = 'DE'
;


SELECT *
FROM latest_vault.cms_mysql.hotel_sale_offer hso
WHERE hso.sale_id = 'A20764'
;



SELECT *
FROM data_vault_mvp.dwh.offer_inclusion oil
WHERE oil.se_offer_id IN (
						  'A16572',
						  'A16573',
						  'A22587',
						  'A22586',
						  'A16569'
	)
;

------------------------------------------------------------------------------------------------------------------------


SELECT *
FROM se.data.dim_sale ds
INNER JOIN latest_vault.cms_mysql.hotel_sale_offer hso
	ON ds.se_sale_id = hso.sale_id
INNER JOIN data_vault_mvp.dwh.offer_inclusion oi
	ON 'A' || hso.offer_id = oi.se_offer_id
WHERE ds.sale_active
-- AND ds.tech_platform = 'TRAVELBIRD';

SELECT *
FROM latest_vault.cms_mysql.hotel_sale_offer hso
;

SELECT *
FROM data_vault_mvp.dwh.se_offers_inclusions_rates soir
;

WITH
	tb_sf_opps AS (
		SELECT DISTINCT
			ds.salesforce_opportunity_id
		FROM se.data.dim_sale ds
	AND ds.sale_active
	)

SELECT *
FROM data_vault_mvp.dwh.se_offers_inclusions_rates soir
INNER JOIN tb_sf_opps
	ON soir.salesforce_opportunity_id = tb_sf_opps.salesforce_opportunity_id


------------------------------------------------------------------------------------------------------------------------

WITH
	offers_for_active_sales AS (
		-- create a list of active offers
		SELECT
			se_sale.se_sale_id,
			se_offer.se_offer_id,
			se_sale.base_currency,
			territory.locale AS sale_locale
		FROM se.data.se_offer_attributes se_offer
		INNER JOIN latest_vault.cms_mysql.hotel_sale_offer
			ON se_offer.se_offer_id = 'A' || hotel_sale_offer.offer_id
		INNER JOIN se.data.se_sale_attributes se_sale
			ON hotel_sale_offer.sale_id = se_sale.se_sale_id
			AND se_sale.sale_active
		INNER JOIN latest_vault.cms_mysql.territory territory
			ON se_sale.posa_territory = territory.name
		WHERE se_offer.offer_active
	)
		,
	model_inclusions AS (
		-- model inclusions data for active offers
		SELECT
			offers.se_sale_id,
			offers.se_offer_id,
			offers.base_currency,
			offers.sale_locale,
			offer_inclusions.salesforce_opportunity_id,
			offer_inclusions.account_id,
			offer_inclusions.inclusion_id,
			offer_inclusions.salesforce_offer_id,
			offer_inclusions.offer_name,
			offer_inclusions.board_basis,
			offer_inclusions.inclusion_name,
			offer_inclusions.currencyisocode,
			offer_inclusions.inclusion_type__c                       AS inclusion_type,
			offer_inclusions.inclusion_rate__c                       AS inclusion_rate,
			offer_inclusions.inclusion_level__c                      AS inclusion_level,
			offer_inclusions.active_inclusion__c                     AS active_inclusion,
			offer_inclusions.inclusion_value_local,
			-- inclusions and their values are stored in various permutations, need to
			-- model value to a comparative amount
			CASE
				-- for per person inclusions multiply it by the calculated number of people
				WHEN
					offer_inclusions.inclusion_rate__c IN
					('Per Person, First Night',
					 'Per Person, Per Day',
					 'Per Person, Per Stay')
					THEN offer_inclusions.inclusion_value_local * 2
				WHEN
					offer_inclusions.inclusion_rate__c IN
					('Per Room, Per Day',
					 'Per Room, Per Stay',
					 'Per Room, First Night')
					THEN offer_inclusions.inclusion_value_local
			END                                                      AS inclusion_value_local_calculated,
			COALESCE(fx_rates.fx_rate, 1)                            AS local_rate_to_sale_rate,
			IFF(offer_inclusions.currencyisocode = offers.base_currency, inclusion_value_local_calculated,
				inclusion_value_local_calculated * fx_rates.fx_rate) AS inclusion_value_sale_currency,
			offer_inclusions.local_rate_to_gbp,
			offer_inclusions.inclusion_value_gbp
		FROM offers_for_active_sales offers
		INNER JOIN data_vault_mvp.dwh.offer_inclusion offer_inclusions
			ON offers.se_offer_id = offer_inclusions.se_offer_id
			-- Convert local inclusion values to sale currency (at the time of writing this there's
			-- no evidence of them being different. We've had multiple instances in the past where
			-- this doesn't always remain the case.
		LEFT JOIN data_vault_mvp.fx.rates fx_rates
			ON offer_inclusions.currencyisocode = fx_rates.source_currency
			AND fx_rates.target_currency = offers.base_currency
			AND fx_rates.fx_date = CURRENT_DATE()
-- 		WHERE offers.se_sale_id = 'A60882' -- TODO REMOVE -- example uk
-- 		WHERE offers.se_sale_id = 'A16254' -- TODO REMOVE -- example de

	),
	inclusion_filters_offer_level AS (
		-- filtering out no value inclusions
		-- choosing top 5 inclusions for each offer
		SELECT *,
			   ROW_NUMBER() OVER (PARTITION BY model_inclusions.se_sale_id, model_inclusions.se_offer_id ORDER BY model_inclusions.inclusion_value_local DESC) AS inclusion_index
		FROM model_inclusions
		WHERE -- filter inclusions that we do not want to show
			  (
				  -- only show inclusions with a moneytary value
				  model_inclusions.inclusion_value_local > 0
				  )
		-- show top 5 inclusions based on value
		QUALIFY inclusion_index <= 5
	)
		,
	offer_inclusions_aggregation AS (
		-- aggregate inclusion value up to offer level to decipher best offer
		SELECT
			inclusion_filters_offer_level.se_sale_id,
			inclusion_filters_offer_level.se_offer_id,
			SUM(inclusion_filters_offer_level.inclusion_value_local) AS inclusion_value_local,
			COUNT(*)                                                 AS inclusions
		FROM inclusion_filters_offer_level
		GROUP BY inclusion_filters_offer_level.se_sale_id,
				 inclusion_filters_offer_level.se_offer_id
	)
		,
	calculate_best_offer AS (
		-- calculate the best offer associated according to the total inclusion value on a offer
		SELECT *
		FROM offer_inclusions_aggregation
		QUALIFY
			ROW_NUMBER() OVER (PARTITION BY offer_inclusions_aggregation.se_sale_id ORDER BY offer_inclusions_aggregation.inclusion_value_local DESC, offer_inclusions_aggregation.inclusions DESC, offer_inclusions_aggregation.se_sale_id DESC) =
			1
	),
	sale_inclusions AS (
		SELECT
			inclusions.se_sale_id,
			inclusions.se_offer_id,
			inclusions.base_currency,
			inclusions.sale_locale,
			-- necessary to use with snowflake cortex
			-- note that cortex translate only accommodates for set languages:
			-- https://docs.snowflake.com/en/sql-reference/functions/translate-snowflake-cortex#usage-notes
			DECODE(inclusions.sale_locale,
				   'en_GB', 'en',
				   'en_US', 'en',
				   'de_DE', 'de',
				   'it_IT', 'it',
				   'pl_PL', 'pl',
				   'nl_NL', 'nl',
				   'fr_FR', 'fr',
				   'es_ES', 'es',
				   'sv', 'sv',
				   'en' -- all other languages leave as english, you cannot pass a null translation language to cortex translate
			)                              AS sale_translation_language,
			inclusions.salesforce_opportunity_id,
			inclusions.account_id,
			inclusions.inclusion_id,
			inclusions.salesforce_offer_id,
			inclusions.offer_name,
			inclusions.board_basis,
			inclusions.inclusion_name,
			IFF(sale_translation_language IS NOT NULL,
				snowflake.cortex.translate(
						inclusions.inclusion_name,
						'en', -- all written in english
						sale_translation_language -- translate to language
				),
				inclusions.inclusion_name) AS inclusion_name_translated,
			inclusions.currencyisocode,
			inclusions.inclusion_type,
			inclusions.inclusion_rate,
			inclusions.inclusion_level,
			inclusions.active_inclusion,
			inclusions.inclusion_value_local,
			inclusions.local_rate_to_sale_rate,
			inclusions.inclusion_value_sale_currency,
			inclusions.local_rate_to_gbp,
			inclusions.inclusion_value_gbp,
			inclusions.inclusion_index,
			OBJECT_CONSTRUCT(
					'inclusionName', inclusions.inclusion_name,
					'inclusionNameTranslated', inclusion_name_translated,
					'inclusionType', inclusions.inclusion_type,
					'inclusionCurrency', inclusions.currencyisocode,
					'inclusionValueLocal', inclusions.inclusion_value_local,
					'inclusionValueLocalCalculated', inclusion_value_local_calculated,
					'inclusionValueGBP', inclusions.inclusion_value_gbp,
					'inclusionValueSaleCurrency', inclusion_value_sale_currency,
					'inclusionLevel', inclusions.inclusion_level,
					'inclusionRate', inclusions.inclusion_rate,
					'inclusionIndex', inclusions.inclusion_index
			)                              AS inclusion_object
		FROM inclusion_filters_offer_level inclusions
			-- limit inclusions to only ones related to the best offer associated to a sale
		INNER JOIN calculate_best_offer
			ON inclusions.se_sale_id = calculate_best_offer.se_sale_id
			AND inclusions.se_offer_id = calculate_best_offer.se_offer_id

	)

SELECT
	sale_inclusions.se_sale_id,
	sale_inclusions.se_offer_id,
	SUM(inclusion_value_sale_currency)                                AS total_inclusion_value_sale_currency,
	LISTAGG(sale_inclusions.inclusion_name_translated, '\n')
			WITHIN GROUP (ORDER BY sale_inclusions.inclusion_index)   AS inclusions_list,
	ARRAY_AGG(sale_inclusions.inclusion_object)
			  WITHIN GROUP (ORDER BY sale_inclusions.inclusion_index) AS inclusions_array
FROM sale_inclusions
GROUP BY sale_inclusions.se_sale_id,
		 sale_inclusions.se_offer_id



-- example of job we send to kingfisher
/*SELECT *
FROM se.data_science.data_science_sale_clustering*/



SELECT
	name,
	t.locale,
	DECODE(t.locale,
		   'en_GB', 'en',
		   'en_US', 'en',
		   'de_DE', 'de',
		   'it_IT', 'it',
		   'pl_PL', 'pl',
		   'nl_NL', 'nl',
		   'fr_FR', 'fr',
		   'es_ES', 'es',
		   'sv', 'sv'
	) AS sale_translation_language,
FROM latest_vault.cms_mysql.territory t

