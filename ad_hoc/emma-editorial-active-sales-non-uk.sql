WITH
	active_sales AS (
		SELECT *
		FROM se.data.dim_sale ds
		WHERE ds.sale_active
		  AND ds.se_brand = 'SE Brand'
	)

SELECT
	active_sales.salesforce_opportunity_id,
	COUNT(DISTINCT active_sales.posa_territory)                   AS active_territory_sales_count,
	LISTAGG(DISTINCT active_sales.posa_territory, ', ')
			WITHIN GROUP ( ORDER BY active_sales.posa_territory ) AS active_territory_list,
	COUNT_IF(active_sales.posa_territory = 'UK')                  AS active_uk_sales_count
FROM active_sales
GROUP BY 1
HAVING active_uk_sales_count = 0
;


SELECT
	ds.salesforce_opportunity_id,
	COUNT(DISTINCT se_sale_id)                                                            AS sales_count,
	COUNT(DISTINCT IFF(ds.sale_active, ds.posa_territory, NULL))                          AS active_territory_sales_count,
	LISTAGG(DISTINCT IFF(ds.sale_active, ds.posa_territory, NULL), ', ')
			WITHIN GROUP ( ORDER BY IFF(ds.sale_active, ds.posa_territory, NULL) )        AS active_territory_list,
	COUNT(DISTINCT IFF(ds.posa_territory = 'UK', ds.se_sale_id, NULL))                    AS uk_sales_count,
	COUNT(DISTINCT IFF(ds.sale_active AND ds.posa_territory = 'UK', ds.se_sale_id, NULL)) AS active_uk_sales_count
FROM se.data.dim_sale ds
WHERE ds.se_brand = 'SE Brand'
GROUP BY 1
-- sale active in at least one territory
HAVING active_territory_sales_count > 0
   -- sale has no UK territory sales
   AND uk_sales_count = 0


SELECT
	ssa.salesforce_opportunity_id,
	ssa.se_sale_id,
	ssa.posa_territory,
	ssa.sale_active
FROM se.data.se_sale_attributes ssa
-- copy paste the salesforce opportunity id from the previous query to investigate the underlying sales
WHERE ssa.salesforce_opportunity_id = '0066900001HzGWj';

------------------------------------------------------------------------------------------------------------------------

SELECT
	ds.salesforce_opportunity_id,
	COUNT(DISTINCT se_sale_id)                                                            AS sales_count,
	COUNT(DISTINCT IFF(ds.sale_active, ds.posa_territory, NULL))                          AS active_territory_sales_count,
	LISTAGG(DISTINCT IFF(ds.sale_active, ds.posa_territory, NULL), ', ')
			WITHIN GROUP ( ORDER BY IFF(ds.sale_active, ds.posa_territory, NULL) )        AS active_territory_list,
	COUNT(DISTINCT IFF(ds.posa_territory = 'UK', ds.se_sale_id, NULL))                    AS uk_sales_count,
	COUNT(DISTINCT IFF(ds.sale_active AND ds.posa_territory = 'UK', ds.se_sale_id, NULL)) AS active_uk_sales_count
FROM se.data.dim_sale ds
WHERE ds.se_brand = 'SE Brand'
GROUP BY 1
-- sale active in at least one territory
HAVING active_territory_sales_count > 0
   -- sale has no UK territory sales
   AND uk_sales_count = 0
;



-- Would you be able to help me to create a snowflake report that would tell me all the HO deals that have been live
-- over the last two years (even if they're offline now) and whether or not they have any UK copy in CMS?


WITH
	live_within_2_years AS (
		SELECT DISTINCT
			ds.salesforce_opportunity_id
		FROM se.data.sale_active sa
		INNER JOIN se.data.dim_sale ds
			ON sa.se_sale_id = ds.se_sale_id
		WHERE sa.tech_platform = 'SECRET_ESCAPES'
		  AND sa.view_date >= CURRENT_DATE - INTERVAL '2 years'
		  AND ds.product_configuration = 'Hotel'
	)
SELECT
	ds.salesforce_opportunity_id,
	COUNT(DISTINCT se_sale_id)                                                            AS sales_count,
	COUNT(DISTINCT IFF(ds.sale_active, ds.posa_territory, NULL))                          AS active_territory_sales_count,
	LISTAGG(DISTINCT IFF(ds.sale_active, ds.posa_territory, NULL), ', ')
			WITHIN GROUP ( ORDER BY IFF(ds.sale_active, ds.posa_territory, NULL) )        AS active_territory_list,
	COUNT(DISTINCT IFF(ds.posa_territory = 'UK', ds.se_sale_id, NULL))                    AS uk_sales_count,
	COUNT(DISTINCT IFF(ds.sale_active AND ds.posa_territory = 'UK', ds.se_sale_id, NULL)) AS active_uk_sales_count
FROM se.data.dim_sale ds
INNER JOIN live_within_2_years lw
	ON ds.salesforce_opportunity_id = lw.salesforce_opportunity_id
WHERE ds.se_brand = 'SE Brand'
GROUP BY 1
;



SELECT
	ds.se_sale_id,
	ds.posa_territory,
	ds.salesforce_opportunity_id,
	ds.array_sale_translation
FROM se.data.dim_sale ds
WHERE ds.sale_active
  AND ds.se_brand = 'SE Brand'
  AND ds.salesforce_opportunity_id = '0061r00001DcB56'
;

WITH
	active_in_one_territory AS (
		SELECT DISTINCT
			ds.salesforce_opportunity_id
		FROM se.data.dim_sale ds
		WHERE ds.sale_active
		  AND ds.se_brand = 'SE Brand'
		  AND ds.product_configuration = 'Hotel'
	),
	sales AS (
		SELECT
			ds.se_sale_id,
			ds.posa_territory,
			ds.salesforce_opportunity_id,
			ds.array_sale_translation
		FROM se.data.dim_sale ds
		INNER JOIN active_in_one_territory a
			ON ds.salesforce_opportunity_id = a.salesforce_opportunity_id
	),
	stack_translations AS (
		SELECT
			s.salesforce_opportunity_id,
			ARRAY_UNION_AGG(array_sale_translation) AS array_sale_translation_stacked
		FROM sales s
		GROUP BY 1
	)
SELECT * FROM stack_translations

SELECT
	salesforce_opportunity_id,
	translations.value['locale']::VARCHAR                                    AS locale,
	translations.value['territory_information']['destination_name']::VARCHAR AS destination_name,
	translations.value['territory_information']['reason_to_love']::VARCHAR   AS reason_to_love,
	translations.value['territory_information']['room_description']::VARCHAR AS room_description,
	translations.value['territory_information']['slug']::VARCHAR             AS slug,
	translations.value['territory_information']['title']::VARCHAR            AS title,
	translations.value['territory_information']['url']::VARCHAR              AS url,
	translations.value['territory_information']['we_like']::VARCHAR          AS we_like,
FROM stack_translations st,
	 LATERAL FLATTEN(INPUT => array_sale_translation_stacked, OUTER => TRUE) translations
WHERE translations.value['locale']::VARCHAR = 'en_GB'
;

{
  "locale": "en_GB",
  "territory_information": {
    "destination_name": "Hotel-Residence Klosterpforte, Germany",
    "locale": "en_GB",
    "reason_to_love": "Sprawling German countryside hotel with spa facilities",
    "room_description": "room",
    "slug": "Historic property with bijou spa opposite an 800-year-old monastery in Germany's Westphalia region - includes breakfast, one optional dinner and more",
    "title": "Historic East Westphalia monastery stay  ",
    "url": "historic-east-westphalia-monastery-stay-fully-refundable-hotel-residence-klosterpforte-germany",
    "we_like": "The sleepy village of Marienfeld sits in the middle of East Westphalia, part of Germany’s North Rhine-Westphalia region, which is home to cities like Cologne, Münster and Dortmund. Your location is beautifully rural, with rolling fields and forests peppering the landscape, so this is a great place to get a bit of R&R. <div><br /></div><div>Spend your days here taking leisurely cycles along the bike paths or whizzing across the golf greens in a buggy. There are also plenty of hiking trails and historic sights, from churches and monasteries to castles and palaces. Roughly an hour from the hotel is the Teutoberg Forest, definitely worth a day trip for its beautiful landscapes.</div>"
  }
};



