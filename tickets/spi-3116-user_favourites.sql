WITH
	user_wish_list AS (
		SELECT
			wish_list.user_id                                                   AS shiro_user_id,

			-- keywords can be duplicated from the source
			-- de-duplicate by taking max last_updated and remove case and delimiters from the keyword
			REPLACE(REPLACE(LOWER(wish_list_item.keyword), ' ', ''), ',', '')   AS wish_list_item_keyword,

			-- the format explicitly requested
			MAX(TO_VARCHAR(wish_list_item.last_updated, 'YYYY-MM-DD HH:MI:SS')) AS wish_list_item_last_revised,

			country_helper.country_id                                           AS country_id,
			city_helper.city_id                                                 AS city_id
		FROM latest_vault.cms_mysql.wish_list wish_list
			LEFT JOIN latest_vault.cms_mysql.wish_list_item wish_list_item
					  ON wish_list.id = wish_list_item.wish_list_id
			LEFT JOIN data_vault_mvp.dwh.iterable__user_profile_activity__step15__locale_country_lookup_helper AS country_helper
					  ON country_helper.country_name =
						 COALESCE(LOWER(REPLACE(REPLACE(SPLIT(wish_list_item.keyword, ',')[1], '"', ''), ' ', '')),
								  LOWER(REPLACE(REPLACE(SPLIT(wish_list_item.keyword, ',')[0], '"', ''), ' ', '')))
			LEFT JOIN data_vault_mvp.dwh.iterable__user_profile_activity__step16__locale_city_lookup_helper AS city_helper
					  ON city_helper.city_name =
						 LOWER(REPLACE(REPLACE(SPLIT(wish_list_item.keyword, ',')[0], '"', ''), ' ', ''))
		WHERE wish_list_item.keyword IS NOT NULL
		  AND COALESCE(wish_list.user_id, '-1')
			-- explicitly, exclude anomalous users as a number of these have very large wishlists lists
			NOT IN (
				  SELECT
					  COALESCE(attributed_user_id, '-1') AS attributed_user_id
				  FROM data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
				  WHERE stitched_identity_type = 'se_user_id'
					AND TRY_TO_NUMBER(attributed_user_id) IS NOT NULL
				  GROUP BY COALESCE(attributed_user_id, '-1')
			  )
		GROUP BY wish_list.user_id,
				 REPLACE(REPLACE(LOWER(wish_list_item.keyword), ' ', ''), ',', ''),
				 country_helper.country_id,
				 city_helper.city_id
	)
SELECT
	shiro_user_id,
	ARRAY_AGG(OBJECT_CONSTRUCT(
				  -- we pin back the keyword in its original form here
					  'wishListKeyword', wish_list_item.keyword,
				  -- previously, this attributed-value pair was called: "lastUpdated" however, in Iterable once a field has
				  -- ..been created, it's not possible to change the underlying datatype (which in this instance would be string->to->datetime)
				  -- ..for this reason, field: "lastRevised" has been introduced and replaces field: "lastUpdated"
					  'lastRevised', wish_list_item_last_revised,
					  'countryId', country_id,
					  'cityId', city_id
			  )) WITHIN GROUP (
				  ORDER BY
				  wish_list_item_last_revised
				  DESC) AS user_wishlist_array
FROM user_wish_list AS modelled_wish_list
	INNER JOIN latest_vault.cms_mysql.wish_list wish_list
			   ON modelled_wish_list.shiro_user_id = wish_list.user_id
	INNER JOIN latest_vault.cms_mysql.wish_list_item wish_list_item
			   ON wish_list.id = wish_list_item.wish_list_id
				   AND modelled_wish_list.wish_list_item_last_revised = wish_list_item.last_updated
				   AND modelled_wish_list.wish_list_item_keyword =
					   REPLACE(REPLACE(LOWER(wish_list_item.keyword), ' ', ''), ',', '')
GROUP BY shiro_user_id



SELECT
	wish_list.user_id,
	wish_list_item.wish_list_id,
	wish_list.date_created      AS wish_list_created,
	wish_list_item.id           AS wish_list_item_id,
	wish_list_item.keyword,
	wish_list_item.date_created AS wish_list_item_ceated,
	wish_list_item.last_updated
FROM latest_vault.cms_mysql.wish_list wish_list
	LEFT JOIN latest_vault.cms_mysql.wish_list_item wish_list_item
			  ON wish_list.id = wish_list_item.wish_list_id
WHERE wish_list.user_id = 10527537
;


------------------------------------------------------------------------------------------------------------------------

WITH
	user_favorites AS (
		SELECT
			favorite.user_id                                AS shiro_user_id,
			TO_VARCHAR(favorite.last_updated,
					   'YYYY-MM-DD HH24:MI:SS +00:00')      AS favorite_last_amended,
			IFF(favorite.last_updated::DATE >= CURRENT_DATE() - 1,
				TRUE, FALSE)                                AS is_past_day,

			-- check this with the team
			COALESCE(favorite.sale_id::VARCHAR,
					 'A' || favorite.base_sale_id::VARCHAR) AS se_sale_id
		FROM latest_vault.cms_mysql.favorite favorite
			INNER JOIN data_vault_mvp.dwh.dim_sale ds

						   -- check this with the team
					   ON ds.se_sale_id =
						  COALESCE('A' || favorite.base_sale_id::VARCHAR, favorite.sale_id::VARCHAR)

		WHERE ds.sale_active = TRUE
		  AND LEFT(ds.se_sale_id, 3) IS DISTINCT FROM 'TVL' --remove travelist spvs
		  AND COALESCE(favorite.user_id, '-1')
			-- explicitly, exclude anomalous users as a number of these have very large favorite lists
			NOT IN (
				  SELECT
					  COALESCE(attributed_user_id, '-1') AS attributed_user_id
				  FROM data_vault_mvp.single_customer_view_stg.module_anomalous_user_dates
				  WHERE stitched_identity_type = 'se_user_id'
					AND TRY_TO_NUMBER(attributed_user_id) IS NOT NULL
				  GROUP BY COALESCE(attributed_user_id, '-1')
			  )
	)
SELECT
	shiro_user_id,
	ARRAY_AGG(OBJECT_CONSTRUCT(
					  'saleId', se_sale_id,
					  'pastDay', is_past_day,
					  'lastAmended', favorite_last_amended
			  )) WITHIN GROUP (ORDER BY favorite_last_amended DESC) AS user_favorites_array
FROM user_favorites
GROUP BY shiro_user_id
;



SELECT
	favorite.user_id                                AS shiro_user_id,
	TO_VARCHAR(favorite.last_updated,
			   'YYYY-MM-DD HH24:MI:SS +00:00')      AS favorite_last_amended,
	IFF(favorite.last_updated::DATE >= CURRENT_DATE() - 1,
		TRUE, FALSE)                                AS is_past_day,

	-- check this with the team
	COALESCE(favorite.sale_id::VARCHAR,
			 'A' || favorite.base_sale_id::VARCHAR) AS se_sale_id
FROM latest_vault.cms_mysql.favorite favorite
	INNER JOIN data_vault_mvp.dwh.dim_sale ds

				   -- check this with the team
			   ON ds.se_sale_id =
				  COALESCE('A' || favorite.base_sale_id::VARCHAR, favorite.sale_id::VARCHAR)

WHERE ds.sale_active = TRUE
  AND LEFT(ds.se_sale_id, 3) IS DISTINCT FROM 'TVL' --remove travelist spvs
  AND COALESCE(favorite.user_id, '-1')
	AND shiro_user_id = 10527537
