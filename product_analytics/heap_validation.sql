WITH
	input_query AS (
		SELECT *
		FROM heap_main_production.heap.pageviews p
		WHERE p.path LIKE ANY ('%/sale',
							   '%/sale-offers',
							   '%/sale-hotel'
			)
		  AND p.time::DATE >= '2024-04-21'
	)
SELECT
	time::DATE AS date,
	COUNT(*)   AS sale_page_camilla
FROM input_query
GROUP BY 1
ORDER BY 1
