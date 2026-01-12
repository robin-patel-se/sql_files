SELECT
	hscv.posu_cluster            AS cluster,
	hscv.posu_cluster_region     AS region,
	hscv.posu_cluster_sub_region AS subregion,
	hocv.view_date               AS view_date,
	kdd.start_date               AS key_date_start,
	kdd.end_date                 AS key_date_end,
	ROUND(AVG(hocv.total_rate_gbp), 2) AVERAGE_RATE_KEY_PERIOD
FROM se.bi.harmonised_offer_calendar_view_snapshot hocv
	LEFT JOIN se.bi.harmonised_sale_calendar_view_snapshot hscv
			  ON hocv.salesforce_opportunity_id = hscv.salesforce_opportunity_id
	LEFT JOIN latest_vault.cro_gsheets.key_dates_definition kdd
			  ON CONCAT(hscv.posu_cluster, hscv.posu_cluster_region, hscv.posu_cluster_sub_region,
						MONTHNAME(hscv.view_date)) =
				 CONCAT(kdd.cluster, kdd.cluster_region, kdd.cluster_sub_region, MONTHNAME(kdd.ref_date))

WHERE hocv.view_date IN ('2023-06-06', '2023-06-07')
  AND kdd.ref_date = DATE_TRUNC('MONTH', CURRENT_DATE())
  AND hocv.calendar_date >= CURRENT_DATE()
  AND hocv.calendar_date <= kdd.end_date
  AND hscv.posu_cluster_region = 'UK'
GROUP BY 1, 2, 3, 4, 5, 6