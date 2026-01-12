SELECT 
	page_urlpath AS landing_page,
	--The number of unique sessions which visited a given landing page
	COUNT(DISTINCT global_session_id) AS session_visits,
	--The total number of visits to a given landing page
	COUNT(global_session_id) AS total_visits
FROM sami_sturdy.sessionised_events_v
--Filtering only for landing pages which have URLs ending in 'sale', 'sale-hotel', etc. 
WHERE page_urlpath LIKE '%/sale%'
--Filtering out generic URLs which contain 'sale' such as '/sale/currentSales'
AND page_urlpath NOT LIKE '%/sale/%'
GROUP BY page_urlpath
ORDER BY session_visits DESC
--Limited to only show the top 10 landing pages
LIMIT 10
