SELECT
	sae.contexts_com_secretescapes_user_state_context_1[0]['navigation_type'],
	COUNT(*)
FROM snowplow.atomic.events sae
WHERE sae.collector_tstamp >= '2024-08-23 17:05:25' -- when cs tracking went live
  AND sae.event_name = 'page_view'
  AND sae.v_tracker LIKE 'js%'
GROUP BY 1
;


SELECT
	ses.contexts_com_secretescapes_user_state_context_1[0]['navigation_type']::VARCHAR AS navigation_type,
	*
FROM se.data_pii.scv_event_stream ses
WHERE ses.collector_tstamp >= CURRENT_DATE
  AND ses.event_name = 'page_view'
  AND ses.v_tracker LIKE 'js%'
;


/*			CASE
				WHEN ses.page_urlpath LIKE '%/sale-hotel' -- client side
-- 			OR (ses.page_urlpath LIKE '%/sale-offers' AND
-- 				ses.device_platform = 'web')
					OR ses.page_urlpath LIKE '%/sale' -- client side
					OR ses.page_urlpath LIKE '%/offerta'
					OR ses.page_urlpath LIKE '%/aanbiedingen'
					OR ses.page_url REGEXP
					   '.*\\/(sales.travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. sales.travelbird.de
					OR ses.page_url REGEXP
					   '.*\\/(sales.([a-z,A-Z]{2}).travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. sales.fr.travelbird.be
					OR ses.page_url REGEXP
					   '.*\\/([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. de.sales.secretescapes.com
					OR ses.page_url REGEXP
					   '.*\\/([a-z,A-Z]{2}\\.)([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. co.uk.sales.secretescapes.com
					OR ses.page_url REGEXP
					   '.*\\/\\/(secretescapes|secretescapesnl)\\.journaway\\.(com|de|nl)\\/[a-z,A-Z]{2}\\/(angebote|angebot|aanbiedingen)\\/.*' -- e.g. secretescapes.journaway.com/de/angebote
					OR ses.page_url REGEXP
					   '.*\\/\\/(secretescapes|secretescapesnl)\\.neon-reisen\\.(com|de|nl)\\/(angebote|angebot|aanbiedingen)\\/.*' -- e.g. secretescapes.neon-reisen.de/angebot
					THEN TRUE
				ELSE FALSE
			END                      AS sale_page_url_classification,
     */
*/


WITH
	input_data AS (
		SELECT
			ses.contexts_com_secretescapes_user_state_context_1[0]['navigation_type']::VARCHAR AS navigation_type,
			*
		FROM se.data_pii.scv_event_stream ses
		WHERE ses.collector_tstamp >= '2024-08-27 16:00:00'
		  AND ses.event_name = 'page_view'
		  AND ses.v_tracker LIKE 'js%'
			AND ses.se_brand = 'SE Brand'

------ sale_page_definition
		  AND (ses.page_urlpath LIKE '%/sale-hotel' -- client side
			OR ses.page_urlpath LIKE '%/sale' -- client side
			OR ses.page_urlpath LIKE '%/offerta'
			OR ses.page_urlpath LIKE '%/aanbiedingen'
			OR ses.page_url REGEXP
			   '.*\\/(sales.travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. sales.travelbird.de
			OR ses.page_url REGEXP
			   '.*\\/(sales.([a-z,A-Z]{2}).travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. sales.fr.travelbird.be
			OR ses.page_url REGEXP
			   '.*\\/([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. de.sales.secretescapes.com
			OR ses.page_url REGEXP
			   '.*\\/([a-z,A-Z]{2}\\.)([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. co.uk.sales.secretescapes.com
			OR ses.page_url REGEXP
			   '.*\\/\\/(secretescapes|secretescapesnl)\\.journaway\\.(com|de|nl)\\/[a-z,A-Z]{2}\\/(angebote|angebot|aanbiedingen)\\/.*' -- e.g. secretescapes.journaway.com/de/angebote
			OR ses.page_url REGEXP
			   '.*\\/\\/(secretescapes|secretescapesnl)\\.neon-reisen\\.(com|de|nl)\\/(angebote|angebot|aanbiedingen)\\/.*' -- e.g. secretescapes.neon-reisen.de/angebot
			)
	)
SELECT
	ind.navigation_type,
	COUNT(*)
FROM input_data ind
GROUP BY 1