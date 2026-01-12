------------------------------------------------------------------------------------------------------------------------
SELECT
	ses.page_url,
	PARSE_URL(ses.page_url, 1)           AS parsed_url,
	parsed_url['path']::VARCHAR          AS page_urlpath2,
	IFF(parsed_url IS NULL, FALSE, TRUE) AS is_valid_url,
	CASE

		WHEN
			page_urlpath2 LIKE '%current-sales%'
				OR page_urlpath2 LIKE '%aktuelle-angebote%'
				OR page_urlpath2 LIKE '%currentSales'
				OR page_urlpath2 LIKE 'aanbiedingen' -- NL
				OR page_urlpath2 LIKE '%offerte-in-corso%' -- IT
				OR page_urlpath2 LIKE '%nuvaerende-salg%'
				OR page_urlpath2 LIKE '%aktuella-kampanjer%'
				OR page_urlpath2 IN ('/', '')
			THEN 'home page'

		WHEN
			page_urlpath2 LIKE '%/sale-hotel' -- client side
				OR page_urlpath2 LIKE '%/sale' -- client side
				OR page_urlpath2 LIKE '%/offerta'
				OR page_urlpath2 LIKE '%/aanbiedingen'
				OR page_url REGEXP
				   '.*\\/(sales.travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. sales.travelbird.de
				OR page_url REGEXP
				   '.*\\/(sales.([a-z,A-Z]{2}).travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. sales.fr.travelbird.be
				OR page_url REGEXP
				   '.*\\/([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. de.sales.secretescapes.com
				OR page_url REGEXP
				   '.*\\/([a-z,A-Z]{2}\\.)([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. co.uk.sales.secretescapes.com
				OR page_url REGEXP
				   '.*\\/\\/(secretescapes|secretescapesnl)\\.journaway\\.(com|de|nl)\\/[a-z,A-Z]{2}\\/(angebote|angebot|aanbiedingen)\\/.*' -- e.g. secretescapes.journaway.com/de/angebote
				OR page_url REGEXP
				   '.*\\/\\/(secretescapes|secretescapesnl)\\.neon-reisen\\.(com|de|nl)\\/(angebote|angebot|aanbiedingen)\\/.*' -- e.g. secretescapes.neon-reisen.de/angebot
			THEN 'sale page'
		WHEN
			page_urlpath2 LIKE 'instant-access/%' -- added 27-03
				OR page_urlpath2 = 'login'
			THEN 'instant access'
		WHEN
			page_urlpath2 IN ('/bookings',
							  'bookings',
							  'boekingen',
							  'bokningar',
							  'prenotazioni'
				) -- added 27-03
				OR page_urlpath2 IN ('/buchungen', 'buchungen') -- added 27-03
			THEN 'my bookings'
		WHEN
			page_urlpath2 LIKE '%/filter%' -- updated 28-02
				OR page_urlpath2 LIKE '%/filtra%' -- updated 28-02
			THEN 'filter_page'
		WHEN
			page_urlpath2 LIKE '%/checkout'
				OR page_urlpath2 LIKE 'sale/book-hotel%' -- HO ndm
				OR page_urlpath2 LIKE '/sale/book' -- added in 29-03
				OR page_urlpath2 LIKE '/api/graphql/' -- added in 29-03
			THEN 'booking_form'
		WHEN
			page_urlpath2 LIKE '%/rooms'
			THEN 'booking_flow_room_selection'
		WHEN
			page_urlpath2 LIKE '%/flights'
			THEN 'booking_flow_flight_selection'
		WHEN
			page_urlpath2 LIKE '%/dates'
			THEN 'booking_flow_date_selection'
		WHEN
			page_urlpath2 LIKE '%/accommodation'
			THEN 'booking_flow_accommodation_selection'
		WHEN
			page_urlpath2 LIKE '%/roundtrip' -- to check with Mehmet what this is
			THEN 'booking_flow_roundtrip_selection'
		WHEN
			page_urlpath2 LIKE '%/extras'
			THEN 'booking_flow_extras_selection'
		WHEN
			page_urlpath2 LIKE '%/tickets' -- the step to choose activities/leisures/experiences
			THEN 'booking_flow_tickets_selection'
		WHEN
			page_urlpath2 LIKE '%/cars'
			THEN 'booking_flow_cars_selection'
		WHEN
			page_urlpath2 LIKE '%/insurance'
			THEN 'booking_flow_insurance_selection'

		WHEN
			page_urlpath2 LIKE '%search/search%'
				OR page_urlpath2 LIKE '%mbSearch/mbSearch'
			THEN 'search results'
		WHEN
			page_urlpath2 LIKE '%/sale-offers' -- client side
			THEN 'offer page'
		WHEN
			page_urlpath2 LIKE '%/ueber-uns%' -- updated 01-03
				OR page_urlpath2 LIKE '%/about-us%' -- updated 01-03
			THEN 'about us'
		WHEN
			page_urlpath2 LIKE ANY ('contact', '%/contact%') -- updated 28-02
				OR page_urlpath2 LIKE '%/kontakt%' -- updated 28-02
			THEN 'contact'
		WHEN
			page_urlpath2 LIKE '%forgottenpassword%'
				OR page_urlpath2 LIKE '%/passwortvergessen%'
			THEN 'forgotten password'
		WHEN
			page_urlpath2 LIKE '%/accounts/%' -- TB
				OR page_urlpath2 LIKE '%your-account%'
				OR page_urlpath2 LIKE '%konto%'
				OR page_urlpath2 = 'votre-compte'
			THEN 'your account'
		WHEN
			page_urlpath2 LIKE ANY ('credits', '/credits%') -- updated 28-02
				OR page_urlpath2 LIKE ANY ('/guthaben%', 'guthaben') -- updated 28-02
			THEN 'credits'
		WHEN
			page_urlpath2 = 'faq' -- updated 28-02
				OR page_urlpath2 LIKE '%mobile-faq%' -- updated 28-02
			THEN 'faq'
		WHEN
			page_urlpath2 LIKE '%/my-favourites%' -- updated 28-02
				OR page_urlpath2 LIKE '%meine-favoriten' -- updated 28-02
			THEN 'my favourites'
		WHEN
			page_urlpath2 LIKE '%freunde-einladen'
				OR page_urlpath2 LIKE '%invite_friends'
			THEN 'invite friends'
		WHEN
			page_urlpath2 LIKE '%payment/directSuccess/%'
			THEN 'booking confirmation'
		WHEN
			page_urlpath2 LIKE '%/zahlungsinformationen/%'
			THEN 'payment information'
		WHEN
			page_urlpath2 LIKE '/payment/altPaymentFailure/%'
			THEN 'payment failure page'
		WHEN
			page_urlpath2 LIKE '%/media%'
			THEN 'media'
		WHEN
			page_url LIKE '%mp.secretescapes%'
			THEN 'competition'
		WHEN
			page_urlpath2 LIKE '/privacy-policy%' -- updated 28-02
				OR page_urlpath2 LIKE '/mobile-privacy-policy% ' -- updated 28-02
				OR page_url LIKE '%datenschutzerklaerung' -- updated 01-03
			THEN 'privacy policy'
		WHEN
			page_urlpath2 LIKE '%/magazine-de/%'
			THEN 'se_magazine'
		WHEN
			page_urlpath2 LIKE '/your-subscriptions%'
				OR page_urlpath2 LIKE '/ihre-abonnemente%' -- updated 01-03
				OR page_urlpath2 IN ('uw-abonnementen', 'vos-abonnements', 'tue-iscrizioni', 'nastaveni-oznameni')
			THEN 'subscriptions'
		WHEN
			page_urlpath2 LIKE '%terms-and-conditions%'
				OR page_urlpath2 LIKE '%/agb'
			THEN 'terms and conditions'
		WHEN
			page_urlpath2 LIKE ANY ('%/voucher%', 'voucher/buyVoucher')
				OR page_urlpath2 LIKE '%/geschenkgutscheine%'
				OR page_urlpath2 = 'vouchers-offer'
			THEN 'vouchers'
		WHEN
			page_urlpath2 LIKE '%work-with-us'
				OR page_urlpath2 LIKE '%workWithUs'
				OR page_urlpath2 LIKE '%/addyourhotelprivacy-policy%'
			THEN 'work with us'
		WHEN
			page_urlpath2 LIKE '/reminders%' -- added 01-03
				OR page_urlpath2 LIKE '/erinnerungen%' -- added 01-03
			THEN 'reminders'
		WHEN
			page_urlpath2 LIKE '%/holds%' -- added 01-03
				OR page_urlpath2 LIKE '%/reservierungen%' -- added 01-03
				OR page_urlpath2 = 'reservationer'
			THEN 'holds'
		WHEN
			page_urlpath2 LIKE '%/territory%' -- added 01-03
			THEN 'territory'
		WHEN
			page_urlpath2 LIKE '/hotelSale/calendar'
				OR page_urlpath2 LIKE '/sale/allocationsByDate'
				OR page_urlpath2 LIKE '%/sale-calendar'
			THEN 'calendar'
		WHEN page_urlpath2 REGEXP
			 '^booking\\/\\d+\\/[^/]+\\/v3-\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}-\\d{13}-\\w{8}(\\/|\\/null)?$'
			THEN 'plan your trip'
		WHEN page_urlpath2 LIKE '%sale-preferences' THEN 'sale preferences'
		WHEN page_urlpath2 LIKE '%your-subscriptions' THEN 'your subscriptions'
		WHEN
			is_valid_url = FALSE THEN 'broken_url'
-- 		ELSE 'other'
	END                                  AS page_classification
FROM se.data_pii.scv_event_stream ses
WHERE ses.event_tstamp >= CURRENT_DATE
  AND ses.page_url IS NOT NULL
--   AND page_classification IS NULL
  AND ses.se_brand = 'SE Brand'
  AND ses.event_name IN ('page_view', 'screen_view')
;


SELECT
	stbfv.page_url,
	PARSE_URL(stbfv.page_url, 1)['path']::VARCHAR                                                              AS page_urlpath,
	page_urlpath REGEXP '^booking\\/\\d+\\/[^/]+\\/v3-\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}-\\d{13}-\\w{8}\\/?$' AS is_bfv
FROM se.data.scv_touched_booking_form_views stbfv
WHERE stbfv.event_tstamp >= CURRENT_DATE - 1
  AND is_bfv = FALSE
;

SELECT
	'booking/116329/das-naturparadies-chalkidiki-de/v3-36764217-366a-4adc-a78f-1584ac3ff02b-1725954880241-N7YDFCG7/null' AS page_urlpath,
	page_urlpath REGEXP
	'^booking\\/\\d+\\/[^/]+\\/v3-\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}-\\d{13}-\\w{8}(\\/|\\/null)?$'                     AS is_bfv,


------------------------------------------------------------------------------------------------------------------------


SET page_url = 'https://www.secretescapes.com/'

SET page_url = 'https://co.uk.sales.secretescapes.com/119427/cruise-mardi-gras-new-orleans-ai-caribbean-cruise/?urlSlug=epic-new-orleans-mardi-gras-and-all-inclusive-caribbean-cruise-flexible-holiday-usa-mexico-honduras-and-belize-uk'

SET page_url = 'https://www.secretescapes.de/auszeit-and-tradition-in-den-bayerischen-bergen-golf-and-alpin-wellness-resort-hotel-ludwig-royal-oberstaufen-steibis-allgaeu-bayern-deutschland/sale-hotel'

SET page_url = 'https://de.sales.secretescapes.com/118250/glass-resort-2022-2023-rovaniemi-finland-de/?noPasswordSignIn=true&utm_medium=email&utm_source=newsletter&utm_campaign=7179936&utm_platform=ITERABLE&utm_content=SEGMENT_CORE_DE_ACT_01M&copyVersion=athenaTuesday_2&messageId=36409354f8c742818f589d30b53d1f42&urlSlug=nordlichter-and-luxus-glas-iglus-in-lappland-glass-resort-rovaniemi-lappland-finnland'



SELECT
	PARSE_URL($page_url, 1)              AS parsed_url,
	$page_url                            AS page_url,
	parsed_url['path']::VARCHAR          AS page_urlpath,
	parsed_url['host']::VARCHAR          AS page_urlhost,
	IFF(parsed_url IS NULL, FALSE, TRUE) AS is_valid_url,
	CASE

		WHEN
			page_urlpath LIKE '%current-sales%'
				OR page_urlpath LIKE '%aktuelle-angebote%'
				OR page_urlpath LIKE '%currentSales'
				OR page_urlpath LIKE 'aanbiedingen' -- NL
				OR page_urlpath LIKE '%offerte-in-corso%' -- IT
				OR page_urlpath LIKE '%nuvaerende-salg%'
				OR page_urlpath LIKE '%aktuella-kampanjer%'
				OR page_urlpath IN ('/', '')
				OR page_urlpath IS NULL
			THEN 'home page'

		WHEN
			page_urlpath LIKE '%/sale-hotel' -- client side
				OR page_urlpath LIKE '%/sale' -- client side
				OR page_urlpath LIKE '%/offerta'
				OR page_urlpath LIKE '%/aanbiedingen'
				OR page_url REGEXP
				   '.*\\/(sales.travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. sales.travelbird.de
				OR page_url REGEXP
				   '.*\\/(sales.([a-z,A-Z]{2}).travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. sales.fr.travelbird.be
				OR page_url REGEXP
				   '.*\\/([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. de.sales.secretescapes.com
				OR page_url REGEXP
				   '.*\\/([a-z,A-Z]{2}\\.)([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. co.uk.sales.secretescapes.com
				OR page_url REGEXP
				   '.*\\/\\/(secretescapes|secretescapesnl)\\.journaway\\.(com|de|nl)\\/[a-z,A-Z]{2}\\/(angebote|angebot|aanbiedingen)\\/.*' -- e.g. secretescapes.journaway.com/de/angebote
				OR page_url REGEXP
				   '.*\\/\\/(secretescapes|secretescapesnl)\\.neon-reisen\\.(com|de|nl)\\/(angebote|angebot|aanbiedingen)\\/.*' -- e.g. secretescapes.neon-reisen.de/angebot
			THEN 'sale page'
		WHEN
			page_urlpath LIKE 'instant-access/%' -- added 27-03
				OR page_urlpath = 'login'
			THEN 'instant access'
		WHEN
			page_urlpath IN ('/bookings',
							 'bookings',
							 'boekingen',
							 'bokningar',
							 'prenotazioni'
				) -- added 27-03
				OR page_urlpath IN ('/buchungen', 'buchungen') -- added 27-03
			THEN 'my bookings'
		WHEN
			page_urlpath LIKE '%/filter%' -- updated 28-02
				OR page_urlpath LIKE '%/filtra%' -- updated 28-02
			THEN 'filter page'
		WHEN
			page_urlpath LIKE '%/checkout'
				OR page_urlpath LIKE 'sale/book-hotel%' -- HO ndm
				OR page_urlpath LIKE '/sale/book' -- added in 29-03
				OR page_urlpath LIKE '/api/graphql/' -- added in 29-03
			THEN 'booking form'
		WHEN
			page_urlpath LIKE '%/rooms'
			THEN 'booking flow room selection'
		WHEN
			page_urlpath LIKE '%/flights'
			THEN 'booking flow flight selection'
		WHEN
			page_urlpath LIKE '%/dates'
			THEN 'booking flow date selection'
		WHEN
			page_urlpath LIKE '%/accommodation'
			THEN 'booking flow accommodation selection'
		WHEN
			page_urlpath LIKE '%/roundtrip' -- to check with Mehmet what this is
			THEN 'booking flow roundtrip selection'
		WHEN
			page_urlpath LIKE '%/extras'
			THEN 'booking flow extras selection'
		WHEN
			page_urlpath LIKE '%/tickets' -- the step to choose activities/leisures/experiences
			THEN 'booking flow tickets selection'
		WHEN
			page_urlpath LIKE '%/cars'
			THEN 'booking flow cars selection'
		WHEN
			page_urlpath LIKE '%/insurance'
			THEN 'booking flow insurance selection'

		WHEN
			page_urlpath LIKE '%search/search%'
				OR page_urlpath LIKE '%mbSearch/mbSearch'
			THEN 'search results'
		WHEN
			page_urlpath LIKE '%/sale-offers' -- client side
			THEN 'offer page'
		WHEN
			page_urlpath LIKE '%/ueber-uns%' -- updated 01-03
				OR page_urlpath LIKE '%/about-us%' -- updated 01-03
			THEN 'about us'
		WHEN
			page_urlpath LIKE ANY ('contact', '%/contact%') -- updated 28-02
				OR page_urlpath LIKE '%/kontakt%' -- updated 28-02
			THEN 'contact'
		WHEN
			page_urlpath LIKE '%forgottenpassword%'
				OR page_urlpath LIKE '%/passwortvergessen%'
			THEN 'forgotten password'
		WHEN
			page_urlpath LIKE '%/accounts/%' -- TB
				OR page_urlpath LIKE '%your-account%'
				OR page_urlpath LIKE '%konto%'
				OR page_urlpath = 'votre-compte'
			THEN 'your account'
		WHEN
			page_urlpath LIKE ANY ('credits', '/credits%') -- updated 28-02
				OR page_urlpath LIKE ANY ('/guthaben%', 'guthaben') -- updated 28-02
			THEN 'credits'
		WHEN
			page_urlpath = 'faq' -- updated 28-02
				OR page_urlpath LIKE '%mobile-faq%' -- updated 28-02
			THEN 'faq'
		WHEN
			page_urlpath LIKE '%/my-favourites%' -- updated 28-02
				OR page_urlpath LIKE '%meine-favoriten' -- updated 28-02
			THEN 'my favourites'
		WHEN
			page_urlpath LIKE '%freunde-einladen'
				OR page_urlpath LIKE '%invite_friends'
			THEN 'invite friends'
		WHEN
			page_urlpath LIKE '%payment/directSuccess/%'
			THEN 'booking confirmation'
		WHEN
			page_urlpath LIKE '%/zahlungsinformationen/%'
			THEN 'payment information'
		WHEN
			page_urlpath LIKE '/payment/altPaymentFailure/%'
			THEN 'payment failure page'
		WHEN
			page_urlpath LIKE '%/media%'
			THEN 'media'
		WHEN
			page_url LIKE '%mp.secretescapes%'
			THEN 'competition'
		WHEN
			page_urlpath LIKE '/privacy-policy%' -- updated 28-02
				OR page_urlpath LIKE '/mobile-privacy-policy% ' -- updated 28-02
				OR page_url LIKE '%datenschutzerklaerung' -- updated 01-03
			THEN 'privacy policy'
		WHEN
			page_urlpath LIKE '%/magazine-de/%'
				OR page_urlhost = 'magazine.secretescapes.com'
			THEN 'se magazine'
		WHEN
			page_urlpath LIKE '/your-subscriptions%'
				OR page_urlpath LIKE '/ihre-abonnemente%' -- updated 01-03
				OR page_urlpath IN ('uw-abonnementen', 'vos-abonnements', 'tue-iscrizioni', 'nastaveni-oznameni')
			THEN 'subscriptions'
		WHEN
			page_urlpath LIKE '%terms-and-conditions%'
				OR page_urlpath LIKE '%/agb'
			THEN 'terms and conditions'
		WHEN
			page_urlpath LIKE ANY ('%/voucher%', 'voucher/buyVoucher')
				OR page_urlpath LIKE '%/geschenkgutscheine%'
				OR page_urlpath = 'vouchers-offer'
			THEN 'vouchers'
		WHEN
			page_urlpath LIKE '%work-with-us'
				OR page_urlpath LIKE '%workWithUs'
				OR page_urlpath LIKE '%/addyourhotelprivacy-policy%'
			THEN 'work with us'
		WHEN
			page_urlpath LIKE '/reminders%' -- added 01-03
				OR page_urlpath LIKE '/erinnerungen%' -- added 01-03
			THEN 'reminders'
		WHEN
			page_urlpath LIKE '%/holds%' -- added 01-03
				OR page_urlpath LIKE '%/reservierungen%' -- added 01-03
				OR page_urlpath = 'reservationer'
			THEN 'holds'
		WHEN
			page_urlpath LIKE '%/territory%' -- added 01-03
			THEN 'territory'
		WHEN
			page_urlpath LIKE '/hotelSale/calendar'
				OR page_urlpath LIKE '/sale/allocationsByDate'
				OR page_urlpath LIKE '%/sale-calendar'
			THEN 'calendar'
		WHEN page_urlpath REGEXP
			 '^booking\\/\\d+\\/[^/]+\\/v3-\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}-\\d{13}-\\w{8}(\\/|\\/null)?$'
			THEN 'plan your trip'
		WHEN page_urlpath LIKE '%sale-preferences' THEN 'sale preferences'
		WHEN page_urlpath LIKE '%your-subscriptions' THEN 'your subscriptions'
		WHEN
			is_valid_url = FALSE THEN 'broken url'
		ELSE 'other'
	END                                  AS page_classification
;



CREATE OR REPLACE FUNCTION se_dev_robin.data.page_url_categorisation(page_url VARCHAR
																	)
	RETURNS VARCHAR
	LANGUAGE SQL
AS
$$ WITH prep AS (
	SELECT
		page_url,
		PARSE_URL(page_url, 1)               AS parsed_url,
		parsed_url['path']::VARCHAR          AS page_urlpath,
		parsed_url['host']::VARCHAR          AS page_urlhost,
		IFF(parsed_url IS NULL, FALSE, TRUE) AS is_valid_url,
		CASE
			WHEN
				page_urlpath LIKE '%current-sales%'
					OR page_urlpath LIKE '%aktuelle-angebote%'
					OR page_urlpath LIKE '%currentSales'
					OR page_urlpath LIKE 'aanbiedingen' -- NL
					OR page_urlpath LIKE '%offerte-in-corso%' -- IT
					OR page_urlpath LIKE '%nuvaerende-salg%'
					OR page_urlpath LIKE '%aktuella-kampanjer%'
					OR page_urlpath IN ('/', '')
					OR page_urlpath IS NULL
				THEN 'home page'

			WHEN
				page_urlpath LIKE '%/sale-hotel' -- client side
					OR page_urlpath LIKE '%/sale' -- client side
					OR page_urlpath LIKE '%/offerta'
					OR page_urlpath LIKE '%/aanbiedingen'
					OR page_url REGEXP
					   '.*\\/(sales.travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. sales.travelbird.de
					OR page_url REGEXP
					   '.*\\/(sales.([a-z,A-Z]{2}).travelbird.([a-z,A-Z]{2}))\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. sales.fr.travelbird.be
					OR page_url REGEXP
					   '.*\\/([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. de.sales.secretescapes.com
					OR page_url REGEXP
					   '.*\\/([a-z,A-Z]{2}\\.)([a-z,A-Z]{2}\\.)(sales.secretescapes.com)\\/[0-9]{1,6}\\/([a-z,A-Z,0-9]|-|_)*\\/.*' -- e.g. co.uk.sales.secretescapes.com
					OR page_url REGEXP
					   '.*\\/\\/(secretescapes|secretescapesnl)\\.journaway\\.(com|de|nl)\\/[a-z,A-Z]{2}\\/(angebote|angebot|aanbiedingen)\\/.*' -- e.g. secretescapes.journaway.com/de/angebote
					OR page_url REGEXP
					   '.*\\/\\/(secretescapes|secretescapesnl)\\.neon-reisen\\.(com|de|nl)\\/(angebote|angebot|aanbiedingen)\\/.*' -- e.g. secretescapes.neon-reisen.de/angebot
				THEN 'sale page'
			WHEN
				page_urlpath LIKE 'instant-access/%' -- added 27-03
					OR page_urlpath = 'login'
				THEN 'instant access'
			WHEN
				page_urlpath IN ('/bookings',
								 'bookings',
								 'boekingen',
								 'bokningar',
								 'prenotazioni'
					) -- added 27-03
					OR page_urlpath IN ('/buchungen', 'buchungen') -- added 27-03
				THEN 'my bookings'
			WHEN
				page_urlpath LIKE '%/filter%' -- updated 28-02
					OR page_urlpath LIKE '%/filtra%' -- updated 28-02
				THEN 'filter page'
			WHEN
				page_urlpath LIKE '%/checkout'
					OR page_urlpath LIKE 'sale/book-hotel%' -- HO ndm
					OR page_urlpath LIKE '/sale/book' -- added in 29-03
					OR page_urlpath LIKE '/api/graphql/' -- added in 29-03
				THEN 'booking form'
			WHEN page_urlpath REGEXP
				 '^booking\\/\\d+\\/[^/]+\\/v3-\\w{8}-\\w{4}-\\w{4}-\\w{4}-\\w{12}-\\d{13}-\\w{8}(\\/|\\/null)?$'
				THEN 'plan your trip'
			WHEN
				page_urlpath LIKE '%/rooms'
				THEN 'booking flow room selection'
			WHEN
				page_urlpath LIKE '%/flights'
				THEN 'booking flow flight selection'
			WHEN
				page_urlpath LIKE '%/dates'
				THEN 'booking flow date selection'
			WHEN
				page_urlpath LIKE '%/accommodation'
				THEN 'booking flow accommodation selection'
			WHEN
				page_urlpath LIKE '%/roundtrip' -- to check with Mehmet what this is
				THEN 'booking flow roundtrip selection'
			WHEN
				page_urlpath LIKE '%/extras'
				THEN 'booking flow extras selection'
			WHEN
				page_urlpath LIKE '%/tickets' -- the step to choose activities/leisures/experiences
				THEN 'booking flow tickets selection'
			WHEN
				page_urlpath LIKE '%/cars'
				THEN 'booking flow cars selection'
			WHEN
				page_urlpath LIKE '%/insurance'
				THEN 'booking flow insurance selection'

			WHEN
				page_urlpath LIKE '%search/search%'
					OR page_urlpath LIKE '%mbSearch/mbSearch'
				THEN 'search results'
			WHEN
				page_urlpath LIKE '%/sale-offers' -- client side
				THEN 'offer page'
			WHEN
				page_urlpath LIKE '%/ueber-uns%' -- updated 01-03
					OR page_urlpath LIKE '%/about-us%' -- updated 01-03
				THEN 'about us'
			WHEN
				page_urlpath LIKE ANY ('contact', '%/contact%') -- updated 28-02
					OR page_urlpath LIKE '%/kontakt%' -- updated 28-02
				THEN 'contact'
			WHEN
				page_urlpath LIKE '%forgottenpassword%'
					OR page_urlpath LIKE '%/passwortvergessen%'
				THEN 'forgotten password'
			WHEN
				page_urlpath LIKE '%/accounts/%' -- TB
					OR page_urlpath LIKE '%your-account%'
					OR page_urlpath LIKE '%konto%'
					OR page_urlpath = 'votre-compte'
				THEN 'your account'
			WHEN
				page_urlpath LIKE ANY ('credits', '/credits%') -- updated 28-02
					OR page_urlpath LIKE ANY ('/guthaben%', 'guthaben') -- updated 28-02
				THEN 'credits'
			WHEN
				page_urlpath = 'faq' -- updated 28-02
					OR page_urlpath LIKE '%mobile-faq%' -- updated 28-02
				THEN 'faq'
			WHEN
				page_urlpath LIKE '%/my-favourites%' -- updated 28-02
					OR page_urlpath LIKE '%meine-favoriten' -- updated 28-02
				THEN 'my favourites'
			WHEN
				page_urlpath LIKE '%freunde-einladen'
					OR page_urlpath LIKE '%invite_friends'
				THEN 'invite friends'
			WHEN
				page_urlpath LIKE '%payment/directSuccess/%'
				THEN 'booking confirmation'
			WHEN
				page_urlpath LIKE '%/zahlungsinformationen/%'
				THEN 'payment information'
			WHEN
				page_urlpath LIKE '/payment/altPaymentFailure/%'
				THEN 'payment failure page'
			WHEN
				page_urlpath LIKE '%/media%'
				THEN 'media'
			WHEN
				page_url LIKE '%mp.secretescapes%'
				THEN 'competition'
			WHEN
				page_urlpath LIKE '/privacy-policy%' -- updated 28-02
					OR page_urlpath LIKE '/mobile-privacy-policy% ' -- updated 28-02
					OR page_url LIKE '%datenschutzerklaerung' -- updated 01-03
				THEN 'privacy policy'
			WHEN
				page_urlpath LIKE '%/magazine-de/%'
				OR page_urlhost = 'magazine.secretescapes.com'
				THEN 'se magazine'
			WHEN
				page_urlpath LIKE '/your-subscriptions%'
					OR page_urlpath LIKE '/ihre-abonnemente%' -- updated 01-03
					OR page_urlpath IN ('uw-abonnementen', 'vos-abonnements', 'tue-iscrizioni', 'nastaveni-oznameni')
				THEN 'subscriptions'
			WHEN
				page_urlpath LIKE '%terms-and-conditions%'
					OR page_urlpath LIKE '%/agb'
				THEN 'terms and conditions'
			WHEN
				page_urlpath LIKE ANY ('%/voucher%', 'voucher/buyVoucher')
					OR page_urlpath LIKE '%/geschenkgutscheine%'
					OR page_urlpath = 'vouchers-offer'
				THEN 'vouchers'
			WHEN
				page_urlpath LIKE '%work-with-us'
					OR page_urlpath LIKE '%workWithUs'
					OR page_urlpath LIKE '%/addyourhotelprivacy-policy%'
				THEN 'work with us'
			WHEN
				page_urlpath LIKE '/reminders%' -- added 01-03
					OR page_urlpath LIKE '/erinnerungen%' -- added 01-03
				THEN 'reminders'
			WHEN
				page_urlpath LIKE '%/holds%' -- added 01-03
					OR page_urlpath LIKE '%/reservierungen%' -- added 01-03
					OR page_urlpath = 'reservationer'
				THEN 'holds'
			WHEN
				page_urlpath LIKE '%/territory%' -- added 01-03
				THEN 'territory'
			WHEN
				page_urlpath LIKE '/hotelSale/calendar'
					OR page_urlpath LIKE '/sale/allocationsByDate'
					OR page_urlpath LIKE '%/sale-calendar'
				THEN 'calendar'

			WHEN page_urlpath LIKE '%sale-preferences' THEN 'sale preferences'
			WHEN page_urlpath LIKE '%your-subscriptions' THEN 'your subscriptions'
			WHEN
				is_valid_url = FALSE THEN 'invalid url'
			ELSE 'other'
		END                                  AS page_url_category
	)
	SELECT page_url_category FROM prep
$$
;

SELECT
	se_dev_robin.data.page_url_categorisation('https://www.secretescapes.de/auszeit-and-tradition-in-den-bayerischen-bergen-golf-and-alpin-wellness-resort-hotel-ludwig-royal-oberstaufen-steibis-allgaeu-bayern-deutschland/sale-hotel')


SELECT
	se.data.page_url_categorisation(ses.page_url) AS page_category,
	ses.page_url
FROM se.data_pii.scv_event_stream ses
WHERE ses.page_url IS NOT NULL
  AND ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.se_brand = 'SE Brand'
  AND ses.event_name IN ('page_view')
;



SELECT
	PARSE_URL(
			'https://magazine.secretescapes.com/escapist/regal-escapes-8-stunning-breaks-inspired-by-empress-sisi-of-austria/',
			1)['host']::VARCHAR
;


SELECT
	COUNT(*)
FROM se.data_pii.scv_event_stream ses
WHERE ses.page_url IS NOT NULL
  AND ses.event_tstamp >= CURRENT_DATE - 1
  AND ses.se_brand = 'SE Brand'
  AND ses.event_name IN ('page_view')
-- AND se_dev_robin.data.page_url_categorisation(ses.page_url) = 'other';
1,686,617


SELECT * FROM latest_vault.kingfisher.sales_kingfisher sk;


SELECT * FROM