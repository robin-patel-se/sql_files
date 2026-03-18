/*
Hello Team,

We noticed an issue where the SE rate_local_calculated is sent to fornova as Sell Rate per day instead of per stay,

For the Deluxe Room 5N DR5:DR5RACK) the rate we sent to Fornova on 17/02/2026 was €156 instead of €780.00 and that is the rate per day if you divide by the fixed stay of 5 nights.

I investigated a few more deals from PAT and MARI and it seems only one deal is affected,

Can you have a look and fix it? Maybe I'm missing something,

Opportunity ID - 0066900001NkrQX
https://camilla-cms.secretescapes.tech/hotelSale/edit/26772
https://mari.secretescapes.tech/hotel/0016900002dTGuO
See the data we sent to Fornova from this morning attached.
*/

0066900001NkrQX

SELECT *
FROM data_vault_mvp.dwh.se_offers_inclusions_rates
WHERE se_offers_inclusions_rates.salesforce_opportunity_id = '0066900001NkrQX'
  AND se_offers_inclusions_rates.allocation_date = '2026-02-17'
;

-- checking through the logic as this is a PER_STAY rate type the rate is brought directly from the harmonised offer calendar view using column total_rate_rc

SELECT *
FROM data_vault_mvp.dwh.harmonised_offer_calendar_view
WHERE harmonised_offer_calendar_view.calendar_date = '2026-02-17'
  AND harmonised_offer_calendar_view.salesforce_opportunity_id = '0066900001NkrQX'
;
;

-- harmonised offer calendar view retrieves the rate from travelbird_offer_calendar table, using field rate_rate_rc

SELECT *
FROM data_vault_mvp.dwh.travelbird_offer_calendar toc
WHERE calendar_date = '2026-02-17'
  AND salesforce_opportunity_id = '0066900001NkrQX'
;
;

-- travelbird_offer_calendar retrieves the rate from travelbird_tracy_room_rates from rate_rc field
-- using hotel code and room code

SELECT *
FROM data_vault_mvp.dwh.travelbird_tracy_room_rates
WHERE hotel_code = '0016900002dTGuO'
  AND room_code = 'DSVB3N'
  AND rate_date = '2026-02-17'
;

-- this rate comes from offer permutation travelbird_tracy_offer_permutation_component_allocation_and_rates under field rate_amount_rate_currency
-- biapp/task_catalogue/dv/dwh/allocation/travelbird/travelbird_offer_permutation_component.py

SELECT *
FROM data_vault_mvp.dwh.travelbird_tracy_offer_permutation_component_allocation_and_rates
WHERE hotel_code = '0016900002dTGuO'
  AND room_code = 'DSVB3N'
  AND calendar_date = '2026-02-17'
-- qualify lifted from travelbird_tracy_room_rates
QUALIFY ROW_NUMBER() OVER (
	PARTITION BY
		calendar_date,
		hotel_code,
		room_code,
		rate_plan_code,
		rate_plan_rack_code
	ORDER BY
		package_total
	) = 1
;



SELECT *,
       ROW_NUMBER() OVER (
	PARTITION BY
		calendar_date,
		hotel_code,
		room_code,
		rate_plan_code,
		rate_plan_rack_code
	ORDER BY
		package_total
	)
FROM data_vault_mvp.dwh.travelbird_tracy_offer_permutation_component_allocation_and_rates
WHERE hotel_code = '0016900002dTGuO'
  AND room_code = 'DSVB3N'
  AND calendar_date = '2026-02-17'
  AND rate_plan_code = 'DR5'
-- qualify lifted from travelbird_tracy_room_rates


-- the 156 rate is what is showing in this data set


-- rate_amount_rate_currency is calculated using, rate_amount which is a calculation on package_price_per_night
-- this is based on a flatten on travelbird__packages_data;
-- biapp/task_catalogue/dv/dwh/travelbird/packages_data.py


SELECT * FROM data_vault_mvp.dwh.travelbird__packages_data tpd
WHERE tpd.travel_date = '2026-02-17';


