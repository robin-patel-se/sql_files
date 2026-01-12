SELECT *
FROM latest_vault.travelbird_mysql.offers_markupbase om
WHERE om.offer_id = 118575

-- biapp/task_catalogue/dv/dwh/sale/packages/package_component_harmonised.py

SELECT *
FROM data_vault_mvp.dwh.package_component_harmonised pch
	LEFT JOIN latest_vault.travelbird_mysql.offers_markupbase om ON pch.offer_id = om.offer_id
WHERE pch.offer_id = '122861'
  AND pch.component_configuration_id = '1e5eeaa8179cc8386512843183acec5cb50559e969b82cbd73def44d99831d3b'
;

SELECT *
FROM latest_vault.travelbird_mysql.offers_markupbase om
QUALIFY COUNT(*) OVER (PARTITION BY om.offer_id) > 1
;

-- each componnent will have

-- MarkupPercentage
-- default markup is hardcoded
-- calculation type is hardcorded


SELECT *
FROM data_vault_mvp.dwh.travelbird_offer_allocation_and_rates toaar
;


--     for product_type, percentage, max_amount in (
--         (ProductTypeEnum.ACCOMMODATION, 18, None),
--         (ProductTypeEnum.TRANSFER, 15, None),
--         (ProductTypeEnum.CAR, 17, None),
--         (ProductTypeEnum.LEISURE, 15, None),
--         (ProductTypeEnum.TOUR, 18, None),
--         (ProductTypeEnum.FLIGHT, 10, None),
--     )


-- ProductTypeEnum.ACCOMMODATION: MarkupCalculationModeEnum.PROFIT_MARGIN,
-- ProductTypeEnum.TRANSFER: MarkupCalculationModeEnum.MARKUP,
-- ProductTypeEnum.CAR: MarkupCalculationModeEnum.MARKUP,
-- ProductTypeEnum.LEISURE: MarkupCalculationModeEnum.PROFIT_MARGIN,
-- ProductTypeEnum.FLIGHT: MarkupCalculationModeEnum.MARKUP,
-- ProductTypeEnum.TOUR: MarkupCalculationModeEnum.PROFIT_MARGIN


SELECT *
FROM data_vault_mvp.dwh.travelbird__packages_data
;

-- products in data_vault_mvp.dwh.travelbird__packages_data at component level holds margin and cost price so a calculation of take rate can be computed


SELECT *
FROM latest_vault.travelbird.configs_data_se_catalogue_de cdscd
;

------------------------------------------------------------------------------------------------------------------------

SELECT *
FROM latest_vault.travelbird_mysql.offers_accommodationmarkupfilter oa
;

SELECT *
FROM latest_vault.travelbird_mysql.offers_flightmarkupfilter o
;

SELECT *
FROM latest_vault.travelbird_mysql.offers_leisuremarkupfilter ol
;

SELECT *
FROM latest_vault.travelbird_mysql.offers_tourmarkupfilter ot
;

SELECT *
FROM latest_vault.travelbird_mysql.offers_accommodationmarkupfilter oa
;



SELECT *
FROM latest_vault.travelbird_mysql.offers_accommodationmarkupfilter_allocation_units oaau
;

SELECT *
FROM latest_vault.travelbird_mysql.offers_leisuremarkupfilter_leisure_units ollu


SELECT *
FROM latest_vault.travelbird_mysql.offers_tourmarkupfilter_tour_units ottu
;

------------------------------------------------------------------------------------------------------------------------


