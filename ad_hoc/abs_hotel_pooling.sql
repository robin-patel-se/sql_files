SELECT DISTINCT
	unit.id         AS unit_id,
	tour.id         AS tour_id,
	section.id      AS section_id,
	hotel.id        AS hotel_id,
	tb_offer.concept_name,
	tb_offer.active AS sale_active
FROM latest_vault.travelbird_mysql.touroperating_tour tour
INNER JOIN latest_vault.travelbird_mysql.touroperating_tourunit unit
	ON tour.id = unit.tour_id
INNER JOIN latest_vault.travelbird_mysql.touroperating_toursection_tour_units toursection
	ON unit.id = toursection.tourunit_id
INNER JOIN latest_vault.travelbird_mysql.touroperating_toursection section
	ON toursection.toursection_id = section.id
	AND tour.id = section.tour_id
INNER JOIN latest_vault.travelbird_mysql.touroperating_toursectionproduct product product
	ON section.id = product.toursection_id
INNER JOIN latest_vault.travelbird_mysql.products_hotelproduct hotel
	ON product.object_id = hotel.id
INNER JOIN latest_vault.travelbird_mysql.offers_tourproductlink tour_product_link
	ON tour.id = tour_product_link.tour_id
INNER JOIN latest_vault.travelbird_mysql.offers_offer offer
	ON tour_product_link.offer_id = offer.id
INNER JOIN data_vault_mvp.dwh.tb_offer tb_offer
	ON offer.id = tb_offer.id


/* unique_objects_per_tour AS (
        SELECT
            DISTINCT
                product.object_id,
                product.toursection_id,
                product.is_main,
                product.content_type_id
        FROM LATEST_VAULT.TRAVELBIRD_MYSQL.touroperating_toursectionproduct product
        WHERE product.content_type_id = '496'*/


SELECT
	unit.id    AS unit_id,
	tour.id    AS tour_id,
	section.id AS section_id,
-- 	hotel.id        AS hotel_id,
-- 	tb_offer.concept_name,
-- 	tb_offer.active AS sale_active
FROM latest_vault.travelbird_mysql.touroperating_tour tour
INNER JOIN latest_vault.travelbird_mysql.touroperating_tourunit unit
	ON tour.id = unit.tour_id
INNER JOIN latest_vault.travelbird_mysql.touroperating_toursection_tour_units toursection
	ON unit.id = toursection.tourunit_id
INNER JOIN latest_vault.travelbird_mysql.touroperating_toursection section
	ON toursection.toursection_id = section.id
	AND tour.id = section.tour_id


-- tour is overall wrapper - eg. Luxury Sri Lanka
-- tour unit - 10/12/14 night package
-- tour section - split out itinerary
-- hotel - a property within the section - there can be multiple properties within a section


SELECT *
FROM latest_vault.travelbird_mysql.touroperating_tourunit unit

SELECT *
FROM latest_vault.travelbird_mysql.touroperating_tour tour



WITH
	active_tb_offers AS (
		SELECT
			offer_to_tour_link.tour_id,
			LISTAGG(DISTINCT offer.concept_name, ', ') WITHIN GROUP ( ORDER BY offer.concept_name ) AS concept_name,
			MAX(offer.sale_active) AS max_sale_active
		FROM data_vault_mvp.dwh.tb_offer offer
		INNER JOIN latest_vault.travelbird_mysql.offers_tourproductlink offer_to_tour_link
			ON offer.id = offer_to_tour_link.offer_id
-- 		WHERE offer.sale_active
-- 		  WHERE offer_to_tour_link.tour_id = 105 -- TODO REMOVE
		GROUP BY offer_to_tour_link.tour_id
	)
SELECT
	tour.id                                            AS tour_id,
	tour.name,                                                          -- TODO REMOVE
	unit.id                                            AS unit_id,
	unit.name,                                                          -- TODO REMOVE
	toursection.toursection_id                         AS section_id,
	section.internal_name                              AS section_name, -- TODO REMOVE
	section_to_hotel_link.object_id                    AS hotel_id,
	hotel.name,                                                         -- TODO REMOVE
	active_tb_offers.concept_name,
	active_tb_offers.max_sale_active AS tour_has_active_offer

-- 	tb_offer.se_sale_id,
-- 	tb_offer.id                     AS offer_id,
-- 	tb_offer.concept_name,
-- 	tb_offer.sale_active,
-- FROM data_vault_mvp.dwh.tb_offer tb_offer
FROM latest_vault.travelbird_mysql.touroperating_tour tour
INNER JOIN latest_vault.travelbird_mysql.touroperating_tourunit unit
	ON tour.id = unit.tour_id
	-- TODO need to understand how we filter for active units
INNER JOIN latest_vault.travelbird_mysql.touroperating_toursection_tour_units toursection
	ON unit.id = toursection.tourunit_id
INNER JOIN latest_vault.travelbird_mysql.touroperating_toursection section
	ON toursection.toursection_id = section.id -- TODO REMOVE
INNER JOIN latest_vault.travelbird_mysql.touroperating_toursectionproduct AS section_to_hotel_link
	ON toursection.toursection_id = section_to_hotel_link.toursection_id
	AND section_to_hotel_link.content_type_id = 496 -- accommodation tour section products only
INNER JOIN latest_vault.travelbird_mysql.products_hotelproduct hotel
	ON section_to_hotel_link.object_id = hotel.id -- TODO REMOVE
LEFT JOIN active_tb_offers
	ON tour.id = active_tb_offers.tour_id
-- WHERE tour.id = 105 -- TODO REMOVE
;





WITH
	active_tb_offers AS (
		SELECT
			offer_to_tour_link.tour_id,
			LISTAGG(DISTINCT offer.concept_name, ', ') WITHIN GROUP ( ORDER BY offer.concept_name ) AS concept_name,
			MAX(offer.sale_active) AS max_sale_active
		FROM data_vault_mvp.dwh.tb_offer offer
		INNER JOIN latest_vault.travelbird_mysql.offers_tourproductlink offer_to_tour_link
			ON offer.id = offer_to_tour_link.offer_id
		GROUP BY offer_to_tour_link.tour_id
	)
SELECT
	tour.id                                            AS tour_id,
	unit.id                                            AS unit_id,
	toursection.toursection_id                         AS section_id,
	section_to_hotel_link.object_id                    AS hotel_id,
	active_tb_offers.concept_name,
	active_tb_offers.max_sale_active AS tour_has_active_offer
FROM latest_vault.travelbird_mysql.touroperating_tour tour
INNER JOIN latest_vault.travelbird_mysql.touroperating_tourunit unit
	ON tour.id = unit.tour_id
	-- TODO need to understand how we filter for active units
INNER JOIN latest_vault.travelbird_mysql.touroperating_toursection_tour_units toursection
	ON unit.id = toursection.tourunit_id
INNER JOIN latest_vault.travelbird_mysql.touroperating_toursectionproduct AS section_to_hotel_link
	ON toursection.toursection_id = section_to_hotel_link.toursection_id
	AND section_to_hotel_link.content_type_id = 496 -- accommodation tour section products only
LEFT JOIN active_tb_offers
	ON tour.id = active_tb_offers.tour_id
;

SELECT *
FROM latest_vault.travelbird_mysql.offers_tourproductlink
WHERE offers_tourproductlink.tour_id = 105

SELECT *
FROM latest_vault.travelbird_mysql.touroperating_toursectionproduct
------------------------------------------------------------------------------------------------------------------------
-- based on the enrichment columns we want to add to the granularity, we can start at offer and use joins to actively
-- fan out to the desired grain

-- started with tb offer
-- limited to columns we need
-- looked at offers_tourproductlink to see what offers are linked to the 105 tour
-- filtered tb_offer to  offer ids linked to tour 105
-- joined offers_tourproductlink

-- identified that tb offers can be associated to multiple tours
-- need to aggregate tb offer to tour level separately


SELECT * FROm latest_vault.travelbird_mysql.touroperating_tourunit WHERe TOUROPERATING_TOURUNIT.tour_id = 105

-- need to find out how units are classified as active