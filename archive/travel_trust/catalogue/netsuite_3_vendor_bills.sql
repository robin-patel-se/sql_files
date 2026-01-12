GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_vendor_bills_report TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_vendor_bills_report TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_vendor_bills_report TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_vendor_bills_report TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_vendor_bills_report TO ROLE personal_role__ezraphilips;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_vendor_bills_report TO ROLE personal_role__sebastianmaczka;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_vendor_bills_report TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_vendor_bills_report TO ROLE personal_role__barbarazacchino;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_vendor_bills_report TO ROLE personal_role__ailiemcderment;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_vendor_bills_report TO ROLE personal_role__andypauer;

CREATE OR REPLACE VIEW collab.travel_trust.tb_netsuite_vendor_bills_report COPY GRANTS AS
(
WITH flatten_events AS (
    --flatten creation events
    SELECT oo.order_id,
           oo.event_type,
           oo.created_at_dts,
           coi_elements.value                                    AS created_order_items,
           coi_elements.value:id::INT                            AS order_item_id,
           coi_elements.value:creation_datetime::TIMESTAMP       AS order_item_created_timestamp,
           coi_elements.value:start_date::VARCHAR                AS start_date,
           coi_elements.value:end_date::VARCHAR                  AS end_date,
           coi_elements.value:partner_name::VARCHAR              AS partner_name,
           coi_elements.value:partner_id::VARCHAR                AS partner_id,
           coi_elements.value:supplier_reference::VARCHAR        AS supplier_reference,
           coi_elements.value:flight_reservation_number::VARCHAR AS flight_reservation_number,
           coi_elements.value:is_external::BOOLEAN               AS is_external,
           coi_elements.value:order_item_type::VARCHAR           AS order_item_type,
           coi_elements.value:order_item_type_id::VARCHAR        AS order_item_type_id,
           coi_elements.value:main_order_item_type::VARCHAR      AS main_order_item_type,
           coi_elements.value:main_order_item_type_id::VARCHAR   AS main_order_item_type_id,
           coi_elements.value:partner_invoice_terms::VARCHAR     AS partner_invoice_terms
    FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oo,
         LATERAL FLATTEN(INPUT => PARSE_JSON(oo.event_data):created_orderitems, OUTER => TRUE) coi_elements
),
     created_confirmed_events AS (
         SELECT *
         FROM flatten_events fe
         WHERE fe.event_type IN ('ORDER_CONFIRMED', 'ORDER_CREATED')
     ),
     aggregate_events AS (
         --aggregate creation events to order level
         SELECT cce.order_id,
                MIN(IFF(event_type = 'ORDER_CONFIRMED', cce.created_at_dts, NULL)) AS booking_confirmation_date,
                MIN(IFF(event_type = 'ORDER_CREATED', cce.start_date, NULL))       AS start_date,
                MAX(IFF(event_type = 'ORDER_CREATED', cce.end_date, NULL))         AS end_date
         FROM created_confirmed_events cce
         GROUP BY 1
     )
SELECT oo.id                                                         AS booking_record_reference,
       oo.id || '.' || ooi.id                                        AS component_reference,
       oof.product_line,
       COALESCE(cce.order_item_created_timestamp, oo.created_at_dts) AS booking_date,
       ae.booking_confirmation_date,
       ae.start_date                                                 AS holiday_start_date,
       ae.end_date                                                   AS holiday_end_date,
       cce.start_date                                                AS component_start_date,
       cce.end_date                                                  AS component_end_date,
       CASE
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 462 THEN 'Extra'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 724
               THEN 'Financial Protection'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 463 THEN 'Booking Fee'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 728 THEN 'Car Extra'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 435 THEN 'Car'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 464 THEN 'Change Fee'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 431 THEN 'Flight'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 442 THEN 'Accommodation'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 417 THEN 'Included'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 457 THEN 'Leisure'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 429 THEN 'Luggage'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 465 THEN 'Rounding'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 418 THEN 'Included'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 460 THEN 'Tour'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 459 THEN 'Extra'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 455 THEN 'Transfer'
           WHEN COALESCE(ee.order_item_type_id, cce.main_order_item_type_id, cce.order_item_type_id) = 466 THEN 'Calamity Fund'
           END                                                       AS booking_component,
       cce.partner_name                                              AS supplier_name,
       cce.partner_id                                                AS vendor_id,
       COALESCE(NULLIF(cce.supplier_reference, ''),
                NULLIF(cce.flight_reservation_number, ''))           AS supplier_reference,
       cc.code                                                       AS supplier_currency,
       ooi.cost_price_excl_vat                                       AS total,
       se.data.posa_territory_from_tb_site_id(oo.site_id)                                                     AS territory,
       oof.external_reference                                        AS cms_sale_id,

       CASE
           WHEN cce.partner_invoice_terms = '5W-b-td' THEN 'Five weeks before travel date'
           WHEN cce.partner_invoice_terms = '3W-b-td' THEN 'Three weeks before travel date'
           WHEN cce.partner_invoice_terms = '1W-b-td' THEN 'One week before travel date'
           WHEN cce.partner_invoice_terms = 'td' THEN 'Travel date'
           WHEN cce.partner_invoice_terms = '5D-a-td' THEN 'Five days after travel date'
           WHEN cce.partner_invoice_terms = '1W-a-td' THEN 'One week after travel date'
           WHEN cce.partner_invoice_terms = '1M-a-td' THEN 'One month after travel date'

           WHEN cce.partner_invoice_terms = '1W-a-od' THEN 'One week after order date '
           WHEN cce.partner_invoice_terms = '2W-a-od' THEN 'Two weeks after order date '
           WHEN cce.partner_invoice_terms = '1M -a-od ' THEN 'One month after order date '

           WHEN cce.partner_invoice_terms = '28D - AFTER -rfp ' THEN 'Twentyeight days after rfp '
           WHEN cce.partner_invoice_terms = ' END - OF -travelmonth ' THEN 'End of travel month '
           WHEN cce.partner_invoice_terms = '3D -b-td ' THEN 'Three days before travel date '

           WHEN cce.partner_invoice_terms = ' p-i-u ' THEN 'Partner invoices us '
           WHEN cce.partner_invoice_terms = ' p-i-u-4 WK -b-td ' THEN 'Piu more than four weeks before arrival '
           WHEN cce.partner_invoice_terms = ' p-i-u-1 to4wk-b-td ' THEN 'Piu between one and four weeks before arrival '
           WHEN cce.partner_invoice_terms = ' p-i-u-a-od ' THEN 'Piu afterorder date '
           WHEN cce.partner_invoice_terms = ' p-i-u-a-td ' THEN 'Piu after travel date '

           WHEN cce.partner_invoice_terms = ' CUSTOM ' THEN 'CUSTOM'
           WHEN cce.partner_invoice_terms = ' PREPAID ' THEN 'PREPAID'
           END
                                                                     AS payment_terms,
       ooi.commission_sold_price_incl_vat_in_cost_currency           AS commissioned_total

FROM data_vault_mvp.travelbird_mysql_snapshots.orders_order_snapshot oo
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_orderitembase_snapshot ooi ON oo.id = ooi.order_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.offers_offer_snapshot oof ON oo.offer_id = oof.id
         LEFT JOIN created_confirmed_events cce ON ooi.id = cce.order_item_id
    --external events
         LEFT JOIN flatten_events ee ON ooi.id = ee.order_item_id AND ee.is_external
         LEFT JOIN aggregate_events ae ON oo.id = ae.order_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.currency_currency_snapshot cc ON ooi.cost_price_currency_id = cc.id
    )
;

SELECT DISTINCT tnvbr.booking_component
FROM collab.travel_trust.tb_netsuite_vendor_bills_report tnvbr;


SELECT *
FROM collab.travel_trust.tb_netsuite_vendor_bills_report tnvbr
WHERE tnvbr.booking_record_reference = 21903465;
-- class PartnerInvoiceTerms:
--     # Explanation: this labels were generated when every partner could be
--     # invoiced only weekly. Then someone had the idea to make it possible to
--     # invoice partners every 4 weeks.
--     # When you see f.i. "Five weeks before travel date" you have to think that
--     # we incorporated that one week invoice cycle, meaning that we calculate
--     # the "invoice after date" to be 6 weeks before travel date.
--     # Combined with partner invoice cycle you can have:
--     #   *** Invoice cycle 1 week  ***
--     #     Real invoice date made by finance would be not later than 5 weeks
--     #     (6-1) before travel date (In this case the label is correct)
--     #   *** Invoice cycle 4 weeks ***
--     #     Real invoice date made by finance would be not later than 2 weeks
--     #     (6-4) before travel date (In this case the label is misleading)
--
--     FIVE_WEEKS_BEFORE_TRAVEL_DATE = "5w-b-td"
--     THREE_WEEKS_BEFORE_TRAVEL_DATE = "3w-b-td"
--     ONE_WEEK_BEFORE_TRAVEL_DATE = "1w-b-td"
--     TRAVEL_DATE = "td"
--     FIVE_DAYS_AFTER_TRAVEL_DATE = "5d-a-td"
--     ONE_WEEK_AFTER_TRAVEL_DATE = "1w-a-td"
--     ONE_MONTH_AFTER_TRAVEL_DATE = "1m-a-td"
--
--     ONE_WEEK_AFTER_ORDER_DATE = "1w-a-od"
--     TWO_WEEKS_AFTER_ORDER_DATE = "2w-a-od"
--     ONE_MONTH_AFTER_ORDER_DATE = "1m-a-od"
--
--     TWENTYEIGHT_DAYS_AFTER_RFP = "28d-after-rfp"
--     END_OF_TRAVEL_MONTH = "end-of-travelmonth"
--     THREE_DAYS_BEFORE_TRAVEL_DATE = "3d-b-td"
--
--     PARTNER_INVOICES_US = ' p-i-u '
--     PIU_MORE_THAN_FOUR_WEEKS_BEFORE_ARRIVAL = ' p-i-u-4 WK -b-td '
--     PIU_BETWEEN_ONE_AND_FOUR_WEEKS_BEFORE_ARRIVAL = ' p-i-u-1 to4wk-b-td '
--     PIU_AFTER_ORDER_DATE = ' p-i-u-a-od '
--     PIU_AFTER_TRAVEL_DATE = ' p-i-u-a-td '
--
--     # Non-canonical terms
--     CUSTOM = "custom"
--     #: In case the partner creates the invoice
--     PREPAID = "prepaid"


_ORDERITEM_CLASS_TO_BOOKING_COMPONENT
-- HotelOrderItem: "Accommodation",
-- FlightOrderItem: "Flight",
-- LeisureOrderItem: "Leisure",
-- TransferOrderItem: "Transfer",
-- CarOrderItem: "Car",
-- TourOrderItem: "Tour",
-- AllocationServiceOrderItem: "Extra",
-- TourServiceOrderItem: "Extra",
-- LuggageOrderItem: "Luggage",
-- IncludedServiceOrderItem: "Included",
-- TourIncludedServiceOrderItem: "Included",
-- BookingFeeOrderItem: "Booking Fee",
-- RoundingOrderItem: "Rounding",
-- CalamityFundOrderItem: "Calamity Fund",
-- ChangeFeeOrderItem: "Change Fee",
-- AtolOrderItem: "Financial Protection",
-- CarExtraOrderItem: "Car Extra",
-- CorrectionOrderItem: "Correction",



SELECT DISTINCT
       coi_elements.value:order_item_type::VARCHAR      AS order_item_type,
       coi_elements.value:main_order_item_type::VARCHAR AS main_order_item_type
FROM data_vault_mvp.travelbird_cms.orders_orderevent_snapshot oo,
     LATERAL FLATTEN(INPUT => PARSE_JSON(oo.event_data):created_orderitems, OUTER => TRUE) coi_elements





