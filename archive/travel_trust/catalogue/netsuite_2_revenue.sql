GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_revenue_report TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_revenue_report TO ROLE personal_role__kirstengrieve;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_revenue_report TO ROLE personal_role__gianniraftis;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_revenue_report TO ROLE personal_role__tanithspinelli;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_revenue_report TO ROLE personal_role__ezraphilips;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_revenue_report TO ROLE personal_role__sebastianmaczka;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_revenue_report TO ROLE personal_role__saurdash;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_revenue_report TO ROLE personal_role__barbarazacchino;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_revenue_report TO ROLE personal_role__ailiemcderment;
GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_revenue_report TO ROLE personal_role__andypauer;

------------------------------------------------------------------------------------------------------------------------
--revenue report
CREATE OR REPLACE VIEW collab.travel_trust.tb_netsuite_revenue_report COPY GRANTS AS
(
WITH flatten_events AS (
    --flatten creation events
    SELECT oo.order_id,
           oo.event_type,
           oo.created_at_dts,
           coi_elements.value                              AS created_order_items,
           coi_elements.value:start_date::VARCHAR          AS start_date,
           coi_elements.value:end_date::VARCHAR            AS end_date,
           coi_elements.value:sold_price_incl_vat::VARCHAR AS sold_price_incl_vat,
           coi_elements.value:vat_percentage::VARCHAR      AS vat_percentage

    FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderevent_snapshot oo,
         LATERAL FLATTEN(INPUT => PARSE_JSON(oo.event_data):created_orderitems, OUTER => TRUE) coi_elements

    WHERE oo.event_type IN ('ORDER_CONFIRMED', 'ORDER_CREATED')
),
     aggregate_events AS (
         --aggregate creation events to order level
         SELECT fe.order_id,
                MIN(IFF(event_type = 'ORDER_CONFIRMED', fe.created_at_dts, NULL))                        AS booking_confirmation_date,
                MIN(IFF(event_type = 'ORDER_CREATED', fe.start_date, NULL))                              AS start_date,
                MAX(IFF(event_type = 'ORDER_CREATED', fe.end_date, NULL))                                AS end_date,
                SUM(IFF(event_type = 'ORDER_CREATED', fe.sold_price_incl_vat, NULL))                     AS sold_price_incl_vat,
                SUM(IFF(event_type = 'ORDER_CREATED', fe.sold_price_incl_vat * fe.vat_percentage, NULL)) AS total_vat_percentage
         FROM flatten_events fe
         GROUP BY 1
     ),
     psp_a AS (
         SELECT op.order_id,
                SUM(op.amount) AS psp_amount
         FROM data_vault_mvp.travelbird_mysql_snapshots.orders_payment_snapshot op
         WHERE op.state IN ('PROCESSING', 'PAID', 'TO_BE_SETTLED', 'SETTLED')
           AND op.polymorphic_ctype_id IN (554, 709)
         GROUP BY 1
     ),
     fees AS (
         SELECT oo.order_id,
                SUM(IFF(ob.orderitembase_ptr_id IS NOT NULL, oo.sold_price_incl_vat, 0)) AS booking_fee,
                SUM(IFF(oa.orderitembase_ptr_id IS NOT NULL, oo.sold_price_incl_vat, 0)) AS bonding_fee
         FROM data_vault_mvp.travelbird_mysql_snapshots.orders_orderitembase_snapshot oo
                  LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_bookingfeeorderitem_snapshot ob
                            ON oo.id = ob.orderitembase_ptr_id
                  LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.orders_atolorderitem_snapshot oa
                            ON oo.id = oa.orderitembase_ptr_id
         GROUP BY 1
     )
SELECT oo.created_at_dts                                  AS booking_date,
       ae.booking_confirmation_date,
       oo.id                                              AS booking_id,
       oof.product_line,
       ps.psp_amount, --incorrect psp amount due to the orders_payment table missing polymorphic ctype filter
       cc.code                                            AS customer_currency,
       oo.discount                                        AS cash_credit_used,
       0                                                  AS non_cash_credit_used,
       CASE
           WHEN cr.id IS NOT NULL THEN 'REFERRAL'
           ELSE 'TB COUPON - ' || coc.code
           END                                            AS type_of_credit,
       se.data.posa_territory_from_tb_site_id(oo.site_id) AS territory,
       ae.sold_price_incl_vat                             AS total_amount,
       ae.total_vat_percentage                            AS total_vat,
       f.booking_fee,
       oof.external_reference                             AS cms_sale_id,
       oo.travel_date                                     AS start_date,
       oo.return_date                                     AS end_date,
       f.bonding_fee

FROM data_vault_mvp.travelbird_mysql_snapshots.orders_order_snapshot oo
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.offers_offer_snapshot oof ON oo.offer_id = oof.id
         LEFT JOIN aggregate_events ae ON oo.id = ae.order_id
         LEFT JOIN psp_a ps ON oo.id = ps.order_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.common_sitesettings_snapshot cs ON oo.site_id = cs.id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.currency_currency_snapshot cc ON cs.site_currency_id = cc.id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.coupons_coupon_order_snapshot cco ON oo.id = cco.order_id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.coupons_coupon_snapshot coc ON cco.coupon_id = coc.id
         LEFT JOIN data_vault_mvp.travelbird_mysql_snapshots.coupons_referralcode_snapshot cr
                   ON coc.id = cr.coupon_id --Table is currently empty so snapshot is just a view on raw vault
         LEFT JOIN fees f ON oo.id = f.order_id
    )
;


GRANT SELECT ON VIEW collab.travel_trust.tb_netsuite_revenue_report TO ROLE personal_role__kirstengrieve;

SELECT *
FROM collab.travel_trust.tb_netsuite_revenue_report
WHERE tb_netsuite_revenue_report.cms_sale_id = 'A8940'

SELECT *
FROM se.data.tb_booking tb
WHERE tb.booking_id = 'TB-21904507';


