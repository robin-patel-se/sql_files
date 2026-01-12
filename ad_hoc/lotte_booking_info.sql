WITH order_item_agg AS (
    SELECT toi.order_id,
           MIN(IFF(toi.order_item_type = 'FLIGHT', toi.start_date, NULL))                      AS flight_departure,
           MAX(IFF(toi.order_item_type = 'FLIGHT', toi.end_date, NULL))                        AS flight_arrival,
           SUM(IFF(toi.order_item_type = 'ACCOMMODATION', toi.sold_price_incl_vat_eur, 0)) AS accommodation_sold_price_inc_vat_eur,
           SUM(IFF(toi.order_item_type = 'ACCOMMODATION', toi.cost_price_excl_vat_eur, 0)) AS accommodation_cost_price_excl_vat_eur,
           COUNT(DISTINCT IFF(toi.order_item_type = 'ACCOMMODATION', toi.order_item_id, NULL)) AS rooms_booked,
           MIN(IFF(toi.order_item_type IS DISTINCT FROM 'FLIGHT', toi.start_date, NULL))       AS arrival_date,
           MAX(IFF(toi.order_item_type IS DISTINCT FROM 'FLIGHT', toi.end_date, NULL))         AS departure_date
    FROM se.data.tb_order_item toi
    GROUP BY 1
)

SELECT tb.order_id,
       oia.flight_departure,
       oia.flight_arrival,
       oia.accommodation_sold_price_inc_vat_eur,
       oia.accommodation_cost_price_excl_vat_eur,
       oia.rooms_booked,
       sua.first_name || ' ' || sua.surname AS member_name,
       oia.arrival_date,
       oia.departure_date,
       tb.adult_guests,
       tb.child_guests,
       tb.infant_guests
FROM se.data.tb_booking tb
         LEFT JOIN order_item_agg oia ON tb.order_id = oia.order_id
         LEFT JOIN se.data_pii.se_user_attributes sua ON tb.shiro_user_id = sua.shiro_user_id
WHERE tb.offer_id = 117011
  AND tb.payment_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE');

SELECT * FROm se.data.tb_offer t