WITH bookings AS (
    SELECT db.transaction_id                                                                             as transaction_id,
           fb.key_date_booked,
--            datepart(week, fb.key_date_booked)    as week,
           fb.derived_exchange_rate,
           dc.divide_by_to_get_constant_currency                                                         as divide_by_to_get_constant_currency, --for constant currency calculation
           dc.currency_code,
           fb.commission_ex_vat,                                                                                                                -- this is in GBP
--            fb.commission_ex_vat / 0.90565           as commission_ex_vat_eur,              -- approx currency rate for non EUR bookings found derived exchange rate to be unreliable

           CASE
               WHEN currency_code = 'EUR' THEN commission_ex_vat * derived_exchange_rate
               ELSE commission_ex_vat / 0.90565 END                                                      as commission_ex_vat_eur,              -- fixed derived exchange rate

           fb.payment_surcharge_net_rate,                                                                                                       -- this is in GBP
           fb.booking_fee_net_rate,                                                                                                             -- this is in GBP
           fb.insurance_net_rate,                                                                                                               -- this is in GBP
           fb.toms_vat,                                                                                                                         -- this is in GBP
           fb.margin,                                                                                                                           -- this is in GBP
           (fb.margin * fb.derived_exchange_rate)
               /
           dc.divide_by_to_get_constant_currency                                                         as margin_constant_currency,           -- constant currency calculation from chiasma views fact_bookings_v.sql
           fb.vat_on_booking_fee,                                                                                                               -- this is in GBP TB currently deduct this from commission ex vat


           fb.margin_gross_of_toms,                                                                                                             -- this is in GBP
           CASE
               WHEN currency_code = 'EUR' THEN margin_gross_of_toms * derived_exchange_rate
               ELSE margin_gross_of_toms / 0.90565 END                                                   as margin_gross_of_toms_eur,
           commission_ex_vat
               + payment_surcharge_net_rate
               + booking_fee_net_rate
               + insurance_net_rate
               -
           toms_vat                                                                                      as margin_calc_sensecheck,             -- this is in GBP
           CASE
               WHEN currency_code = 'EUR' THEN margin * derived_exchange_rate
               ELSE margin / 0.90565 END                                                                 as margin_eur,                         -- approx currency rate for non EUR bookings RP: found derived exchange rate to be unreliable
           dst.sale_type,                                                                                                                       -- used to identify/filter catalogue bookings
           ds.status,                                                                                                                           -- used to filter in cube output
           bu.territory_branch,
           dss.country,
           d.supplier,

           -- derived exchange rate is computed in cube via: coalesce((total_sell_rate_in_currency / nullif(total_sell_rate,0))  , (gross_booking_value_in_currency / nullif(gross_booking_value,0) ) )
           fb.total_sell_rate_in_currency,
           fb.total_sell_rate,
           fb.gross_booking_value_in_currency,
           fb.gross_booking_value


    FROM dim_bookings AS db
             LEFT JOIN fact_bookings AS fb ON fb.key_booking = db.key_booking
             LEFT JOIN dim_currencies AS dc ON fb.key_currency = dc.key_currency
             LEFT JOIN dim_sales AS dss ON fb.key_sale = dss.key_sale -- for sale country (toms vat)
             LEFT JOIN dim_sale_types dst on fb.key_sale_type = dst.key_sale_type -- for filter sale_type (toms vat)
             LEFT JOIN dim_status ds on db.key_status = ds.key_status -- for filter on cancelled
             LEFT JOIN business_units bu
                       on fb.key_current_business_unit_id = bu.business_unit_id -- for territory (toms vat)
             LEFT JOIN dim_suppliers d on fb.key_supplier = d.key_supplier -- for supplier name (toms vat)

    WHERE
    db.transaction_id IN ('TB-21872289', 'TB-21872662', 'TB-21872969') AND
        db.transaction_id LIKE 'TB-%'
      AND fb.key_date_booked BETWEEN '2019-08-16' AND '2019-09-15'
      AND status = 'Booked'
--     ORDER BY fb.key_date_booked, fb.key_time_booked
)

SELECT key_date_booked,
--     week,
       sum(margin)               AS margin,
       sum(margin_gross_of_toms) AS margin_gross_of_toms,
       sum(commission_ex_vat)    AS commission_ex_vat,
       sum(booking_fee_net_rate) AS booking_fee_net_rate,
       sum(insurance_net_rate)   AS insurance_net_rate,
       sum(toms_vat)             AS toms_vat

FROM bookings
group by key_date_booked
;



-- --carmens
--
-- select
-- dim_bookings.transaction_id,
--     fact_bookings.key_date_booked,
--        fact_bookings.derived_exchange_rate,
--    dim_currencies.currency_code,
--     fact_bookings.commission_ex_vat, -- this is in GBP
--        fact_bookings.commission_ex_vat * derived_exchange_rate AS commission_ex_vat_eur,
--     fact_bookings.payment_surcharge_net_rate, -- this is in GBP
--     fact_bookings.booking_fee_net_rate, -- this is in GBP
--     fact_bookings.toms_vat, -- this is in GBP
--     fact_bookings.margin_gross_of_toms, -- this is in GBP
--        fact_bookings.margin_gross_of_toms * derived_exchange_rate AS margin_gross_of_toms_eur, -- this is in GBP
--     fact_bookings.margin, -- this is in GBP
--     commission_ex_vat
--         + payment_surcharge_net_rate
--         + booking_fee_net_rate
--         + insurance_net_rate
--         - toms_vat as margin_calc_sensecheck -- this is in GBP
-- ​
-- from dim_bookings
-- left join fact_bookings on fact_bookings.key_booking = dim_bookings.key_booking
-- left join dim_currencies on fact_bookings.key_currency = dim_currencies.key_currency
-- where --dim_bookings.transaction_id = 'TB-21871834' AND
-- key_date_booked >= '2019-08-26' and key_date_booked <= '2019-09-26'
-- and dim_bookings.transaction_id LIKE 'TB-%'
-- order by key_date_booked, key_time_booked;
--