WITH sebooking AS (
    SELECT b.booking_id,
           b.territory,
           b.currency,
           b.booking_completed_date,
           b.check_in_date,
           b.se_sale_id,
           b.gross_revenue_sc,
           b.customer_total_price_sc,
           b.margin_gross_of_toms_sc,
           b.booking_fee_sc,
           b.payment_surcharge_sc,
           b.insurance_commission_sc,
           b.atol_fee_sc,
           b.flight_buy_rate_sc,
           b.sale_base_currency,
           b.has_flights,
           b.supplier_name,
           b.sale_base_currency,
           sb.company_id
    FROM se.data.se_booking AS b
        LEFT JOIN se.data.se_sale_attributes AS sb ON sb.se_sale_id = b.se_sale_id
    WHERE b.check_in_date > CURRENT_DATE()
      AND b.booking_completed_date IS NOT NULL
)
SELECT se.*,
       ca.vcc_enabled,
       ca.company_id::integer AS company_id
FROM sebooking AS se
    INNER JOIN se.data.se_company_attributes AS ca ON se.company_id::VARCHAR = ca.company_id::VARCHAR