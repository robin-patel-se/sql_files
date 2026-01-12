-- Out of all upcoming AOHO bookings, how many % are tied to an offer that is no longer live on the website?

SELECT offer_active,
       COUNT(*)
FROM se.data.se_booking sb
    INNER JOIN se.data.se_offer_attributes soa ON sb.offer_id = soa.se_offer_id
    INNER JOIN se.data.se_sale_attributes ssa ON sb.se_sale_id = ssa.se_sale_id
WHERE sb.check_in_date > CURRENT_DATE -- upcoming booking
AND ssa.product_configuration = 'Hotel'
GROUP BY 1;


SELECT *
FROM se.data.se_offer_attributes soa;