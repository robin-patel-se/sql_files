SELECT ssa.salesforce_opportunity_id,
       COUNT(*) AS active_offers
FROM se.data.se_sale_attributes ssa
    INNER JOIN se.data.se_hotel_sale_offer shso ON ssa.se_sale_id = shso.sale_id
    INNER JOIN se.data.se_offer_attributes soa ON 'A' || shso.offer_id = soa.se_offer_id AND soa.offer_active
WHERE ssa.sale_active
GROUP BY 1;
