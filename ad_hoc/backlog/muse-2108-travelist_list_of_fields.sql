SELECT sb.booking_completed_date::DATE,
       COUNT(*)
FROM se.data.se_booking sb
WHERE sb.territory IS NULL
  AND sb.booking_status IN ('COMPLETE', 'REFUNDED')
GROUP BY 1;


SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary bs
WHERE bs.booking_id = 'A8758847';


SELECT HEX_DECODE_STRING('4D656574696E677321') AS column1;

SELECT *
FROM hygiene_snapshot_vault_mvp.cms_mysql.rebooking r
    QUALIFY
                amount <> LAG(amount, 1) OVER (PARTITION BY booking_id, order_item_id ORDER BY component_created_dt ASC) OR
                amount_gbp <> LAG(amount_gbp, 1) OVER (PARTITION BY booking_id, order_item_id ORDER BY component_created_dt ASC) OR
                revenue_start_dt <> LAG(revenue_start_dt, 1) OVER (PARTITION BY booking_id, order_item_id ORDER BY component_created_dt ASC) OR
                revenue_end_dt <> LAG(revenue_end_dt, 1) OVER (PARTITION BY booking_id, order_item_id ORDER BY component_created_dt ASC) OR
                supplier_reference <> LAG(supplier_reference, 1) OVER (PARTITION BY booking_id, order_item_id ORDER BY component_created_dt ASC) OR
                LAG(amount, 1) OVER (PARTITION BY booking_id, order_item_id ORDER BY component_created_dt ASC) IS NULL