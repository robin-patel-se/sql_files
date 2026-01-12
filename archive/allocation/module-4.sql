------------------------------------------------------------------------------------------------------------------------
--module 4

-- The goal is to get to all tables to reservation table grain (i.e. one row per reservation_id / transaction id) - therefore creating view on reservation room confirmation which has sum up total number of adults / children / infants to roll up to reservation_id grain.
WITH sum_reservation_room_confirmation AS (
    SELECT reservation_id,
           SUM(adults)   AS no_of_adults,
           SUM(children) AS no_of_children,
           SUM(infants)  AS no_of_infants
    FROM data_vault_mvp_dev_robin.mari_snapshots.reservation_room_confirmation_snapshot
    GROUP BY reservation_id
)
   , sum_reservation_tax AS (
    SELECT reservation_id,
           SUM(amount) AS total_tax_amount
    FROM data_vault_mvp_dev_robin.mari_snapshots.reservation_tax_snapshot
    GROUP BY reservation_id
)

-- Output query which has all the details on a reservation at a reservation_id level

SELECT res.id::NUMBER AS reservation_id,
       res.date_created,
       res.last_updated,
       res.res_id,
       res.hotel_code,
       res.room_type_code,
       res.start_date,
       res.end_date,
       res.amount_before_tax,
       res.amount_after_tax,
       sum_res_tax.total_tax_amount,
       res.currency,
       res.status,
       sum_res_room.no_of_adults,
       sum_res_room.no_of_children,
       sum_res_room.no_of_infants,
       res_sup_charges.children_amount,
       res_sup_charges.infants_amount,
       res_rate_plan.free_children,
       res_rate_plan.free_infants
FROM data_vault_mvp_dev_robin.mari_snapshots.reservation_snapshot res
         LEFT JOIN sum_reservation_room_confirmation sum_res_room ON sum_res_room.reservation_id = res.id
         LEFT JOIN data_vault_mvp_dev_robin.mari_snapshots.reservation_supplement_charges_snapshot res_sup_charges
                   ON res_sup_charges.reservation_id = res.id
         LEFT JOIN sum_reservation_tax sum_res_tax ON sum_res_tax.reservation_id = res.id
         LEFT JOIN data_vault_mvp_dev_robin.mari_snapshots.reservation_rate_plan_snapshot res_rate_plan
                   ON res_rate_plan.reservation_id = res.id
WHERE res.status = 'CONFIRMED'
ORDER BY res.id
;


SELECT *
FROM se.data.se_user_attributes
WHERE app_cohort_id IS NOT NULL;



SELECT RESERVATION_SNAPSHOT.id::number
FROM data_vault_mvp.mari_snapshots.reservation_snapshot;



