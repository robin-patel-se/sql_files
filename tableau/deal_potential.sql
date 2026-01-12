SELECT view_date,
       salesforce_opportunity_id,
       date,
       SUM(room_remaining_potential_margin_gbp) AS deal_remaining_potential_margin_gbp,
       SUM(room_total_potential_margin_gbp)     AS deal_total_potential_margin_gbp,
       SUM(room_sell_through)                   AS deal_total_sold_margin_gbp
FROM (
         SELECT view_date,
                salesforce_opportunity_id,
                room_type_code,
                date,
                MAX(room_offer_remaining_potential_margin_gbp) AS room_remaining_potential_margin_gbp,
                MAX(room_offer_total_potential_margin_gbp)     AS room_total_potential_margin_gbp,
                SUM(room_offer_sell_through)                   AS room_sell_through
         FROM (
                  SELECT rr.view_date,
                         s.salesforce_opportunity_id,
                         hso.hotel_offer_id                                                 AS offer_id,
                         rr.room_type_code,
                         rr.date,
                         rr.available * rr.rate * (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
                         CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_remaining_potential_margin_gbp,
                         (rr.available + rr.booked_any_offer) * rr.rate *
                         (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
                         CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_total_potential_margin_gbp,
                         rr.booked_this_offer * rr.rate * (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
                         CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_sell_through
                  FROM data_vault_mvp.dwh.se_sale s
                           INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hso
                                      ON s.se_sale_id = hso.hotel_sale_id
                           INNER JOIN data_vault_mvp.dwh.cms_mari_link mari ON hso.hotel_offer_id = mari.offer_id
                           INNER JOIN data_vault_mvp.cms_mysql_snapshots.offer_snapshot o ON hso.hotel_offer_id = o.id
                           INNER JOIN data_vault_mvp.dwh.hotel_offer_rooms_and_rates_snapshot rr
                                      ON mari.hotel_code = rr.hotel_code AND hso.hotel_offer_id = rr.offer_id
                           LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency fx ON
                          rr.currency = fx.base_currency AND
                          CURRENT_DATE >= fx.start_date AND
                          CURRENT_DATE <= fx.end_date AND
                          fx.currency = 'GBP' AND
                          fx.category = 'Primary'
                  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
              ) t1
         GROUP BY 1, 2, 3, 4
     ) t2
GROUP BY 1, 2, 3

------------------------------------------------------------------------------------------------------------------------


SELECT rr.view_date,
       s.salesforce_opportunity_id,
       hso.hotel_offer_id                                                 AS offer_id,
       rr.room_type_code,
       rr.date,
       rr.available * rr.rate * (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
       CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_remaining_potential_margin_gbp,
       (rr.available + rr.booked_any_offer) * rr.rate *
       (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
       CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_total_potential_margin_gbp,
       rr.booked_this_offer * rr.rate * (CASE WHEN rr.currency = 'GBP' THEN 1 ELSE fx.multiplier END) *
       CASE WHEN o.commission > 0 THEN o.commission ELSE s.commission END AS room_offer_sell_through
FROM data_vault_mvp.dwh.se_sale s
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.hotel_sale_offer_snapshot hso
                    ON s.se_sale_id = 'A' || hso.hotel_sale_id
         INNER JOIN data_vault_mvp.dwh.cms_mari_link mari ON hso.hotel_offer_id = mari.offer_id
         INNER JOIN data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot o ON hso.hotel_offer_id = o.id
         INNER JOIN data_vault_mvp.dwh.hotel_offer_rooms_and_rates_snapshot rr
                    ON mari.hotel_code = rr.hotel_code AND hso.hotel_offer_id = rr.offer_id
         LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency fx ON
        rr.currency = fx.base_currency AND
        CURRENT_DATE >= fx.start_date AND
        CURRENT_DATE <= fx.end_date AND
        fx.currency = 'GBP' AND
        fx.category = 'Primary'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;


SELECT *
FROM se.data.se_sale_attributes ssa
         LEFT JOIN se.data.se_hotel_sale_offer shso ON ssa.se_sale_id = shso.sale_id
WHERE shso.sale_id IS NULL
  AND ssa.sale_active;

CREATE OR REPLACE SCHEMA se_dev_robin.bi;

self_describing_task --include 'se/bi/misc.py'  --method 'run' --start '2021-04-08 00:00:00' --end '2021-04-08 00:00:00'

CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.hotel_offer_rooms_and_rates_snapshot CLONE data_vault_mvp.dwh.hotel_offer_rooms_and_rates_snapshot;
CREATE OR REPLACE VIEW data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_snapshot AS
SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot;
DROP TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.base_offer_snapshot;


SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.base_offer_snapshot bos;

SELECT *
FROM se_dev_robin.bi.global_deal_potential_pit;

CREATE OR REPLACE VIEW se_dev_robin.data.se_hotel_offer_rooms_and_rates_snapshot AS
SELECT *
FROM se.data.se_hotel_offer_rooms_and_rates_snapshot;