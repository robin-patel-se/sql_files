
SELECT *
FROM data_vault_mvp.dwh.se_offers_inclusions_rates soir
WHERE soir.salesforce_hotel_name = 'Finca Hotel Can Canals & Spa'
  AND soir.allocation_date >= '2022-01-01'
ORDER BY rate_plan_rack_code, allocation_date
;

self_describing_task --include 'dv/dwh/fornova/se_offers_inclusions_rates.py'  --method 'run' --start '2022-02-16 00:00:00' --end '2022-02-16 00:00:00'


SELECT hotel_code,
       salesforce_opportunity_id,
       soir.hotel_rate_rack_code,
       rate_plan_rack_code,
       allocation_date,
       min_length_of_stay,
       rate_type,
       rate_local_calculated,
       rate_local
FROM data_vault_mvp.dwh.se_offers_inclusions_rates soir
WHERE soir.salesforce_hotel_name = 'Finca Hotel Can Canals & Spa'
  AND soir.allocation_date >= '2022-01-01'
ORDER BY rate_plan_rack_code, allocation_date
;

self_describing_task --include 'dv/bi/scv/customer_yearly_first_session.py'  --method 'run' --start '2022-02-17 00:00:00' --end '2022-02-17 00:00:00'



            WITH rolling_rates AS (
                SELECT
                    al.hotel_code,
                    al.salesforce_opportunity_id,
                    al.rate_code,
                    al.date,
                    al.min_length_of_stay,
                    al.rate,
                       LISTAGG(rc.date, ', '),
                    SUM(rc.rate) AS rolling_rate,
                    COUNT(rc.date) AS rate_days
                FROM data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step02__rate_plan_rooms_and_rates_blow_out al
                    LEFT JOIN data_vault_mvp_dev_robin.dwh.se_offers_inclusions_rates__step02__rate_plan_rooms_and_rates_blow_out rc
                            ON al.hotel_code = rc.hotel_code
                            AND al.rate_code = rc.rate_code
                            AND rc.date BETWEEN  al.date AND DATEADD(DAY, al.min_length_of_stay-1, al.date)
                            AND al.salesforce_opportunity_id = rc.salesforce_opportunity_id
                WHERE al.rate_type = 'PER_NIGHT'
                AND al.salesforce_opportunity_id = '0061r00001IneUl'
                GROUP BY 1, 2, 3, 4, 5, 6
            )
            SELECT
                rr.hotel_code,
                rr.salesforce_opportunity_id,
                rr.rate_code,
                rr.date,
                rr.min_length_of_stay,
                IFF(rr.min_length_of_stay = rr.rate_days,
                    rr.rolling_rate,
                    rr.rate * rr.min_length_of_stay
                ) AS rate
            FROM rolling_rates rr