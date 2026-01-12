# old model
SELECT s.id           as saleId,
       s.top_discount as topDiscount,
       s.start        as startDate,
       s.end          as endDate,
       t.name         as territory_name,
       min(rate)      as leadRate
FROM sale s
         LEFT JOIN offer o ON o.sale_id = s.id
         LEFT JOIN allocation a ON a.offer_id = o.id
         LEFT JOIN sale_territory st on s.id = st.sale_id
         LEFT JOIN territory t on st.territory_id = t.id


WHERE s.active = true -- sale active
  AND s.start <= now()
  AND s.end > now()
  AND o.active = true
  AND a.start >= current_date
group by 1, 2, 3, 4, 5;

# new model
SELECT s.id,
       s.start,
       s.end,
       s.active,
       s.salesforce_opportunity_id,
       t.name
FROM base_sale s
         LEFT JOIN territory t on s.territory_id = t.id
WHERE s.active = true -- sale active
  AND s.start <= now()
  AND s.end > now()

group by 1, 2, 3, 4, 5;

SELECT *
from base_sale
ORDER by last_updated desc;