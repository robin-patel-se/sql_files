-- age is years are all 2 so I am using days instead
select sum("age_<857_days") "age_<857_days",
sum("age_<887_days") "age_<887_days",
sum("age_<917_days") "age_<917_days",
sum("age_<947_days") "age_<947_days"
from (
SELECT (now()::date - extracted_at::date) AS age_days,
(now()::date - extracted_at::date)/365 AS age_years,
-- 827-922
CASE WHEN (now()::date - extracted_at::date) <857
	THEN 1 ELSE 0 END AS "age_<857_days",
CASE WHEN (now()::date - extracted_at::date) <887 and (now()::date - extracted_at::date) >=857
	THEN 1 ELSE 0 END AS "age_<887_days",
CASE WHEN (now()::date - extracted_at::date) <917 and (now()::date - extracted_at::date) >=887
	THEN 1 ELSE 0 END AS "age_<917_days",
CASE WHEN (now()::date - extracted_at::date) <947 and (now()::date - extracted_at::date) >=917
	THEN 1 ELSE 0 END AS "age_<947_days",
*
-- 827-922
FROM david_pell.members
  INNER JOIN (
    SELECT member_id, MAX(extracted_at) AS maxsign FROM david_pell.members GROUP BY member_id
  ) most_recent ON members.member_ID = most_recent.member_ID
  and members.extracted_at = most_recent.maxsign

) as member_ages