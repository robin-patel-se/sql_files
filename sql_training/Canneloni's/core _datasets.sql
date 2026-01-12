SELECT *
FROM se.data_pii.se_user_attributes sua
WHERE first_name IS NOT NULL
LIMIT 2;

SELECT fcb.tech_platform,
       COUNT(*)
FROM se.data.fact_complete_booking fcb
GROUP BY 1;




------------------------------------------------------------------------------------------------------------------------
