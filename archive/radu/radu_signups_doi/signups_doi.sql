SELECT * FROM se.data_pii.se_user_attributes sua;
SELECT * FROM se.data.se_user_attributes sua;

SELECT sua.original_affiliate_name,
       count(*)
FROM se.data.se_user_attributes sua
GROUP BY 1