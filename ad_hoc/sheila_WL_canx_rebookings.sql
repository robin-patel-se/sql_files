SELECT *
FROM collab.covid_pii.covid_master_list_ho_packages bs
         INNER JOIN se.data.se_booking sb ON sb.transaction_id = bs.transactionid
WHERE bs.checkin >= '2020-03-01'
  AND bs.checkin <= '2020-06-30'
  AND sb.affiliate_user_id IS NOT NULL
  AND (bs.cancelled
    OR bs.refunded
    OR bs.adjusted_check_in_date IS NOT NULL);

--

SELECT *
FROM