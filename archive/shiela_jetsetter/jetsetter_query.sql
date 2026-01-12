SELECT au.id                           AS affiliate_user_id,
       a.name                          AS affiliate,
       au.email,
       count(DISTINCT b.booking_id)    AS bookings,
       sum(b.margin_gross_of_toms_gbp) AS margin
FROM data_vault_mvp.cms_mysql_snapshots.affiliate_user_snapshot au
         LEFT JOIN data_vault_mvp.cms_mysql_snapshots.affiliate_snapshot a ON au.affiliate_id = a.id
         INNER JOIN data_vault_mvp.dwh.se_booking b ON au.id = b.affiliate_user_id AND b.booking_status = 'COMPLETE'
WHERE a.id IN (1440, 1439) --jetsetter uk and us
GROUP BY 1, 2, 3;