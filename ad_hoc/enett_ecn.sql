SET startdate = '2020-10-01'; --first date (including)
SET enddate = '2020-10-31'; --end date (including)
SET rowlimit = 300; --number of rows to split by
SET ecn = 410233; --eg 410233

SELECT evsr.issued_to_ecn,
       transaction_datetime                                   AS settlement_datetime,
       TO_CHAR(evsr.transaction_datetime, 'dd/MM/yyyy')       AS settlement_date,
       transaction_currency                                   AS currency,
       -transaction_amount                                    AS net_settled,
       user_reference_1                                       AS description,
       FLOOR(ROW_NUMBER() OVER
           (PARTITION BY evsr.issued_to_ecn, evsr.transaction_currency
           ORDER BY settlement_date) / $rowlimit) + 1         AS category_description,
       CASE
           WHEN evsr.issued_to_ecn IN (409980, 410228, 410229) THEN evsr.user_reference_1
           WHEN evsr.issued_to_ecn = 410227 THEN evsr.user_reference_4
           WHEN evsr.issued_to_ecn IN (410230, 410231, 410232) THEN
               REGEXP_SUBSTR(evsr.user_reference_1, '-(.*)', 1, 1, 'e') || ' WEBFARE'
           WHEN evsr.issued_to_ecn = 410233 THEN
                   REGEXP_SUBSTR(evsr.user_reference_1, '-(.*)', 1, 1, 'e') || ' ' || se.finance.user_reference_by_ecn(evsr.issued_to_ecn, evsr.user_reference_2)
           END                                                AS user_reference,
       evsr.issued_to_ecn || currency || category_description AS unload_bucket
FROM collab.finance.enett__van_settlement_report evsr
WHERE transaction_datetime::DATE >= $startdate
  AND transaction_datetime::DATE <= $enddate
  AND evsr.issued_to_ecn = $ecn;

