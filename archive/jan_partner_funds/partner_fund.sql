SELECT ml.transactionid,
       ml.currency,
       ml.territory,
       ml.commissionexvat,
--        bs.commission_ex_vat_gbp,
       ml.totalsellrateincurrency,
       ml.grossbookingvalueincurrency,
       ml.flashgrosscommissioninsuppliercurrency,
--        bs.record__o['commissionExVatInSupplierCurrency']::INT/100,
       ml.saleid,
       s.base_currency,--currency of supplier via the sale
       bs.record__o,
       CASE
           WHEN LEFT(ml.transaction_id, 1) = 'A'
               THEN 'A' || REGEXP_SUBSTR(ml.transaction_id, '-.*-(.*)', 1, 1, 'e')
           ELSE REGEXP_SUBSTR(ml.transaction_id, '-.*-(.*)', 1, 1, 'e') END                 AS booking_id,
       (bs.record__o['commissionExVatInSupplierCurrency'] / 100) / bs.commission_ex_vat_gbp AS gbp_to_supplier_fx --to get fx,
FROM collab.covid_pii.covid_master_list_ho_packages ml
         LEFT JOIN collab.covid_pii.mongo_booking_summary bs ON
        IFF(LEFT(ml.transaction_id, 1) = 'A', 'A' || REGEXP_SUBSTR(ml.transaction_id, '-.*-(.*)', 1, 1, 'e'),
            REGEXP_SUBSTR(ml.transaction_id, '-.*-(.*)', 1, 1, 'e')) =
        bs.booking_id
         LEFT JOIN se.data.se_sale_attributes s ON ml.saleid = s.sale_id
WHERE ml.checkin >= '2020-03-17'
  AND ml.checkin <= '2020-06-30'
  AND ml.datebooked < '2020-05-01'
  AND upper(ml.type) = 'HOTEL'
  AND lower(ml.dynamicflightbooked) = 'n'
  AND "VIEW" IN ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 DACH Parked View**',
                 '**COVID-19 UK/US and INTL Parked View**',
                 '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Social Media View**', '**Restrictions View**')
  AND company IN ('Sofitel Bangkok Sukhumvit');


SELECT * FROM collab.covid_pii.mongo_booking_summary mbs;


GRANT SELECT ON VIEW collab.covid_pii.mongo_booking_summary TO ROLE personal_role__janhitzke;
GRANT SELECT ON VIEW collab.covid_pii.mongo_booking_summary TO ROLE personal_role__gianniraftis;

SELECT *
FROM data_vault_mvp.cms_report_snapshots.booking_summary;

SELECT *
FROM data_vault_mvp.cms_mysql_snapshots.supplier_snapshot ss;

SELECT base_currency
FROM data_vault_mvp.dwh.se_sale ss;


SELECT bs.currency,
       bs.insuranceincustomercurrency,
       bs.insuranceinsuppliercurrency
FROM hygiene_snapshot_vault_mvp.cms_reports.booking_summary bs;


--ndm currency is in base_sale base_currency
--odm supplier base_currency

SELECT * FROM data_vault_mvp.dwh.se_booking sb;
SELECT * FROM se.data.se_booking sb;


------------------------------------------------------------------------------------------------------------------------
--jan's code

select ml.COMPANY,
       ml.TRANSACTIONID,
       ml.CUSTOMERNAME,
       ml.CUSTOMEREMAIL,
       ml.SALEID,
       ml.DATEBOOKED,
       ml.CHECKIN,
       ml.CHECKOUT,
       ml.CANCELLED,
       ml.REFUNDED,
       ml.CURRENCY,
       mon.RATE_TO_GBP,
       mon.RECORD__O['saleBaseCurrency'],
       mon.RECORD__O['commissionExVatInSupplierCurrency']::INT / 100,
       mon.RECORD__O['grossBookingValueInSupplierCurrency']::INT / 100
FROM collab.covid_pii.mongo_booking_summary mon
         LEFT JOIN collab.covid_pii.covid_master_list_ho_packages ml ON mon.booking_id = ml.dwh_booking_id
WHERE ml."VIEW" in ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 DACH Parked View**',
                    '**COVID-19 UK/US and INTL Parked View**',
                    '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Social Media View**', '**Restrictions View**')
  AND lower(ml.TYPE) = 'hotel'
  AND ml.CHECKIN >= '2020-03-17'
  AND DATE_TRUNC('day', ml.DATEBOOKED) <= '2020-05-01'
  AND lower(ml.DYNAMICFLIGHTBOOKED) = 'n'

------------------------------------------------------------------------------------------------------------------------
--jan's code edited
select ml.company,
       ml.transactionid,
       ml.customername,
       ml.customeremail,
       ml.saleid,
       ml.datebooked,
       ml.checkin,
       ml.checkout,
       ml.cancelled,
       ml.refunded,
       ml.currency,
       sb.cc_rate_to_gbp,
       sb.sale_base_currency,
       sb.commission_ex_vat_sc,
       sb.gross_booking_value_sc
FROM data_vault_mvp.dwh.se_booking sb
         LEFT JOIN collab.covid_pii.covid_master_list_ho_packages ml ON sb.booking_id = ml.dwh_booking_id
WHERE ml."VIEW" in ('**COVID-19 DACH P1/P2 Refusal View**', '**COVID-19 DACH Parked View**',
                    '**COVID-19 UK/US and INTL Parked View**',
                    '**COVID-19 UK&INTL P1/P2 Refusal View**', '**Social Media View**', '**Restrictions View**')
  AND LOWER(ml.TYPE) = 'hotel'
  AND ml.CHECKIN >= '2020-03-17'
  AND DATE_TRUNC('day', ml.DATEBOOKED) <= '2020-05-01'
  AND LOWER(ml.DYNAMICFLIGHTBOOKED) = 'n';