SELECT origin_airport_id                                AS origin,
       destination_airport_id                           AS destination,
       open_jaw_airport_id                              AS openjaw,
       departure_datetime::DATE                         AS outbound,
       return_datetime::DATE                            AS inbound,
       currency_code                                    AS currency,
       date_modified,
       adult_price + payment_fee / 2 + supplier_fee / 2 AS total_1adt

FROM data_vault_mvp.travelbird_cms.flights_flightproduct_snapshot
WHERE departure_datetime::DATE > CURRENT_DATE()::DATE
  AND return_datetime IS NOT NULL
  AND active = 1
  AND released = 1
  AND date_modified::DATE >= dateadd(DAY, -10, CURRENT_DATE())
    QUALIFY row_number()
                    OVER (PARTITION BY origin,
                        destination,
                        openjaw,
                        outbound,
                        inbound,
                        currency ORDER BY date_modified DESC) = 1
