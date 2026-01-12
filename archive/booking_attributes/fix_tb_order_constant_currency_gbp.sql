CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderitembase CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderitembase;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.django_content_type CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.django_content_type;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.currency_exchangerateupdate CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.currency_exchangerateupdate;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order;
CREATE OR REPLACE TRANSIENT TABLE hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency CLONE hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.travelbird_cms.currency_currency_snapshot CLONE data_vault_mvp.travelbird_cms.currency_currency_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.external_booking_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.external_booking_snapshot;


self_describing_task --include 'dv/dwh/transactional/tb_booking.py'  --method 'run' --start '2020-11-19 00:00:00' --end '2020-11-19 00:00:00'

SELECT

    /*
        Creating dataset of financials. SE margin uses booking fee excl VAT so
        need to pull out the booking fee component to take VAT off to compare TB to SE.
    */


    oo.order_id,
    c.code                                                                                AS currency,
    --Getting date here for use later in determining whether to merge / insert / updated
    max(oo.updated_at_dts)                                                                AS updated_at_dts,
    --Duplicated calculation for eur/gbp rather than nesting / multiple joins to do it in steps
    SUM(oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate)                      AS sold_price_total_eur,
    SUM(oo.cost_price_excl_vat * oo.cost_price_to_eur_exchange_rate)                      AS cost_price_total_eur,
    SUM(IFF(dct.model = 'bookingfeeorderitem', oo.sold_price_vat * oo.sold_price_to_eur_exchange_rate,
            0))                                                                           AS booking_fee_vat_eur,
    SUM(IFF(dct.model = 'bookingfeeorderitem', oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate,
            0))                                                                           AS booking_fee_incl_vat_eur,

    SUM(IFF(c.code = 'GBP', oo.sold_price_incl_vat,
            oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate * fx.rate))       AS sold_price_total_gbp,
    SUM(IFF(c.code = 'GBP', oo.cost_price_excl_vat,
            oo.cost_price_excl_vat * oo.cost_price_to_eur_exchange_rate * fx.rate))       AS cost_price_total_gbp,
    SUM(IFF(dct.model = 'bookingfeeorderitem',
            IFF(c.code = 'GBP',
                (oo.sold_price_incl_vat - (oo.sold_price_incl_vat / (1 + oo.sold_price_vat_percentage))),
                (oo.sold_price_incl_vat - (oo.sold_price_incl_vat / (1 + oo.sold_price_vat_percentage))) *
                oo.sold_price_to_eur_exchange_rate * fx.rate
                ),
            0))                                                                           AS booking_fee_vat_gbp,
    SUM(IFF(dct.model = 'bookingfeeorderitem',
            IFF(c.code = 'GBP',
                oo.sold_price_incl_vat,
                oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate * fx.rate)
            0))                                                                           AS booking_fee_incl_vat_gbp,

    SUM(IFF(c.code = 'GBP', oo.sold_price_incl_vat,
            oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate * cc.multiplier)) AS sold_price_total_gbp_constant_currency,
    SUM(IFF(c.code = 'GBP', oo.cost_price_excl_vat,
            oo.cost_price_excl_vat * oo.cost_price_to_eur_exchange_rate * cc.multiplier)) AS cost_price_total_gbp_constant_currency,
    SUM(IFF(dct.model = 'bookingfeeorderitem',
            IFF(c.code = 'GBP',
                (oo.sold_price_incl_vat - (oo.sold_price_incl_vat / (1 + oo.sold_price_vat_percentage))),
                (oo.sold_price_incl_vat - (oo.sold_price_incl_vat / (1 + oo.sold_price_vat_percentage))) *
                oo.sold_price_to_eur_exchange_rate * cc.multiplier
                ),
            0))                                                                           AS booking_fee_vat_gbp_constant_currency,
    SUM(IFF(dct.model = 'bookingfeeorderitem',
            IFF(c.code = 'GBP',
                oo.sold_price_incl_vat,
                oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate * cc.multiplier),
            0))                                                                           AS booking_fee_incl_vat_gbp_constant_currency,

    sold_price_total_eur - cost_price_total_eur - booking_fee_vat_eur                     AS margin_eur,
    sold_price_total_gbp - cost_price_total_gbp - booking_fee_vat_gbp                     AS margin_gbp,
    sold_price_total_gbp_constant_currency - cost_price_total_gbp_constant_currency -
    booking_fee_vat_gbp_constant_currency                                                 AS margin_gbp_constant_currency

FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order o
         INNER JOIN hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderitembase oo ON o.id = oo.order_id
         INNER JOIN hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.django_content_type dct
                    ON oo.polymorphic_ctype_id = dct.id
         INNER JOIN (
    --this should already be deduped. Using group / aggregate here to add extra layer of error catching
    SELECT usage_date, avg(rate) AS rate
    FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.currency_exchangerateupdate
    WHERE source_id = 1
      AND target_id = 2 --EUR->GBP
    GROUP BY 1
) fx ON oo.created_at_dts::DATE = fx.usage_date
         LEFT JOIN data_vault_mvp_dev_robin.travelbird_cms.currency_currency_snapshot c ON oo.sold_price_currency_id = c.id
         LEFT JOIN hygiene_snapshot_vault_mvp_dev_robin.fpa_gsheets.constant_currency cc ON
        (CURRENT_DATE) >= cc.start_date AND
        (CURRENT_DATE) <= cc.end_date AND
        cc.currency = 'GBP' AND
        cc.category = 'Primary' AND
        --order items might have differing currencies, TB have a conversion to EUR,
        --so convert currency to EUR and then from EUR to GBP using constant currency
        cc.base_currency = 'EUR'
GROUP BY 1, 2;



SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
WHERE tb.booking_id = 'TB-21895981'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
WHERE tb.sold_price_currency = 'GBP'
  AND tb.margin_gbp_constant_currency != tb.margin_gbp

SELECT tb.sold_price_currency,
       SUM(tb.margin_gbp),
       SUM(tb.margin_gbp_constant_currency),
       SUM(tb.margin_eur)

FROM data_vault_mvp.dwh.tb_booking tb
GROUP BY 1;

SELECT tb.sold_price_currency,
       SUM(tb.margin_gbp),
       SUM(tb.margin_gbp_constant_currency),
       SUM(tb.margin_eur)

FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
GROUP BY 1;


SELECT tb.booking_id,
       tb.margin_gbp,
       tb.margin_gbp_constant_currency
FROM data_vault_mvp.dwh.tb_booking tb
WHERE booking_id IN (
                     'TB-21895965',
                     'TB-21895979',
                     'TB-21895981',
                     'TB-21869547',
                     'TB-21869614',
                     'TB-21869775')


SELECT tb.booking_id,
       tb.margin_gbp,
       tb.margin_gbp_constant_currency
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
WHERE booking_id IN (
                     'TB-21895965',
                     'TB-21895979',
                     'TB-21895981',
                     'TB-21869547',
                     'TB-21869614',
                     'TB-21869775')

SELECT tb.booking_id,
       tb.sold_price_total_gbp,
       tb.cost_price_total_gbp,
       tb.booking_fee_vat_gbp,
       tb.margin_gbp,
       oo.sold_price_incl_vat,
       oo.cost_price_excl_vat,
       oo.sold_price_currency_id,
       ccs.code AS sold_price_currency,
       cpc.code AS cost_price_currency,
       oo.cost_price_to_eur_exchange_rate
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
         INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderitembase oo ON tb.id = oo.order_id
         INNER JOIN data_vault_mvp.travelbird_cms.currency_currency_snapshot ccs ON oo.sold_price_currency_id = ccs.id
         INNER JOIN data_vault_mvp.travelbird_cms.currency_currency_snapshot cpc ON oo.cost_price_currency_id = cpc.id
WHERE tb.booking_id = 'TB-21895981';


SELECT tb.booking_id,
       tb.sold_price_total_gbp,
       tb.cost_price_total_gbp,
       tb.booking_fee_vat_gbp,
       tb.margin_gbp,
       oo.sold_price_incl_vat,
       oo.cost_price_excl_vat,
       oo.sold_price_vat,
       oo.cost_price_to_eur_exchange_rate
FROM data_vault_mvp.dwh.tb_booking tb
         INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderitembase oo ON tb.id = oo.order_id
WHERE tb.booking_id = 'TB-21895981';


SELECT tb.booking_id,
       c.code,
       oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate                      AS sold_price_total_eur,
       oo.sold_price_incl_vat,
       oo.cost_price_excl_vat * oo.cost_price_to_eur_exchange_rate                      AS cost_price_total_eur,
       oo.cost_price_excl_vat,
       oo.sold_price_to_eur_exchange_rate,
       oo.cost_price_to_eur_exchange_rate,

       IFF(dct.model = 'bookingfeeorderitem', oo.sold_price_vat * oo.sold_price_to_eur_exchange_rate,
           0)                                                                           AS booking_fee_vat_eur,
       IFF(dct.model = 'bookingfeeorderitem', oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate,
           0)                                                                           AS booking_fee_incl_vat_eur,

       IFF(c.code = 'GBP', oo.sold_price_incl_vat,
           oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate * fx.rate)       AS sold_price_total_gbp,
       IFF(c.code = 'GBP', oo.cost_price_excl_vat,
           oo.cost_price_excl_vat * oo.cost_price_to_eur_exchange_rate * fx.rate)       AS cost_price_total_gbp,
       IFF(dct.model = 'bookingfeeorderitem',
           IFF(c.code = 'GBP',
               (oo.sold_price_incl_vat - (oo.sold_price_incl_vat / (1 + oo.sold_price_vat_percentage))),
               (oo.sold_price_incl_vat - (oo.sold_price_incl_vat / (1 + oo.sold_price_vat_percentage))) *
               oo.sold_price_to_eur_exchange_rate * fx.rate
               ),
           0)                                                                           AS booking_fee_vat_gbp,
       IFF(dct.model = 'bookingfeeorderitem',
           IFF(c.code = 'GBP',
               oo.sold_price_incl_vat,
               oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate * fx.rate),
           0)                                                                           AS booking_fee_incl_vat_gbp,

       IFF(c.code = 'GBP', oo.sold_price_incl_vat,
           oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate * cc.multiplier) AS sold_price_total_gbp_constant_currency,
       IFF(c.code = 'GBP', oo.cost_price_excl_vat,
           oo.cost_price_excl_vat * oo.cost_price_to_eur_exchange_rate * cc.multiplier) AS cost_price_total_gbp_constant_currency,
       IFF(dct.model = 'bookingfeeorderitem',
           IFF(c.code = 'GBP',
               (oo.sold_price_incl_vat - (oo.sold_price_incl_vat / (1 + oo.sold_price_vat_percentage))),
               (oo.sold_price_incl_vat - (oo.sold_price_incl_vat / (1 + oo.sold_price_vat_percentage))) *
               oo.sold_price_to_eur_exchange_rate * cc.multiplier
               ),
           0)                                                                           AS booking_fee_vat_gbp_constant_currency,
       IFF(dct.model = 'bookingfeeorderitem',
           IFF(c.code = 'GBP',
               oo.sold_price_incl_vat,
               oo.sold_price_incl_vat * oo.sold_price_to_eur_exchange_rate * cc.multiplier),
           0)                                                                           AS booking_fee_incl_vat_gbp_constant_currency,

       sold_price_total_eur - cost_price_total_eur - booking_fee_vat_eur                AS margin_eur,
       sold_price_total_gbp - cost_price_total_gbp - booking_fee_vat_gbp                AS margin_gbp,
       sold_price_total_gbp_constant_currency - cost_price_total_gbp_constant_currency -
       booking_fee_vat_gbp_constant_currency                                            AS margin_gbp_constant_currency
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb
         INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderitembase oo
                    ON tb.id = oo.order_id
         INNER JOIN data_vault_mvp.travelbird_cms.currency_currency_snapshot c ON oo.sold_price_currency_id = c.id
         INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.django_content_type dct ON oo.polymorphic_ctype_id = dct.id
         INNER JOIN (
    --this should already be deduped. Using group / aggregate here to add extra layer of error catching
    SELECT usage_date, avg(rate) AS rate
    FROM hygiene_snapshot_vault_mvp.travelbird_mysql.currency_exchangerateupdate
    WHERE source_id = 1
      AND target_id = 2 --EUR->GBP
    GROUP BY 1
) fx ON oo.created_at_dts::DATE = fx.usage_date
         LEFT JOIN hygiene_snapshot_vault_mvp.fpa_gsheets.constant_currency cc ON
        (CURRENT_DATE) >= cc.start_date AND
        (CURRENT_DATE) <= cc.end_date AND
        cc.currency = 'GBP' AND
        cc.category = 'Primary' AND
        --order items might have differing currencies, TB have a conversion to EUR,
        --so convert currency to EUR and then from EUR to GBP using constant currency
        cc.base_currency = 'EUR'
WHERE tb.booking_id = 'TB-21895979';



SELECT *
FROM se.data.master_tb_booking_list mtbl
WHERE mtbl.booking_id = 'TB-21895981';



------------------------------------------------------------------------------------------------------------------------


SELECT tb.booking_id,
       tb.sold_price_total_gbp,
       tb.cost_price_total_gbp,
       tb.booking_fee_vat_gbp,
       tb.margin_gbp,
       oo.sold_price_incl_vat,
       oo.cost_price_excl_vat,
       oo.sold_price_currency_id,
       ccs.code AS sold_price_currency,
       cpc.code AS cost_price_currency,
       oo.sold_price_to_eur_exchange_rate,
       oo.cost_price_to_eur_exchange_rate
FROM data_vault_mvp.dwh.tb_booking tb
         INNER JOIN hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderitembase oo ON tb.id = oo.order_id
         INNER JOIN data_vault_mvp.travelbird_cms.currency_currency_snapshot ccs ON oo.sold_price_currency_id = ccs.id
         INNER JOIN data_vault_mvp.travelbird_cms.currency_currency_snapshot cpc ON oo.cost_price_currency_id = cpc.id
WHERE tb.booking_id = 'TB-21895988';

