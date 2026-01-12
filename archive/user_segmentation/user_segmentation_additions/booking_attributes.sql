--se bookings

SELECT fcb.shiro_user_id,
       DATEDIFF(DAY, fcb.check_in_date, fcb.check_out_date) AS nights,
       fcb.margin_gross_of_toms_gbp,
       fcb.margin_gross_of_toms_gbp / NULLIF(nights, 0)     AS ppn

FROM se.data.fact_complete_booking fcb
;



SELECT *
FROM data_vault_mvp.dwh.tb_booking tb;


CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking COPY GRANTS CLONE hygiene_snapshot_vault_mvp.cms_mysql.booking;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation COPY GRANTS CLONE hygiene_snapshot_vault_mvp.cms_mysql.reservation;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary COPY GRANTS CLONE hygiene_snapshot_vault_mvp.cms_mongodb.booking_summary;

SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.booking; --2020-02-28 09:57:08.705000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mysql.reservation; --2020-02-28 09:58:13.350000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.cms_mongodb.booking_summary; --2020-02-28 09:57:17.145000000
DROP TABLE data_vault_mvp_dev_robin.dwh.se_booking;
self_describing_task --include 'dv/dwh/transactional/se_booking'  --method 'run' --start '2020-02-27 00:00:00' --end '2020-02-27 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking sb
WHERE sb.booking_status = 'COMPLETE';

--run in production
CREATE OR REPLACE TABLE data_vault_mvp.dwh.se_booking COPY GRANTS CLONE data_vault_mvp_dev_robin.dwh.se_booking;
ALTER TABLE data_vault_mvp_dev_robin.dwh.se_booking
    RENAME TO se_booking_new;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.se_booking
(
    schedule_tstamp                TIMESTAMPNTZ,
    run_tstamp                     TIMESTAMPNTZ,
    operation_id                   VARCHAR,
    created_at                     TIMESTAMPNTZ,
    updated_at                     TIMESTAMPNTZ,
    booking_id                     VARCHAR NOT NULL,
    transaction_id                 VARCHAR,
    unique_transaction_reference   VARCHAR,
    last_updated                   TIMESTAMPNTZ,
    last_updated_booking_summary   TIMESTAMPNTZ,
    last_updated_bookings          TIMESTAMPNTZ,
    last_updated_reservations      TIMESTAMPNTZ,
    territory                      VARCHAR,
    booking_status                 VARCHAR,
    currency                       VARCHAR,
    booking_completed_date         DATE,
    booking_created_date           DATE,
    booking_completed_timestamp    TIMESTAMPNTZ,
    booking_created_timestamp      TIMESTAMPNTZ,
    shiro_user_id                  NUMBER,
    affiliate_user_id              NUMBER,
    device_platform                VARCHAR,
    rate_to_gbp                    DOUBLE,
    gross_booking_value_cc         DOUBLE,
    vat_on_commission_cc           DOUBLE,
    commission_ex_vat_cc           DOUBLE,
    booking_fee_net_rate_cc        DOUBLE,
    payment_surcharge_net_rate_cc  DOUBLE,
    insurance_commission_cc        DOUBLE,
    flight_commission_cc           DOUBLE,
    gross_booking_value_gbp        DOUBLE,
    vat_on_commission_gbp          DOUBLE,
    commission_ex_vat_gbp          DOUBLE,
    booking_fee_net_rate_gbp       DOUBLE,
    payment_surcharge_net_rate_gbp DOUBLE,
    insurance_commission_gbp       DOUBLE,
    flight_commission_gbp          DOUBLE,
    margin_gross_of_toms_gbp       DOUBLE,
    sale_id                        VARCHAR,
    offer_id                       VARCHAR,
    bundle_id                      VARCHAR,
    check_in_timestamp             TIMESTAMPNTZ,
    check_in_date                  DATE,
    check_out_timestamp            TIMESTAMPNTZ,
    check_out_date                 DATE,
    booking_lead_time_days         NUMBER,
    booking_type                   VARCHAR,
    no_nights                      NUMBER,
    adult_guests                   NUMBER,
    child_guests                   NUMBER,
    infant_guests                  NUMBER,
    sale_type                      VARCHAR,
    has_flights                    VARCHAR,
    price_per_night                FLOAT,
    price_per_person_per_night     FLOAT,
    is_new_model_booking           NUMBER,
    affiliate_id                   VARCHAR,
    affiliate                      VARCHAR,
    affiliate_domain               VARCHAR,
    agent_id                       VARCHAR,
    payment_id                     NUMBER,
    hold_id                        NUMBER
);

INSERT INTO data_vault_mvp_dev_robin.dwh.se_booking
SELECT b.schedule_tstamp,
       b.run_tstamp,
       b.operation_id,
       b.created_at,
       b.updated_at,
       b.booking_id,
       b.transaction_id,
       b.unique_transaction_reference,
       b.last_updated,
       b.last_updated_booking_summary,
       b.last_updated_bookings,
       b.last_updated_reservations,
       b.territory,
       b.booking_status,
       b.currency,
       b.booking_completed_date,
       b.booking_created_date,
       b.booking_completed_timestamp,
       b.booking_created_timestamp,
       b.shiro_user_id,
       b.affiliate_user_id,
       b.device_platform,
       b.rate_to_gbp,
       b.gross_booking_value_cc,
       b.vat_on_commission_cc,
       b.commission_ex_vat_cc,
       b.booking_fee_net_rate_cc,
       b.payment_surcharge_net_rate_cc,
       b.insurance_commission_cc,
       b.flight_commission_cc,
       b.gross_booking_value_gbp,
       b.vat_on_commission_gbp,
       b.commission_ex_vat_gbp,
       b.booking_fee_net_rate_gbp,
       b.payment_surcharge_net_rate_gbp,
       b.insurance_commission_gbp,
       b.flight_commission_gbp,
       b.margin_gross_of_toms_gbp,
       b.sale_id,
       b.offer_id,
       b.bundle_id,
       b.check_in_timestamp,
       b.check_in_date,
       b.check_out_timestamp,
       b.check_out_date,
       b.booking_lead_time_days,
       b.booking_type,
       b.no_nights,
       b.adult_guests,
       b.child_guests,
       b.infant_guests,
       b.sale_type,
       b.has_flights,
       b.gross_booking_value_gbp / NULLIF(b.no_nights, 0) AS price_per_night,
       b.gross_booking_value_gbp
           / NULLIF(b.adult_guests + b.child_guests + b.infant_guests, 0)
           / NULLIF(b.no_nights, 0)                       AS price_per_person_per_night,
       b.is_new_model_booking,
       b.affiliate_id,
       b.affiliate,
       b.affiliate_domain,
       b.agent_id,
       b.payment_id,
       b.hold_id
FROM data_vault_mvp.dwh.se_booking b;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.se_booking sb;

SELECT COUNT(*) FROM data_vault_mvp.dwh.se_booking sb;
SELECT COUNT(*) FROM data_vault_mvp_dev_robin.dwh.se_booking sb;

CREATE OR REPLACE TABLE data_vault_mvp.dwh.se_booking COPY GRANTS CLONE data_vault_mvp_dev_robin.dwh.se_booking;


------------------------------------------------------------------------------------------------------------------------
--tb orders
SELECT ops.order_id,
       count(*)                                                                                                  AS travellers,
       SUM(CASE WHEN DATEDIFF(YEAR, ops.birth_date, oos.travel_date) >= 18 OR ops.birth_date IS NULL THEN 1 END) AS adult_guests,
       COALESCE(SUM(CASE
                        WHEN DATEDIFF(YEAR, ops.birth_date, oos.travel_date) < 18
                            AND DATEDIFF(YEAR, ops.birth_date, oos.travel_date) > 2
                            THEN 1 END), 0)                                                                      AS child_guests,
       COALESCE(SUM(CASE WHEN DATEDIFF(YEAR, ops.birth_date, oos.travel_date) < 2 THEN 1 END), 0)                AS infant_guests


FROM data_vault_mvp.travelbird_cms.orders_order_snapshot oos
         LEFT JOIN data_vault_mvp.travelbird_cms.orders_person_snapshot ops ON oos.id = ops.order_id
GROUP BY 1;

CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order COPY GRANTS CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_order;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer COPY GRANTS CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.offers_offer;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderitembase COPY GRANTS CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.orders_orderitembase;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.django_content_type COPY GRANTS CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.django_content_type;
CREATE OR REPLACE TABLE hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.currency_exchangerateupdate COPY GRANTS CLONE hygiene_snapshot_vault_mvp.travelbird_mysql.currency_exchangerateupdate;

CREATE SCHEMA data_vault_mvp_dev_robin.travelbird_cms;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.travelbird_cms.orders_person_snapshot CLONE data_vault_mvp.travelbird_cms.orders_person_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.travelbird_cms.orders_orderproperty_snapshot CLONE data_vault_mvp.travelbird_cms.orders_orderproperty_snapshot;
CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.external_booking_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.external_booking_snapshot;

SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_order; --2020-02-28 09:56:48.908000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.offers_offer; --2020-02-28 09:59:56.568000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.orders_orderitembase; --2020-02-28 09:59:54.955000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.django_content_type; --2020-06-02 01:03:15.969000000
SELECT MIN(updated_at)
FROM hygiene_snapshot_vault_mvp_dev_robin.travelbird_mysql.currency_exchangerateupdate; --2020-02-28 09:58:42.940000000


DROP TABLE data_vault_mvp_dev_robin.dwh.tb_booking;
self_describing_task --include 'dv/dwh/transactional/tb_booking'  --method 'run' --start '2020-02-27 00:00:00' --end '2020-02-27 00:00:00'

SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb;

ALTER TABLE data_vault_mvp_dev_robin.dwh.tb_booking
    RENAME TO tb_booking_new;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.tb_booking
(

    -- (lineage) metadata for the current job
    schedule_tstamp            TIMESTAMP,
    run_tstamp                 TIMESTAMP,
    operation_id               VARCHAR,
    created_at                 TIMESTAMP,
    updated_at                 TIMESTAMP,

    id                         NUMBER NOT NULL,
    created_at_dts             TIMESTAMP,
    updated_at_dts             TIMESTAMP,
    session_validity           TIMESTAMP,
    payment_status             VARCHAR,
    order_status               VARCHAR,
    manual_order_status        NUMBER,
    payment_reference          VARCHAR,
    payment_method_from_adyen  VARCHAR,
    redeem_on                  VARCHAR,
    complete_date              TIMESTAMP,
    reference_id               VARCHAR,
    token                      VARCHAR,
    token_expiration           TIMESTAMP,
    token_type                 NUMBER,
    customer_ip_address        VARCHAR,
    request_country            VARCHAR,
    comments                   VARCHAR,
    internal_comments          VARCHAR,
    partner_mail_sent          NUMBER,
    utm_source                 VARCHAR,
    utm_medium                 VARCHAR,
    utm_campaign               VARCHAR,
    utm_term                   VARCHAR,
    utm_content                VARCHAR,
    platform                   VARCHAR,
    user_agent                 VARCHAR,
    processed                  NUMBER,
    tracking_pixel_shown       NUMBER,
    missing_data               NUMBER,
    warning_count              NUMBER,
    partner_partial_mail_sent  NUMBER,
    buyer_id                   NUMBER,
    customer_id                NUMBER,
    offer_id                   NUMBER,
    offer_date_id              NUMBER,
    payment_method_id          NUMBER,
    site_id                    NUMBER,
    travel_date                DATE,
    return_date                DATE,
    booking_lead_time_days     NUMBER,
    sold_price_total_eur       FLOAT,
    cost_price_total_eur       FLOAT,
    booking_fee_vat_eur        FLOAT,
    booking_fee_incl_vat_eur   FLOAT,
    sold_price_total_gbp       FLOAT,
    cost_price_total_gbp       FLOAT,
    booking_fee_vat_gbp        FLOAT,
    booking_fee_incl_vat_gbp   FLOAT,
    margin_eur                 FLOAT,
    margin_gbp                 FLOAT,
    se_sale_id                 VARCHAR,
    adult_guests               INT,
    child_guests               INT,
    infant_guests              INT,
    no_nights                  INT,
    price_per_night            FLOAT, --gbp
    price_per_person_per_night FLOAT  --gbp
);

INSERT INTO data_vault_mvp_dev_robin.dwh.tb_booking
WITH passengers AS (
    SELECT o.id                                                                                                AS order_id,
           SUM(CASE WHEN DATEDIFF(YEAR, p.birth_date, o.travel_date) >= 18 OR p.birth_date IS NULL THEN 1 END) AS adult_guests,
           COALESCE(SUM(CASE
                            WHEN DATEDIFF(YEAR, p.birth_date, o.travel_date) < 18
                                AND DATEDIFF(YEAR, p.birth_date, o.travel_date) > 2
                                THEN 1 END), 0)                                                                AS child_guests,
           COALESCE(SUM(CASE WHEN DATEDIFF(YEAR, p.birth_date, o.travel_date) < 2 THEN 1 END), 0)              AS infant_guests
    FROM data_vault_mvp.travelbird_cms.orders_order_snapshot o
             LEFT JOIN data_vault_mvp.travelbird_cms.orders_person_snapshot p ON o.id = p.order_id
    GROUP BY 1
)

SELECT b.schedule_tstamp,
       b.run_tstamp,
       b.operation_id,
       b.created_at,
       b.updated_at,
       b.id,
       b.created_at_dts,
       b.updated_at_dts,
       b.session_validity,
       b.payment_status,
       b.order_status,
       b.manual_order_status,
       b.payment_reference,
       b.payment_method_from_adyen,
       b.redeem_on,
       b.complete_date,
       b.reference_id,
       b.token,
       b.token_expiration,
       b.token_type,
       b.customer_ip_address,
       b.request_country,
       b.comments,
       b.internal_comments,
       b.partner_mail_sent,
       b.utm_source,
       b.utm_medium,
       b.utm_campaign,
       b.utm_term,
       b.utm_content,
       b.platform,
       b.user_agent,
       b.processed,
       b.tracking_pixel_shown,
       b.missing_data,
       b.warning_count,
       b.partner_partial_mail_sent,
       b.buyer_id,
       b.customer_id,
       b.offer_id,
       b.offer_date_id,
       b.payment_method_id,
       b.site_id,
       b.travel_date,
       b.return_date,
       b.booking_lead_time_days,
       b.sold_price_total_eur,
       b.cost_price_total_eur,
       b.booking_fee_vat_eur,
       b.booking_fee_incl_vat_eur,
       b.sold_price_total_gbp,
       b.cost_price_total_gbp,
       b.booking_fee_vat_gbp,
       b.booking_fee_incl_vat_gbp,
       b.margin_eur,
       b.margin_gbp,
       b.se_sale_id,
       p.adult_guests,
       p.child_guests,
       p.infant_guests,
       DATEDIFF(DAY, b.travel_date, b.return_date) AS no_nights,
       b.sold_price_total_gbp
           / NULLIF(no_nights, 0)                  AS price_per_night,
       b.sold_price_total_gbp
           / NULLIF(p.adult_guests + p.child_guests + p.infant_guests, 0)
           / NULLIF(no_nights, 0)                  AS price_per_person_per_night

FROM data_vault_mvp.dwh.tb_booking b
         LEFT JOIN passengers p ON b.id = p.order_id;

SELECT *
FROM data_vault_mvp_dev_robin.dwh.tb_booking tb;

SELECT COUNT(*) FROM data_vault_mvp.dwh.tb_booking tb;
SELECT COUNT(*) FROM data_vault_mvp_dev_robin.dwh.tb_booking tb;

CREATE OR REPLACE TABLE data_vault_mvp.dwh.tb_booking COPY GRANTS CLONE data_vault_mvp.dwh.tb_booking;

------------------------------------------------------------------------------------------------------------------------
--view

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.fact_complete_booking AS (
    WITH tbu AS (
        SELECT order_id,
               value::BIGINT AS shiro_user_id
        FROM data_vault_mvp.travelbird_cms.orders_orderproperty_snapshot
        WHERE namespace = 'se'
          AND name = 'user_id'
    )
    SELECT b.booking_id,
           b.booking_status,
           b.sale_id,
           b.shiro_user_id,

           b.check_in_date,
           b.check_out_date,
           b.booking_lead_time_days,
           b.booking_created_date,
           b.booking_completed_date,

           b.gross_booking_value_gbp,
           b.commission_ex_vat_gbp,
           b.booking_fee_net_rate_gbp,
           b.payment_surcharge_net_rate_gbp,
           b.insurance_commission_gbp,

           b.margin_gross_of_toms_gbp,
           b.no_nights,
           b.adult_guests,
           b.child_guests,
           b.infant_guests,
           b.gross_booking_value_gbp / NULLIF(b.no_nights, 0) AS price_per_night,
           'SECRET_ESCAPES'                                   AS tech_platform

    FROM data_vault_mvp_dev_robin.dwh.se_booking b
    WHERE booking_status IN ('COMPLETE')

    UNION ALL

    SELECT CONCAT('TB-', tbb.id)                                                              AS booking_id,
           tbb.payment_status                                                                 AS booking_status,
           tbb.se_sale_id                                                                     AS sale_id,
           COALESCE(tbu.shiro_user_id, ebs.user_id)                                           AS shiro_user_id,

           tbb.travel_date                                                                    AS check_in_date,
           tbb.return_date                                                                    AS check_out_date,
           tbb.booking_lead_time_days,
           tbb.created_at_dts::DATE                                                           AS booking_created_date,
           tbb.complete_date                                                                  AS booking_completed_date,

           tbb.sold_price_total_gbp                                                           AS gross_booking_value_gbp,
           tbb.sold_price_total_gbp - tbb.cost_price_total_gbp - tbb.booking_fee_incl_vat_gbp AS commission_ex_vat_gbp,
           tbb.booking_fee_incl_vat_gbp - tbb.booking_fee_vat_gbp                             AS booking_fee_net_rate_gbp,
           0                                                                                  AS payment_surcharge_net_rate_gbp,
           0                                                                                  AS insurance_commission_gbp,

           tbb.margin_gbp                                                                     AS margin_gross_of_toms_gbp,

           tbb.adult_guests,
           tbb.child_guests,
           tbb.infant_guests,
           tbb.no_nights                                                                      AS no_nights,
           gross_booking_value_gbp / NULLIF(no_nights, 0)                                     AS price_per_night,
           'TRAVELBIRD'                                                                       AS tech_platform

    FROM data_vault_mvp_dev_robin.dwh.tb_booking tbb
             LEFT JOIN tbu
                       ON tbb.id = tbu.order_id
             LEFT JOIN data_vault_mvp.cms_mysql_snapshots.external_booking_snapshot ebs
                       ON tbb.id = ebs.external_id
    WHERE booking_status IN ('AUTHORISED', 'PARTIAL_PAID', 'LATE')
);

self_describing_task --include 'se/data/fact_complete_booking'  --method 'run' --start '2020-06-01 00:00:00' --end '2020-06-01 00:00:00'

SELECT *
FROM se_dev_robin.data.fact_complete_booking;

self_describing_task --include 'dv/dwh/events/05_touch_channelling/01_module_touch_utm_referrer'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT * FROM se.data.fact_complete_booking fcb;