CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.cms_mysql_snapshots.shiro_user_snapshot CLONE data_vault_mvp.cms_mysql_snapshots.shiro_user_snapshot;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_attributes CLONE data_vault_mvp.dwh.user_attributes;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_activity CLONE data_vault_mvp.dwh.user_activity;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_subscription CLONE data_vault_mvp.dwh.user_subscription;


CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.dwh.user_segmentation CLONE data_vault_mvp.dwh.user_segmentation;
ALTER TABLE data_vault_mvp_dev_robin.dwh.user_segmentation
    ADD COLUMN margin_segment VARCHAR;

self_describing_task --include 'dv/dwh/user_attributes/user_segmentation.py'  --method 'run' --start '2021-04-07 00:00:00' --end '2021-04-07 00:00:00';

USE WAREHOUSE pipe_2xlarge;
-- UPDATE data_vault_mvp_dev_robin.dwh.user_segmentation us
-- SET us.margin_segment = CASE
--                             WHEN us.net_margin > 1250 THEN 'Ultra High'
--                             WHEN us.net_margin > 600 THEN 'High'
--                             WHEN us.net_margin > 150 THEN 'Usual'
--                             WHEN us.net_margin > 0 THEN 'Low'
--                             ELSE 'Non Booker'
--     END
-- WHERE us.margin_segment IS NULL;

DROP TABLE data_vault_mvp_dev_robin.dwh.user_segmentation;

CREATE TABLE IF NOT EXISTS data_vault_mvp_dev_robin.dwh.user_segmentation
(

    -- (lineage) metadata for the current job
    schedule_tstamp                      TIMESTAMP,
    run_tstamp                           TIMESTAMP,
    operation_id                         VARCHAR,
    created_at                           TIMESTAMP,
    updated_at                           TIMESTAMP,

    date                                 DATE,
    shiro_user_id                        NUMBER,
    signup_date                          DATE,
    member_recency_status                VARCHAR,

    gross_bookings                       NUMBER,
    cancelled_bookings                   NUMBER,
    net_bookings                         NUMBER,

    gross_family_bookings                NUMBER,

    gross_domestic_bookings              NUMBER,
    cancelled_domestic_bookings          NUMBER,
    net_domestic_bookings                NUMBER,

    gross_international_bookings         NUMBER,
    cancelled_international_bookings     NUMBER,
    net_international_bookings           NUMBER,

    gross_bookings_less_13m              NUMBER,
    cancelled_bookings_less_13m          NUMBER,
    net_bookings_less_13m                NUMBER,

    gross_bookings_more_13m              NUMBER,
    cancelled_bookings_more_13m          NUMBER,
    net_bookings_more_13m                NUMBER,

    booker_segment                       VARCHAR,

    gross_revenue                        DECIMAL(13, 4),
    cancelled_revenue                    DECIMAL(13, 4),
    net_revenue                          DECIMAL(13, 4),

    max_gross_revenue                    DECIMAL(13, 4),
    avg_gross_revenue                    DECIMAL(13, 4),

    gross_margin                         DECIMAL(13, 4),
    cancelled_margin                     DECIMAL(13, 4),
    net_margin                           DECIMAL(13, 4),

    avg_gross_no_nights                  NUMBER,
    max_gross_price_per_night            DECIMAL(13, 4),
    avg_gross_price_per_night            DECIMAL(13, 4),
    max_gross_price_per_person_per_night DECIMAL(13, 4),
    avg_gross_price_per_person_per_night DECIMAL(13, 4),

    net_bookings_bucket                  VARCHAR,
    net_margin_bucket                    VARCHAR,
    travel_type_segment                  VARCHAR,

    subscription_type                    INT,
    opt_in_status                        VARCHAR,

    engagement_segment                   VARCHAR,
    margin_segment                       VARCHAR
)
    CLUSTER BY (date);

INSERT INTO data_vault_mvp_dev_robin.dwh.user_segmentation
SELECT us.schedule_tstamp,
       us.run_tstamp,
       us.operation_id,
       us.created_at,
       us.updated_at,
       us.date,
       us.shiro_user_id,
       us.signup_date,
       us.member_recency_status,
       us.gross_bookings,
       us.cancelled_bookings,
       us.net_bookings,
       us.gross_family_bookings,
       us.gross_domestic_bookings,
       us.cancelled_domestic_bookings,
       us.net_domestic_bookings,
       us.gross_international_bookings,
       us.cancelled_international_bookings,
       us.net_international_bookings,
       us.gross_bookings_less_13m,
       us.cancelled_bookings_less_13m,
       us.net_bookings_less_13m,
       us.gross_bookings_more_13m,
       us.cancelled_bookings_more_13m,
       us.net_bookings_more_13m,
       us.booker_segment,
       us.gross_revenue,
       us.cancelled_revenue,
       us.net_revenue,
       us.max_gross_revenue,
       us.avg_gross_revenue,
       us.gross_margin,
       us.cancelled_margin,
       us.net_margin,
       us.avg_gross_no_nights,
       us.max_gross_price_per_night,
       us.avg_gross_price_per_night,
       us.max_gross_price_per_person_per_night,
       us.avg_gross_price_per_person_per_night,
       us.net_bookings_bucket,
       us.net_margin_bucket,
       us.travel_type_segment,
       us.subscription_type,
       us.opt_in_status,
       us.engagement_segment,
       CASE
           WHEN net_margin > 1250 THEN 'Ultra High'
           WHEN net_margin > 600 THEN 'High'
           WHEN net_margin > 150 THEN 'Usual'
           WHEN net_margin > 0 THEN 'Low'
           ELSE 'Non Booker'
           END AS margin_segment
FROM data_vault_mvp.dwh.user_segmentation us;

SELECT GET_DDL('table', 'data_vault_mvp_dev_robin.dwh.user_segmentation');

INSERT INTO data_vault_mvp_dev_robin.dwh.user_segmentation2
SELECT user_segmentation.schedule_tstamp,
       user_segmentation.run_tstamp,
       user_segmentation.operation_id,
       user_segmentation.created_at,
       user_segmentation.updated_at,
       user_segmentation.date,
       user_segmentation.shiro_user_id,
       user_segmentation.signup_date,
       user_segmentation.member_recency_status,
       user_segmentation.gross_bookings,
       user_segmentation.cancelled_bookings,
       user_segmentation.net_bookings,
       user_segmentation.gross_family_bookings,
       user_segmentation.gross_domestic_bookings,
       user_segmentation.cancelled_domestic_bookings,
       user_segmentation.net_domestic_bookings,
       user_segmentation.gross_international_bookings,
       user_segmentation.cancelled_international_bookings,
       user_segmentation.net_international_bookings,
       user_segmentation.gross_bookings_less_13m,
       user_segmentation.cancelled_bookings_less_13m,
       user_segmentation.net_bookings_less_13m,
       user_segmentation.gross_bookings_more_13m,
       user_segmentation.cancelled_bookings_more_13m,
       user_segmentation.net_bookings_more_13m,
       user_segmentation.booker_segment,
       user_segmentation.gross_revenue,
       user_segmentation.cancelled_revenue,
       user_segmentation.net_revenue,
       user_segmentation.max_gross_revenue,
       user_segmentation.avg_gross_revenue,
       user_segmentation.gross_margin,
       user_segmentation.cancelled_margin,
       user_segmentation.net_margin,
       user_segmentation.avg_gross_no_nights,
       user_segmentation.max_gross_price_per_night,
       user_segmentation.avg_gross_price_per_night,
       user_segmentation.max_gross_price_per_person_per_night,
       user_segmentation.avg_gross_price_per_person_per_night,
       user_segmentation.net_bookings_bucket,
       user_segmentation.net_margin_bucket,
       user_segmentation.travel_type_segment,
       user_segmentation.subscription_type,
       user_segmentation.opt_in_status,
       user_segmentation.engagement_segment,
       user_segmentation.margin_segment
FROM data_vault_mvp_dev_robin.dwh.user_segmentation;

CREATE OR REPLACE TABLE data_vault_mvp_dev_robin.dwh.user_segmentation clone data_vault_mvp_dev_robin.dwh.user_segmentation2;


airflow backfill --start_date '2021-04-07 07:00:00' --end_date '2021-04-07 07:00:00' --task_regex '.*' se_finance_object_creation__daily_at_07h00