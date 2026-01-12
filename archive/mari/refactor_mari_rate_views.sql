SELECT ssa.se_sale_id,
       ssa.salesforce_opportunity_id,
       ssa.posa_territory,
       sst.tag_name
FROM se.data.se_sale_attributes ssa
         LEFT JOIN se.data.se_sale_tags sst ON ssa.se_sale_id = sst.se_sale_id AND sst.tag_name LIKE '%_NoJetlore'
WHERE ssa.salesforce_opportunity_id = '0061r00001HRpOX';

CREATE TABLE scratch.robinpatel.se_hotel_room_availability AS (
    SELECT shra.mari_hotel_id,
           shra.cms_hotel_id,
           shra.hotel_name,
           shra.hotel_code,
           shra.room_type_id,
           shra.room_type_name,
           shra.room_type_code,
           shra.inventory_date,
           shra.inventory_day,
           shra.no_total_rooms,
           shra.no_available_rooms,
           shra.no_booked_rooms,
           shra.no_closedout_rooms
    FROM se.data.se_hotel_room_availability shra
);

CREATE TABLE robinpatel.se_hotel_room_availability
(
    mari_hotel_id      NUMBER,
    cms_hotel_id       NUMBER,
    hotel_name         VARCHAR,
    hotel_code         VARCHAR,
    room_type_id       NUMBER,
    room_type_name     VARCHAR,
    room_type_code     VARCHAR,
    inventory_date     DATE,
    inventory_day      VARCHAR,
    no_total_rooms     NUMBER,
    no_available_rooms NUMBER,
    no_booked_rooms    NUMBER,
    no_closedout_rooms NUMBER
);

CREATE OR REPLACE SCHEMA data_vault_mvp_dev_robin.mari_snapshots CLONE data_vault_mvp.mari_snapshots;

self_describing_task --include 'dv/dwh/mari/hotel_room_availability.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/mari/se_hotel_room_availability.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'dv/dwh/mari/hotel_room_inventory_snapshot.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/mari/se_hotel_availability_original.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'



SELECT *
FROM data_vault_mvp_dev_robin.dwh.hotel_room_availability;

SELECT min(view_date)
FROM scratch.robinpatel.se_hotel_room_availability shra;


------------------------------------------------------------------------------------------------------------------------
CREATE TABLE scratch.robinpatel.se_room_rates AS (
    SELECT srr.hotel_code,
           srr.hotel_name,
           srr.room_type_id,
           srr.rate_plan_id,
           srr.rate_plan_name,
           srr.rate_plan_code,
           srr.rate_plan_rack_code,
           srr.rate_plan_code_rack_code,
           srr.free_children,
           srr.free_infants,
           srr.cash_to_settle_commission,
           srr.rate_id,
           srr.date,
           srr.rate_gbp,
           srr.rack_rate_gbp,
           srr.single_rate_gbp,
           srr.child_rate_gbp,
           srr.infant_rate_gbp,
           srr.rc_to_gbp,
           srr.rate_eur,
           srr.rack_rate_eur,
           srr.single_rate_eur,
           srr.child_rate_eur,
           srr.infant_rate_eur,
           srr.rc_to_eur,
           srr.currency,
           srr.rate_rc,
           srr.rack_rate_rc,
           srr.single_rate_rc,
           srr.child_rate_rc,
           srr.infant_rate_rc,
           srr.min_length_of_stay,
           srr.max_length_of_stay,
           srr.cash_to_settle_rate_id,
           srr.cts_rate,
           srr.cts_single_rate,
           srr.cts_infant_rate,
           srr.cts_child_rate,
           srr.discount_precentage
    FROM se.data.se_room_rates srr
);

CREATE TABLE robinpatel.se_room_rates
(
    hotel_code                VARCHAR,
    hotel_name                VARCHAR,
    room_type_id              NUMBER,
    rate_plan_id              NUMBER,
    rate_plan_name            VARCHAR,
    rate_plan_code            VARCHAR,
    rate_plan_rack_code       VARCHAR,
    rate_plan_code_rack_code  VARCHAR,
    free_children             NUMBER,
    free_infants              NUMBER,
    cash_to_settle_commission NUMBER,
    rate_id                   NUMBER,
    date                      DATE,
    rate_gbp                  DOUBLE,
    rack_rate_gbp             DOUBLE,
    single_rate_gbp           DOUBLE,
    child_rate_gbp            DOUBLE,
    infant_rate_gbp           DOUBLE,
    rc_to_gbp                 DOUBLE,
    rate_eur                  DOUBLE,
    rack_rate_eur             DOUBLE,
    single_rate_eur           DOUBLE,
    child_rate_eur            DOUBLE,
    infant_rate_eur           DOUBLE,
    rc_to_eur                 DOUBLE,
    currency                  VARCHAR,
    rate_rc                   DOUBLE,
    rack_rate_rc              DOUBLE,
    single_rate_rc            DOUBLE,
    child_rate_rc             DOUBLE,
    infant_rate_rc            DOUBLE,
    min_length_of_stay        NUMBER,
    max_length_of_stay        NUMBER,
    cash_to_settle_rate_id    NUMBER,
    cts_rate                  DOUBLE,
    cts_single_rate           DOUBLE,
    cts_infant_rate           DOUBLE,
    cts_child_rate            DOUBLE,
    discount_precentage       DOUBLE
);


SELECT *
FROM data_vault_mvp.mari_snapshots.rate_snapshot rs;


self_describing_task --include 'dv/dwh/mari/room_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/mari/se_room_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'



------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE scratch.robinpatel.se_room_type_rooms_and_rates AS
SELECT srtrar.room_type_id,
       srtrar.room_type_name,
       srtrar.hotel_code,
       srtrar.hotel_name,
       srtrar.rate_date,
       srtrar.rate_currency,
       srtrar.lead_rate_plan_name,
       srtrar.lead_rate_plan_code,
       srtrar.rt_lead_rate_gbp,
       srtrar.rt_lead_rate_eur,
       srtrar.rt_lead_rate_rc,
       srtrar.rt_avg_rack_rate_gbp,
       srtrar.rt_avg_rack_rate_eur,
       srtrar.rt_avg_rack_rate_rc,
       srtrar.rt_avg_discount_percentage,
       srtrar.rt_top_discount_percentage,
       srtrar.rt_no_rates,
       srtrar.rt_no_total_rooms,
       srtrar.rt_no_available_rooms,
       srtrar.rt_no_booked_rooms,
       srtrar.rt_no_closedout_rooms,
       srtrar.rt_available_lead_rate_gbp,
       srtrar.rt_available_lead_rate_eur,
       srtrar.rt_available_lead_rate_rc,
       srtrar.rt_available_lead_rate_rooms
FROM se.data.se_room_type_rooms_and_rates srtrar;

SELECT get_ddl('table', 'scratch.robinpatel.se_room_type_rooms_and_rates');

CREATE OR REPLACE TABLE se_room_type_rooms_and_rates
(
    room_type_id                 NUMBER,
    room_type_name               VARCHAR,
    hotel_code                   VARCHAR,
    hotel_name                   VARCHAR,
    rate_date                    DATE,
    rate_currency                VARCHAR,
    lead_rate_plan_name          VARCHAR,
    lead_rate_plan_code          VARCHAR,
    rt_lead_rate_gbp             FLOAT,
    rt_lead_rate_eur             FLOAT,
    rt_lead_rate_rc              FLOAT,
    rt_avg_rack_rate_gbp         FLOAT,
    rt_avg_rack_rate_eur         FLOAT,
    rt_avg_rack_rate_rc          FLOAT,
    rt_avg_discount_percentage   FLOAT,
    rt_top_discount_percentage   FLOAT,
    rt_no_rates                  NUMBER,
    rt_no_total_rooms            NUMBER,
    rt_no_available_rooms        NUMBER,
    rt_no_booked_rooms           NUMBER,
    rt_no_closedout_rooms        NUMBER,
    rt_available_lead_rate_gbp   FLOAT,
    rt_available_lead_rate_eur   FLOAT,
    rt_available_lead_rate_rc    FLOAT,
    rt_available_lead_rate_rooms NUMBER
);


self_describing_task --include 'dv/dwh/mari/room_type_rooms_and_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/mari/se_room_type_rooms_and_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

CREATE OR REPLACE TABLE scratch.robinpatel.se_hotel_rooms_and_rates AS
SELECT *
FROM se.data.se_hotel_rooms_and_rates shrar;

SELECT get_ddl('table', 'scratch.robinpatel.se_hotel_rooms_and_rates');

CREATE OR REPLACE TABLE se_hotel_rooms_and_rates
(
    hotel_code                         VARCHAR,
    hotel_name                         VARCHAR,
    date                               DATE,
    day_name                           VARCHAR,
    no_total_rooms                     NUMBER,
    no_available_rooms                 NUMBER,
    no_booked_rooms                    NUMBER,
    no_closedout_rooms                 NUMBER,
    no_rates                           NUMBER,
    rate_currency                      VARCHAR,
    avail_weighted_discount_percentage FLOAT,
    average_discount_percentage        FLOAT,
    top_discount_percentage            FLOAT,
    avail_weighted_rack_rate_gbp       FLOAT,
    avail_weighted_rack_rate_eur       FLOAT,
    avail_weighted_rack_rate_rc        FLOAT,
    lead_rate_room_type_name           VARCHAR,
    lead_rate_plan_name                VARCHAR,
    lead_rate_plan_code                VARCHAR,
    lead_rate_gbp                      FLOAT,
    lead_rate_eur                      FLOAT,
    lead_rate_rc                       FLOAT,
    lead_rate_rooms                    NUMBER,
    percent_rooms_at_lead_rate         NUMBER,
    available_lead_rate_room_type_name VARCHAR,
    available_lead_rate_plan_name      VARCHAR,
    available_lead_rate_plan_code      VARCHAR,
    available_lead_rate_gbp            FLOAT,
    available_lead_rate_eur            FLOAT,
    available_lead_rate_rc             FLOAT,
    available_lead_rate_rooms          NUMBER
);


self_describing_task --include 'dv/dwh/mari/hotel_rooms_and_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/mari/se_hotel_rooms_and_rates.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'


------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE scratch.robinpatel.se_reservation_information AS
SELECT *
FROM se.data.se_reservation_information sri;


SELECT get_ddl('table', 'scratch.robinpatel.se_reservation_information');

CREATE OR REPLACE TABLE se_reservation_information
(
    reservation_id    NUMBER,
    date_created      TIMESTAMP_NTZ,
    last_updated      TIMESTAMP_NTZ,
    res_id            VARCHAR,
    hotel_code        VARCHAR,
    room_type_code    VARCHAR,
    start_date        DATE,
    end_date          DATE,
    amount_before_tax FLOAT,
    amount_after_tax  FLOAT,
    total_tax_amount  FLOAT,
    currency          VARCHAR,
    status            VARCHAR,
    no_of_adults      NUMBER,
    no_of_children    NUMBER,
    no_of_infants     NUMBER,
    children_amount   FLOAT,
    infants_amount    FLOAT,
    free_children     NUMBER,
    free_infants      NUMBER
);

self_describing_task --include 'dv/dwh/mari/reservation_information.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/mari/se_reservation_information.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'



self_describing_task --include 'dv/dwh/mari/hotel_rooms_and_rates_snapshot.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT min(HOTEL_ROOMS_AND_RATES_SNAPSHOT.view_date)
FROM data_vault_mvp_dev_robin.dwh.hotel_rooms_and_rates_snapshot;

SELECT *
FROM se.data.fact_complete_booking fcb


self_describing_task --include 'se/data/mari/se_hotel_rooms_and_rates_snapshot.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
self_describing_task --include 'se/data/mari/se_hotel_availability_snapshot.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'

SELECT * FROM se_dev_robin.data.se_hotel_rooms_and_rates_snapshot;
SELECT * FROM se_dev_robin.data.se_hotel_room_availability_snapshot;

self_describing_task --include 'se/data/dwh/fact_complete_booking.py'  --method 'run' --start '2020-04-13 00:00:00' --end '2020-04-13 00:00:00'
