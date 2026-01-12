WITH dupes AS (SELECT *
FROM data_vault_mvp.dwh.synxis_room_rates
    QUALIFY COUNT(*) OVER (PARTITION BY
        hotel_code,
        rate_date,
        room_code,
        rate_plan_code) > 1)
SELECT COUNT(*) FROM dupes;

--1960 dupes


SELECT
    synxis_room_rates.hotel_code,
    synxis_room_rates.rate_date,
    synxis_room_rates.room_code,
    synxis_room_rates.rate_plan_code,
    synxis_room_rates.schedule_tstamp,
    synxis_room_rates.run_tstamp,
    synxis_room_rates.operation_id,
    synxis_room_rates.created_at,
    synxis_room_rates.updated_at,
    synxis_room_rates.hotel_name,
    synxis_room_rates.room_name,
    synxis_room_rates.rate_plan_rack_code,
    synxis_room_rates.rate_plan_name,
    synxis_room_rates.rate_plan_currency,
    synxis_room_rates.rate_rate_rc,
    synxis_room_rates.rate_rack_rate_rc,
    synxis_room_rates.min_length_of_stay,
    synxis_room_rates.max_length_of_stay,
    synxis_room_rates.rate_required_allocation_end_date,
    synxis_room_rates.rate_closed_to_arrival,
    synxis_room_rates.rate_closed_to_departure,
    synxis_room_rates.discount_percentage,
    synxis_room_rates.rate_gbp,
    synxis_room_rates.rack_rate_gbp,
    synxis_room_rates.rc_to_gbp,
    synxis_room_rates.rate_eur,
    synxis_room_rates.rack_rate_eur,
    synxis_room_rates.rc_to_eur,
    synxis_room_rates.channel_manager_cms_link
FROM data_vault_mvp.dwh.synxis_room_rates
    QUALIFY COUNT(*) OVER (PARTITION BY
        hotel_code,
        rate_date,
        room_code,
        rate_plan_code) > 1


SELECT * FROM se.data.cms_allocation_link cal WHERE cal.channel_manager_cms_link = '6495:SESELL1:SERETAIL1:DZCOB'

SELECT * FROM se.data.cms_allocation_link cal WHERE cal.channel_manager_cms_link = '6495:SESELL1:SERETAIL1:DZCOB';


SELECT * FROM latest_vault.ari.hotel_rates_synxis WHERE hotel_code = 6495;

CREATE SCHEMA latest_vault_dev_robin.ari;

CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.ari.hotel_rates_synxis CLONE latest_vault.ari.hotel_rates_synxis;
CREATE OR REPLACE TRANSIENT TABLE latest_vault_dev_robin.ari.hotel_rates_siteminder CLONE latest_vault.ari.hotel_rates_siteminder;
CREATE OR REPLACE TRANSIENT TABLE data_vault_mvp_dev_robin.fx.rates CLONE data_vault_mvp.fx.rates;


select * from latest_vault.mari.rate_plan where room_type_id = 11542;
