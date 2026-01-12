use database scratch;
use schema scratch.carmenmardirosdev36082;

drop table scratch.carmenmardirosdev36082.raw_snowplow_mock_events;

create table scratch.carmenmardirosdev36082.raw_snowplow_mock_events (
    screenshot               varchar,
    s_given                  varchar,
    s_when                   varchar,
    s_then                   varchar,
    user_id                  varchar,
    domain_sessionidx        varchar,
    collector_tstamp         varchar,
    event_name               varchar,
    event_nth                int,
    page_url                 varchar,
    page_context             variant,
    product_bundle_context   variant,
    product_context          variant,
    booking_update_event     variant,
    booking_snapshot_context variant,
    user_context             variant,
    optimizely_context       variant,
    source_context           variant,
    comment                  varchar
);

put file:///Users/carmenmardiros/Downloads/snowplow.csv  @%raw_snowplow_mock_events;

copy into scratch.carmenmardirosdev36082.raw_snowplow_mock_events
    file_format = (
    type = csv
    field_delimiter = ','
    skip_header = 1
    field_optionally_enclosed_by = '\"'
    record_delimiter = '\\n'
    );



select *
from scratch.carmenmardirosdev36082.raw_snowplow_mock_events;

create or replace view scratch.carmenmardirosdev36082.flat_product_bundles
as
select user_id,
    domain_sessionidx,
    collector_tstamp,
    event_name,
    event_nth,
       booking_update_event,
    page_context[0]['pagetype']::varchar as page_type,
    page_url,
    product_bundle.value['tech_provider']::varchar AS bundle_tech_provider,
    product_bundle.value['business_provider']::varchar AS bundle_business_provider,
    product_bundle.value['concept']::varchar AS bundle_concept,
    product_bundle.value['id']::varchar AS bundle_id,
    product_bundle.value['line']::varchar AS bundle_line,
    product_bundle.value['type']::varchar AS bundle_type,
    product_bundle.value['name']::varchar AS bundle_name,
    product_bundle.value['start_date']::varchar AS bundle_start_date,
    product_bundle.value['end_date']::varchar AS bundle_end_date,
    product_bundle.value['secret_escapes_sale_id']::varchar AS bundle_secret_escapes_sale_id, -- links to a known SE Sale
    source_context[0]['territory']::varchar as territory,
    user_context[0]['member_type']::varchar AS user_member_type,
    source_context[0]['environment']::varchar as environment,
    source_context[0]['tracking_platform']::varchar as tracking_platform,
    source_context[0]['tech_provider']::varchar as tech_provider,
    source_context[0]['affiliate']::varchar as affiliate,
    comment
from scratch.carmenmardirosdev36082.raw_snowplow_mock_events,
    LATERAL FLATTEN(INPUT => product_bundle_context, outer=>true) product_bundle
order by EVENT_NTH
;


create or replace view scratch.carmenmardirosdev36082.flat_product_bundles_with_children
as
select user_id,
    domain_sessionidx,
    collector_tstamp,
    event_name,
    event_nth,
       booking_update_event,
    page_context[0]['pagetype']::varchar as page_type,
    page_url,
    product_bundle.value['tech_provider']::varchar AS bundle_tech_provider,
    product_bundle.value['business_provider']::varchar AS bundle_business_provider,
    product_bundle.value['concept']::varchar AS bundle_concept,
    product_bundle.value['id']::varchar AS bundle_id,
    product_bundle.value['line']::varchar AS bundle_line,
    product_bundle.value['type']::varchar AS bundle_type,
    product_bundle.value['name']::varchar AS bundle_name,
    product_bundle.value['start_date']::varchar AS bundle_start_date,
    product_bundle.value['end_date']::varchar AS bundle_end_date,
    product_bundle.value['secret_escapes_sale_id']::varchar AS bundle_secret_escapes_sale_id, -- links to a known SE Sale
    child_product_bundle.value['id']::varchar AS child_bundle_id,
    child_product_bundle.value['name']::varchar AS child_bundle_name,
    child_product_bundle.value['concept']::varchar AS child_bundle_concept, -- at SE this is an offer
    child_product_bundle.value['price_per_room_per_night_from']::varchar AS child_bundle_price_per_room_per_night_from,
    child_product_bundle.value['min_no_nights']::varchar AS child_bundle_min_no_nights,
    source_context[0]['territory']::varchar as territory,
    user_context[0]['member_type']::varchar AS user_member_type,
    source_context[0]['environment']::varchar as environment,
    source_context[0]['tracking_platform']::varchar as tracking_platform,
    source_context[0]['tech_provider']::varchar as tech_provider,
    source_context[0]['affiliate']::varchar as affiliate,
    comment
from scratch.carmenmardirosdev36082.raw_snowplow_mock_events,
    LATERAL FLATTEN(INPUT => product_bundle_context, outer=>true) product_bundle,
    LATERAL FLATTEN(INPUT => product_bundle.value['product_bundles'], outer=>true) child_product_bundle
order by EVENT_NTH
;


create or replace view scratch.carmenmardirosdev36082.flat_products
as
select user_id,
    domain_sessionidx,
    collector_tstamp,
    event_name,
    event_nth,
    booking_update_event,
    PAGE_CONTEXT[0]['pageType']::varchar AS page_type,
    page_url,
    product.value['name']::varchar AS product_name,
    product.value['id']::varchar AS product_id,
    product.value['type']::varchar AS product_type,
    product_bundle.value['tech_provider']::varchar AS bundle_tech_provider,
    product_bundle.value['business_provider']::varchar AS bundle_business_provider,
    product_bundle.value['concept']::varchar AS bundle_concept,
    product_bundle.value['id']::varchar AS bundle_id,
    user_context[0]['member_type']::varchar AS user_member_type,
    source_context[0]['territory']::varchar as territory,
    source_context[0]['environment']::varchar as environment,
    source_context[0]['tracking_platform']::varchar as tracking_platform,
    source_context[0]['tech_provider']::varchar as tech_provider,
    source_context[0]['affiliate']::varchar as affiliate,
    comment
from scratch.carmenmardirosdev36082.raw_snowplow_mock_events,
    LATERAL FLATTEN(INPUT => product_context, outer=>true) product,
    LATERAL FLATTEN(INPUT => product.value['product_bundles'], outer=>true) product_bundle
order by EVENT_NTH
;


create or replace view scratch.carmenmardirosdev36082.flat_booking_context
as
select
       distinct
       user_id,
    domain_sessionidx,
    collector_tstamp,
    event_name,
    event_nth,
booking_update_event['action']::varchar AS booking_update_event_name,
       booking_update_event,
       booking_snapshot_context[0]['booking']['id']::varchar AS booking_id,
       booking_snapshot_context[0]['booking']['status']::varchar AS booking_status,
-- booking_products.value['name']::varchar AS booking_product_name,
--        booking_products.value['type']::varchar AS booking_product_type,
       booking_product_bundles.value['tech_provider']::varchar AS booking_product_bundle_tech_provider,
       booking_product_bundles.value['business_provider']::varchar AS booking_product_bundle_business_provider,
       booking_product_bundles.value['concept']::varchar AS booking_product_bundle_concept,
       booking_product_bundles.value['id']::varchar AS booking_product_bundle_id,
--     PAGE_CONTEXT[0]['pageType']::varchar AS page_type,
--     page_url,
--      product.value['name']::varchar AS product_name,
--      product.value['id']::varchar AS product_id,
--     product.value['type']::varchar AS product_type,
--     product_bundle.value['tech_provider']::varchar AS bundle_tech_provider,
--     product_bundle.value['business_provider']::varchar AS bundle_business_provider,
--     product_bundle.value['concept']::varchar AS bundle_concept,
--     product_bundle.value['id']::varchar AS bundle_id,
--     user_context[0]['member_type']::varchar AS user_member_type,
--     source_context[0]['territory']::varchar as territory,
--     source_context[0]['environment']::varchar as environment,
--     source_context[0]['tracking_platform']::varchar as tracking_platform,
--     source_context[0]['tech_provider']::varchar as tech_provider,
--     source_context[0]['affiliate']::varchar as affiliate,
    comment
from scratch.carmenmardirosdev36082.raw_snowplow_mock_events,
    LATERAL FLATTEN(INPUT => booking_snapshot_context[0]['products'], outer=>true) booking_products,
     LATERAL FLATTEN(INPUT => booking_products.value['product_bundles'], outer=>true) booking_product_bundles,
    LATERAL FLATTEN(INPUT => product_context, outer=>true) product,
    LATERAL FLATTEN(INPUT => product.value['product_bundles'], outer=>true) product_bundle
where booking_product_bundles.value['tech_provider']::varchar = 'SE Core'
order by user_id, EVENT_NTH
;





-- example harmonising SPVs for TB and SE to the Secret Escapes "Sale" concept
select user_id,
    EVENT_NAME,
    booking_update_event['action']::varchar AS booking_update_event_name,
    EVENT_NTH,
    page_url,
       bundle_business_provider,
    bundle_tech_provider,
    bundle_concept AS original_bundle_concept, -- for TB this is offer
    bundle_secret_escapes_sale_id AS sale_id,
    bundle_line,
    bundle_type,
    bundle_name,
    territory
from scratch.carmenmardirosdev36082.flat_product_bundles
where BUNDLE_TECH_PROVIDER = 'Travelbird'
union all
select user_id,
    EVENT_NAME,
       booking_update_event['action'] AS booking_update_event_name,
    EVENT_NTH,
    page_url,
       bundle_business_provider,
    bundle_tech_provider,
    bundle_concept AS original_bundle_concept,
    bundle_id AS sale_id,
    bundle_line,
    bundle_type,
    bundle_name,
    territory
from scratch.carmenmardirosdev36082.flat_product_bundles
where BUNDLE_TECH_PROVIDER = 'SE Core'
;





select *
from scratch.carmenmardirosdev36082.flat_product_bundles;
select *
from scratch.carmenmardirosdev36082.flat_products;