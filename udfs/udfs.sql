CREATE OR REPLACE FUNCTION se.data.posa_category_from_territory(territory VARCHAR
                                                               )
    RETURNS VARCHAR
AS
$$
    SELECT CASE
               WHEN territory IN ('UK', 'Guardian - UK', 'Conde Nast UK') THEN 'UK'
               WHEN territory IN ('DE', 'CH') THEN 'DACH'
               WHEN territory IN ('SE', 'DK', 'NO') THEN 'Scandi'
               WHEN territory IN ('BE', 'TB-BE_NL', 'TB-BE_FR') THEN 'Belgium'
               WHEN territory IN ('NL', 'TB-NL') THEN 'Netherlands'
               WHEN territory = 'FR' THEN 'France'
               WHEN territory = 'IT' THEN 'Italy'
               WHEN territory = 'ES' THEN 'Spain'
               WHEN territory = 'PL' THEN 'Poland'
               WHEN territory = 'US' THEN 'USA'
               WHEN territory IN ('CZ', 'HU') THEN 'CEE'
               WHEN territory IN ('SG', 'HK', 'MY', 'ID') THEN 'Asia'
               END AS posa_category
$$
;

CREATE OR REPLACE FUNCTION se.data.channel_category(channel VARCHAR
                                                   )
    RETURNS VARCHAR
AS
$$
    SELECT CASE
               WHEN channel IN ('Other', 'Partner', 'Email - Other') THEN 'Other'
               WHEN channel IN ('Blog', 'Direct', 'Organic Search', 'Organic Social') THEN 'Free'
               WHEN channel IN ('Email - Other', 'Media', 'Other', 'Partner', 'YouTube') THEN 'Other'
               WHEN channel IN
                    ('Affiliate Program', 'Display', 'Paid Social', 'PPC - Brand', 'PPC - Non Brand CPA',
                     'PPC - Non Brand CPL',
                     'PPC - Undefined') THEN 'Paid'
               ELSE channel
               END AS channel_category
$$
;

CREATE OR REPLACE FUNCTION se.data.platform_from_touch_experience(touch_experience VARCHAR
                                                                 )
    RETURNS VARCHAR
AS
$$
    SELECT CASE
               WHEN touch_experience IN ('mobile wrap android', 'mobile wrap ios') THEN 'Wrap App'
               ELSE INITCAP(touch_experience)
               END AS platform
$$
;

CREATE OR REPLACE FUNCTION se.data.se_week(input_date DATE
                                          )
    RETURNS INT
    LANGUAGE SQL
AS
$$
    SELECT any_value(sc.se_week) --note this is a udf requirement to only return one row
    FROM se.data.se_calendar sc
    WHERE sc.date_value = input_date::DATE
$$
;

GRANT USAGE ON FUNCTION se.data.channel_category(VARCHAR) TO ROLE se_basic;
GRANT USAGE ON FUNCTION se.data.platform_from_touch_experience(VARCHAR) TO ROLE se_basic;
GRANT USAGE ON FUNCTION se.data.posa_category_from_territory(VARCHAR) TO ROLE se_basic;
GRANT USAGE ON FUNCTION se.data.se_week(DATE) TO ROLE se_basic;
GRANT USAGE ON FUNCTION se.data.se_sale_travel_type(VARCHAR, VARCHAR) TO ROLE se_basic;


CREATE OR REPLACE FUNCTION se.data.se_sale_travel_type(posa_territory VARCHAR, posu_country VARCHAR
                                                      )
    RETURNS VARCHAR
    LANGUAGE SQL
AS
$$
    SELECT CASE
               WHEN posa_territory IS NULL OR posu_country IS NULL
                   THEN NULL

               WHEN posa_territory = 'AT'
                   AND posu_country IN ('Austria', 'Switzerland', 'Germany')
                   THEN 'Domestic'

               WHEN posa_territory IN ('BE', 'TB-BE_FR', 'TB-BE_NL')
                   AND posu_country IN ('Belgium', 'Netherlands', 'Luxemburg')
                   THEN 'Domestic'

               WHEN posa_territory IN ('CH', 'DE')
                   AND posu_country IN ('Austria', 'Switzerland', 'Germany')
                   THEN 'Domestic'

               WHEN posa_territory = 'DK'
                   AND posu_country IN ('Sweden', 'Denmark', 'Norway')
                   THEN 'Domestic'

               WHEN posa_territory = 'ES'
                   AND posu_country IN ('Portugal', 'Spain', 'Andorra')
                   THEN 'Domestic'

               WHEN posa_territory = 'FR'
                   AND posu_country IN ('Monaco', 'France')
                   THEN 'Domestic'

               WHEN posa_territory IN ('HK', 'ID', 'MY', 'SG')
                   AND posu_country IN ('Japan', 'Malaysia', 'Indonesia', 'Maldives', 'Thailand', 'Singapore', 'China')
                   THEN 'Domestic'

               WHEN posa_territory = 'IT'
                   AND posu_country = 'Italy'
                   THEN 'Domestic'

               WHEN posa_territory IN ('NL', 'TB-NL')
                   AND posu_country IN ('Belgium', 'Netherlands', 'Luxemburg')
                   THEN 'Domestic'

               WHEN posa_territory = 'NO'
                   AND posu_country IN ('Sweden', 'Denmark', 'Norway')
                   THEN 'Domestic'

               WHEN posa_territory = 'SE'
                   AND posu_country = 'Sweden'
                   THEN 'Domestic'

               WHEN posa_territory = 'UK'
                   AND posu_country IN ('England', 'Wales/Cymru', 'Scotland', 'Ireland')
                   THEN 'Domestic'

               WHEN posa_territory = 'US'
                   AND posu_country IN ('Canada', 'USA')
                   THEN 'Domestic'


               ELSE 'International'
               END AS travel_type
$$
;

SELECT se.data.se_sale_travel_type('DK', 'Sweden');


SELECT get_ddl('function', 'se.data.se_week(date)');

