-- investigate the validity of channelling for paid channels

SELECT
    CASE
        WHEN stmc.touch_mkt_channel
            IN (
                'Display CPA',
                'Display CPL',
                'Affiliate Program',
                'Paid Social CPA',
                'Paid Social CPL',
                'PPC - Non Brand CPA',
                'PPC - Non Brand CPL'
                 ) THEN 'Performance Marketing'
        ELSE 'Non-Performance Marketing'
        END                                                     AS channel,
    stmc.touch_id,
    stmc.touch_mkt_channel,
    stmc.channel_category,
    stmc.touch_landing_page,
    stmc.touch_hostname,
    stmc.touch_hostname_territory,
    stmc.hostname_posa_territory,
    stmc.utm_campaign,
    stmc.utm_medium,
    stmc.utm_source,
    stmc.utm_term,
    stmc.utm_content,
    stmc.click_id,
    stmc.sub_affiliate_name,
    stmc.affiliate,
    stmc.landing_page_parameters['affiliateUrlString']::VARCHAR AS affiliate_url_string,
    stmc.touch_affiliate_territory,
    stmc.affiliate_posa_territory,
    stmc.awadgroupid,
    stmc.awcampaignid,
    stmc.referrer_hostname,
    stmc.referrer_medium,
    stmc.landing_page_parameters
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
    -- lobster definition of a paid channel
    -- looking to see what channels don't match this definition and see if they SHOULD be in paid
    AND stmc.touch_mkt_channel NOT IN (
                                       'Display CPA',
                                       'Display CPL',
                                       'Affiliate Program',
                                       'Paid Social CPA',
                                       'Paid Social CPL',
                                       'PPC - Non Brand CPA',
                                       'PPC - Non Brand CPL'
        )
    AND stba.touch_start_tstamp >= '2022-09-01';

------------------------------------------------------------------------------------------------------------------------
-- affiliate url string param used instead of affiliate

SELECT
    stmc.touch_id,
    stmc.touch_mkt_channel,
    stmc.channel_category,
    stmc.touch_landing_page,
    stmc.touch_hostname,
    stmc.touch_hostname_territory,
    stmc.hostname_posa_territory,
    stmc.utm_campaign,
    stmc.utm_medium,
    stmc.utm_source,
    stmc.utm_term,
    stmc.utm_content,
    stmc.click_id,
    stmc.sub_affiliate_name,
    stmc.affiliate,
    stmc.landing_page_parameters['affiliateUrlString']::VARCHAR AS affiliate_url_string,
    stmc.touch_affiliate_territory,
    stmc.affiliate_posa_territory,
    stmc.awadgroupid,
    stmc.awcampaignid,
    stmc.referrer_hostname,
    stmc.referrer_medium,
    stmc.landing_page_parameters
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stmc.landing_page_parameters['affiliateUrlString']::VARCHAR IS NOT NULL
  AND stmc.affiliate IS NULL
  AND stba.touch_start_tstamp >= '2022-08-01';

SELECT
    COUNT(*)
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stmc.landing_page_parameters['affiliateUrlString']::VARCHAR IS NOT NULL
  AND stmc.affiliate IS NULL
  AND stba.touch_start_tstamp >= '2022-09-01';

-- 10,317 sessions since beginning of September 22 where the affiliate url string is not null
-- 79,030 sessions since beginning of August 22

SELECT
    COUNT(*)
FROM se.data.scv_touch_basic_attributes stba
WHERE stba.touch_start_tstamp >= '2022-08-01';

-- 2,635,265 total since September 22
-- 18,283,418 total since August 22

SELECT
    stmc.landing_page_parameters['affiliateUrlString']::VARCHAR AS affiliate_url_string,
    COUNT(*)                                                    AS sessions,
    sessions /

FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stmc.landing_page_parameters['affiliateUrlString']::VARCHAR IS NOT NULL
  AND stmc.affiliate IS NULL
  AND stba.touch_start_tstamp >= '2022-08-01'
GROUP BY 1;

-- some of the big offenders
/*
AFFILIATE_URL_STRING	COUNT(*)
de	19021
goodeutsch	7059
be	6342
it	6025
sv	5783
es	5347
nl	5186
goo-cpl-de	4539
dk	2256
facebook-deutsch-all	2030
nor	1614
goo-cpl-brand-de	863

  */
-- most look like operational use of the parameter to adjust the site experience
-- some channels aren't being properly channelled due to this but the number is marginal

-- top line, affiliate url string is something we need to adjust but is unlikely to make any significant change to channels.

-- of the values in `affiliateUrlString` less than 10% appear to be anything non functional to the experience of the site.
-- accommodating for these in channelling will only re-channel circa 0.04% of sessions.

------------------------------------------------------------------------------------------------------------------------

-- looking at sessions that would be in a different channel if we used affiliate to channel them

SELECT
    CASE
        WHEN stmc.touch_mkt_channel
            IN (
                'Display CPA',
                'Display CPL',
                'Affiliate Program',
                'Paid Social CPA',
                'Paid Social CPL',
                'PPC - Non Brand CPA',
                'PPC - Non Brand CPL'
                 ) THEN 'Performance Marketing'
        ELSE 'Non-Performance Marketing'
        END                                                     AS channel,
    stmc.touch_id,
    stmc.touch_mkt_channel,
    stmc.channel_category,
    stmc.touch_landing_page,
    stmc.touch_hostname,
    stmc.touch_hostname_territory,
    stmc.hostname_posa_territory,
    stmc.utm_campaign,
    stmc.utm_medium,
    stmc.utm_source,
    stmc.utm_term,
    stmc.utm_content,
    stmc.click_id,
    stmc.sub_affiliate_name,
    stmc.affiliate,
    stmc.landing_page_parameters['affiliateUrlString']::VARCHAR AS affiliate_url_string,
    stmc.touch_affiliate_territory,
    stmc.affiliate_posa_territory,
    stmc.awadgroupid,
    stmc.awcampaignid,
    stmc.referrer_hostname,
    stmc.referrer_medium,
    stmc.landing_page_parameters
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
    -- lobster definition of a paid channel
    -- looking to see what channels don't match this definition and see if they SHOULD be in paid
    AND stmc.touch_mkt_channel NOT IN (
                                       'Display CPA',
                                       'Display CPL',
                                       'Affiliate Program',
                                       'Paid Social CPA',
                                       'Paid Social CPL',
                                       'PPC - Non Brand CPA',
                                       'PPC - Non Brand CPL'
        )
    AND stba.touch_start_tstamp >= '2022-09-01'
    AND stmc.affiliate IS NOT NULL;


-- check how many sessions fall into non paid but have an affiliate
SELECT *
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stmc.touch_mkt_channel = 'Direct'
  AND stba.touch_start_tstamp >= '2022-09-01'
  AND stmc.affiliate IS NOT NULL
  -- cohort model definition of paid channels
  AND stmc.touch_mkt_channel NOT IN (
                                     'Display CPA',
                                     'Display CPL',
                                     'Affiliate Program',
                                     'Paid Social CPA',
                                     'Paid Social CPL',
                                     'PPC - Non Brand CPA',
                                     'PPC - Non Brand CPL'
    );


SELECT
    stmc.touch_mkt_channel,
    COUNT(*) AS sessions
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stba.touch_start_tstamp >= '2022-09-01'
  AND stmc.affiliate IS NOT NULL
  -- cohort model definition of paid channels
  AND stmc.touch_mkt_channel NOT IN (
                                     'Display CPA',
                                     'Display CPL',
                                     'Affiliate Program',
                                     'Paid Social CPA',
                                     'Paid Social CPL',
                                     'PPC - Non Brand CPA',
                                     'PPC - Non Brand CPL'
    )
GROUP BY 1;

/*
TOUCH_MKT_CHANNEL	SESSIONS
Direct	114192
PPC - Brand	85713
Organic Search Non-Brand	16519
Partner	16362
Other	10555
Organic Search Brand	3917
Organic Social	2707
PPC - Undefined	1471
Email - Newsletter	533
Email - Triggers	59
Media	49
Email - Other	44
Test	1
*/

-- 18,283,418 total since August 22
-- 252,122 (1.4%) sessions that are not in a paid channel but have an affiliate parameter
-- 45% of those sessions (114,192) go into Direct, the next big offender goes into PPC - Brand, this is expected as cohort model doesn't consider this channel as 'Paid'
-- 6% each for Organic Search and Partner

-- top level non paid sessions that might be in different paid channels if we included affiliate is far too low to make an impact


------------------------------------------------------------------------------------------------------------------------
-- when click id is null what happens to these sessions?
-- ppc channels are only assigned IF a click id is present

/*
CASE
    WHEN
    (
        LOWER(t.affiliate) LIKE '%bra%'
            OR
        LOWER(t.affiliate) REGEXP '.*(gooaus|goobelgian|goo-dane|goodeutsch|secret-id|goodutch|goo-norway|gooswede|gooswi|goosups|goousa|goousa-ec|goousa-fl).*'
    )
    AND LOWER(t.affiliate) NOT LIKE '%yahnobra%'
        THEN 'PPC - Brand'

    WHEN
    (
        LOWER(t.affiliate) LIKE '%cpa%'
            OR
        LOWER(t.affiliate) REGEXP '.*(dsa - france|active - eagle|hpa - uk).*'
    )
    AND LOWER(t.affiliate) NOT LIKE '%brand%'
        THEN 'PPC - Non Brand CPA'

    WHEN
    (
        LOWER(t.affiliate) LIKE '%cpl%'
            OR
        LOWER(t.affiliate) LIKE '%secret%'
            OR
        LOWER(t.affiliate) REGEXP '.*(at-dsa|de-dsa|ppc-de2-test-variant-a-de-printfox|ppc-de2-test-variant-b-de-maponos|dsa-italy|nl-dsa|ch-dsa|ppc-uk3-test-variant-a-uk-printfox|ppc-uk3-test-variant-b-uk-maponos|usa-dsa-ec|usa-dsa|stalion-italy|yahooppcdsa|yahooppc|yahnobra-german|yahoo2ppc|yahnobra2-german|yahoo3ppc|yahnobra-dutch|yahnobra-sweden|yahnobra-denmark|yahnobra-usa|yahnobra-norway).*'
    )
    --AND LOWER(t.affiliate) NOT LIKE '%cpa%'
    AND LOWER(t.affiliate) != 'secret-bra-id'
    AND LOWER(t.affiliate) != 'secret-id'

        THEN 'PPC - Non Brand CPL'

    ELSE 'PPC - Undefined'
END
*/

SELECT
    stmc.touch_mkt_channel,
    COUNT(*) AS sessions
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stba.touch_start_tstamp >= '2022-09-01'
  -- not currently a ppc channel
  AND stmc.touch_mkt_channel NOT LIKE 'PPC%'
  -- doesn't have a click id
  AND stmc.click_id IS NULL
GROUP BY 1;

SELECT
    stmc.touch_mkt_channel,
    CASE
        WHEN
                (
                            LOWER(stmc.affiliate) LIKE '%bra%'
                        OR
                            LOWER(stmc.affiliate) REGEXP '.*(gooaus|goobelgian|goo-dane|goodeutsch|secret-id|goodutch|goo-norway|gooswede|gooswi|goosups|goousa|goousa-ec|goousa-fl).*'
                    )
                AND LOWER(stmc.affiliate) NOT LIKE '%yahnobra%'
            THEN 'PPC - Brand'

        WHEN
                (
                            LOWER(stmc.affiliate) LIKE '%cpa%'
                        OR
                            LOWER(stmc.affiliate) REGEXP '.*(dsa - france|active - eagle|hpa - uk).*'
                    )
                AND LOWER(stmc.affiliate) NOT LIKE '%brand%'
            THEN 'PPC - Non Brand CPA'

        WHEN
                (
                            LOWER(stmc.affiliate) LIKE '%cpl%'
                        OR
                            LOWER(stmc.affiliate) LIKE '%secret%'
                        OR
                            LOWER(stmc.affiliate) REGEXP
                            '.*(at-dsa|de-dsa|ppc-de2-test-variant-a-de-printfox|ppc-de2-test-variant-b-de-maponos|dsa-italy|nl-dsa|ch-dsa|ppc-uk3-test-variant-a-uk-printfox|ppc-uk3-test-variant-b-uk-maponos|usa-dsa-ec|usa-dsa|stalion-italy|yahooppcdsa|yahooppc|yahnobra-german|yahoo2ppc|yahnobra2-german|yahoo3ppc|yahnobra-dutch|yahnobra-sweden|yahnobra-denmark|yahnobra-usa|yahnobra-norway).*'
                    )
                --AND LOWER(stmc.affiliate) NOT LIKE '%cpa%'
                AND LOWER(stmc.affiliate) != 'secret-bra-id'
                AND LOWER(stmc.affiliate) != 'secret-id'
            THEN 'PPC - Non Brand CPL'

        ELSE 'Other' -- changed due to filter on click id
        END  AS ppc_channel,
    COUNT(*) AS sessions
FROM se.data.scv_touch_marketing_channel stmc
    INNER JOIN se.data.scv_touch_basic_attributes stba ON stmc.touch_id = stba.touch_id
WHERE stba.touch_start_tstamp >= '2022-09-01'
  -- not currently a ppc channel
  AND stmc.touch_mkt_channel NOT LIKE 'PPC%'
  -- doesn't have a click id
  AND stmc.click_id IS NULL
GROUP BY 1, 2;

------------------------------------------------------------------------------------------------------------------------
--check affiliates logic in ppc


------------------------------------------------------------------------------------------------------------------------
USE WAREHOUSE pipe_xlarge;

-- check channelling of sessions that can be aligned with a sign up event
SELECT
    sua.shiro_user_id,
    sua.signup_tstamp,
    sua.original_affiliate_name,
    sua.member_original_affiliate_classification,
    CASE
        WHEN
                (
                            LOWER(stmc.affiliate) LIKE '%bra%'
                        OR
                            LOWER(stmc.affiliate) REGEXP '.*(gooaus|goobelgian|goo-dane|goodeutsch|secret-id|goodutch|goo-norway|gooswede|gooswi|goosups|goousa|goousa-ec|goousa-fl).*'
                    )
                AND LOWER(stmc.affiliate) NOT LIKE '%yahnobra%'
            THEN 'PPC - Brand'

        WHEN
                (
                            LOWER(stmc.affiliate) LIKE '%cpa%'
                        OR
                            LOWER(stmc.affiliate) REGEXP '.*(dsa - france|active - eagle|hpa - uk).*'
                    )
                AND LOWER(stmc.affiliate) NOT LIKE '%brand%'
            THEN 'PPC - Non Brand CPA'

        WHEN
                (
                            LOWER(stmc.affiliate) LIKE '%cpl%'
                        OR
                            LOWER(stmc.affiliate) LIKE '%secret%'
                        OR
                            LOWER(stmc.affiliate) REGEXP
                            '.*(at-dsa|de-dsa|ppc-de2-test-variant-a-de-printfox|ppc-de2-test-variant-b-de-maponos|dsa-italy|nl-dsa|ch-dsa|ppc-uk3-test-variant-a-uk-printfox|ppc-uk3-test-variant-b-uk-maponos|usa-dsa-ec|usa-dsa|stalion-italy|yahooppcdsa|yahooppc|yahnobra-german|yahoo2ppc|yahnobra2-german|yahoo3ppc|yahnobra-dutch|yahnobra-sweden|yahnobra-denmark|yahnobra-usa|yahnobra-norway).*'
                    )
                --AND LOWER(stmc.affiliate) NOT LIKE '%cpa%'
                AND LOWER(stmc.affiliate) != 'secret-bra-id'
                AND LOWER(stmc.affiliate) != 'secret-id'
            THEN 'PPC - Non Brand CPL'

        ELSE 'Other' -- changed due to filter on click id
        END AS ppc_channel,
    stmc.*
FROM se.data.se_user_attributes sua
    INNER JOIN se.data_pii.scv_touch_basic_attributes stba ON sua.shiro_user_id = TRY_TO_NUMBER(stba.attributed_user_id)
    AND sua.signup_tstamp BETWEEN stba.touch_start_tstamp AND DATEADD(DAY, 1, stba.touch_start_tstamp)
    INNER JOIN se.data.scv_touch_marketing_channel stmc ON stba.touch_id = stmc.touch_id
WHERE sua.signup_tstamp >= '2022-09-01';




