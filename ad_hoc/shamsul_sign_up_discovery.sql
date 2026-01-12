SELECT e.user_id AS shiro_user_id
FROM snowplow.atomic.events e
WHERE collector_tstamp::DATE = '2021-07-07'
  AND se_category = 'user event'
  AND se_action = 'sign up'
  AND se_label LIKE '%REGISTER%'
  AND app_id = 'UK'
    EXCEPT

SELECT sua.shiro_user_id
FROM se.data.se_user_attributes sua
WHERE sua.signup_tstamp::DATE = '2021-07-07'
  AND sua.current_affiliate_territory = 'UK';


WITH exceptions AS (
    SELECT sua.shiro_user_id
    FROM se.data.se_user_attributes sua
    WHERE sua.signup_tstamp::DATE = '2021-07-07'
      AND sua.current_affiliate_territory = 'UK'
        EXCEPT

    SELECT e.user_id AS shiro_user_id
    FROM snowplow.atomic.events e
    WHERE collector_tstamp::DATE = '2021-07-07'
      AND se_category = 'user event'
      AND se_action = 'sign up'
      AND se_label LIKE '%REGISTER%'
      AND app_id = 'UK'
)

SELECT *
FROM se.data.se_user_attributes s
         INNER JOIN exceptions e ON s.shiro_user_id = e.shiro_user_id

--496 missing
--ES magazine -- these should be tracked -138 rows
--ES magazine for v3 mobile apps -- we know this isn't yet tracked -197 rows
--Google PPC -- talk to KJ around if there is another method to signing up outside of the standard flow for CPA/PPC searches --48 row
--remaining appear to be whitelabels, ticket open with mike around tracking the affiliate user id for whitelabel sign ups
-- what is the instant access page? https://www.secretescapes.com/instant-access/es?saleId=26684&targetUri=%2Felegant-cheshire-manor-house-stay-fully-refundable-the-stanneylands-hotel-wilmslow%2Fsale-hotel


SELECT *
FROM snowplow.atomic.events e
WHERE e.user_id IN ('75181126',
                    '75180697',
                    '75178076',
                    '75176479',
                    '75179663',
                    '75177559',
                    '75179034',
                    '75179486',
                    '75179474',
                    '75180494',
                    '75175990',
                    '75179092'
    )
  AND e.collector_tstamp::DATE = '2021-07-07';


SELECT --distinct sua.shiro_user_id,
       --sua.signup_tstamp::date,
       --stba.touch_start_tstamp::date,
       se.data.CHANNEL_CATEGORY(stmc.touch_mkt_channel) AS channel_category,
       COUNT(stba.touch_id) AS sessions
FROM se.data_pii.scv_touch_basic_attributes stba
         JOIN se.data.scv_touch_marketing_channel stmc ON stmc.touch_id = stba.touch_id
         JOIN se.data.se_user_attributes sua ON sua.shiro_user_id::Varchar = stba.attributed_user_id
WHERE stba.touch_start_tstamp::date BETWEEN '2021-06-01' AND '2021-06-30'
  AND stba.touch_start_tstamp::date > sua.signup_tstamp::date
  AND sua.original_affiliate_territory IN ('DE', 'UK')
  AND stmc.touch_mkt_channel IN
      ('Affiliate Program', 'Paid Social CPL', 'PPC - Brand', 'Display CPL', 'PPC - Non Brand CPL', 'Paid Social CPA', 'PPC - Non Brand CPA', 'Display CPA', 'PPC - Undefined')
GROUP BY 1