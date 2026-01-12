USE WAREHOUSE pipe_xlarge;

WITH user_agg AS (
    -- categorise sessions up to common grain by user
    SELECT
        mtba.attributed_user_id,
        IFF(ua.shiro_user_id IS NOT NULL, 'member', 'non member')                 AS member_status,
        DATE_TRUNC('month', COALESCE(ua.signup_tstamp, mtba.touch_start_tstamp))  AS signup_month,
        DATE_TRUNC('month', mtba.touch_start_tstamp)                              AS event_month,
        COALESCE(ua.original_affiliate_territory, mtmc.touch_affiliate_territory) AS session_territory,
        CASE
            WHEN session_territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
            WHEN session_territory IN ('DE', 'CH', 'AT') THEN 'BE'
            WHEN session_territory IN ('TB-NL', 'NL') THEN 'NL'
            WHEN session_territory IN ('CH', 'DK', 'IT', 'NO', 'SE', 'UK')
                THEN session_territory
            ELSE 'Other'
            END                                                                   AS user_session_territory,
        COALESCE(ua.original_affiliate_name, mtmc.touch_mkt_channel)              AS user_original_affiliate,
        CASE
            WHEN user_original_affiliate
                IN (
                    'PPC_NON_BRAND_CPA',
                    'AFFILIATE_PROGRAM',
                    'PAID_SOCIAL_CPA',
                    'DISPLAY_CPA',
                     --non member channels
                    'Affiliate Program',
                    'Display CPA',
                    'Paid Social CPA',
                    'PPC - Non Brand CPA'
                     ) THEN 'CPA'
            WHEN user_original_affiliate
                IN (
                    'DISPLAY_CPL',
                    'PAID_SOCIAL_CPL',
                    'PPC_NON_BRAND_CPL',
                     --non member channels
                    'Display CPL',
                    'Paid Social CPL',
                    'PPC - Non Brand CPL'
                     ) THEN 'CPL'
            ELSE 'Other'
            END                                                                   AS affiliate_category_group,
        CASE
            WHEN mtmc.touch_mkt_channel
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
            END                                                                   AS channel,
        COUNT(DISTINCT mtba.touch_id)                                             AS sessions
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
        LEFT JOIN  data_vault_mvp.dwh.user_attributes ua ON
                TRY_TO_NUMBER(mtba.attributed_user_id) = ua.shiro_user_id
            AND mtba.stitched_identity_type = 'se_user_id'

--     WHERE DATE_TRUNC(MONTH, mtba.touch_start_tstamp) = DATE_TRUNC(MONTH, CURRENT_DATE - 1)

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
     user_channel AS (
         -- aggregate session data up to user by common grain and calculate more prominent channel type
         SELECT
             ua.attributed_user_id,
             ua.member_status,
             ua.signup_month,
             ua.event_month,
             ua.user_session_territory,
             ua.affiliate_category_group,
             COALESCE(SUM(IFF(ua.channel = 'Performance Marketing', ua.sessions, NULL)), 0)                                                  AS performance_marketing_sessions,
             COALESCE(SUM(IFF(ua.channel = 'Non-Performance Marketing', ua.sessions, NULL)), 0)                                              AS non_performance_marketing_sessions,
             --channel is based on most frequent type of sessions and favours Performance Marketing if equal
             IFF(performance_marketing_sessions >= non_performance_marketing_sessions, 'Performance Marketing', 'Non-Performance Marketing') AS channel
         FROM user_agg ua
         GROUP BY 1, 2, 3, 4, 5, 6
     )

SELECT
        uc.signup_month ||
        uc.event_month ||
        uc.user_session_territory ||
        uc.affiliate_category_group ||
        uc.channel                                                                        AS id,
        uc.signup_month,
        uc.event_month,
        uc.user_session_territory,
        uc.affiliate_category_group,
        uc.channel,
        COUNT(DISTINCT IFF(uc.member_status = 'member', uc.attributed_user_id, NULL))     AS member_mau,
        COUNT(DISTINCT IFF(uc.member_status = 'non member', uc.attributed_user_id, NULL)) AS non_member_mau
FROM user_channel uc
GROUP BY 1, 2, 3, 4, 5, 6

------------------------------------------------------------------------------------------------------------------------

-- accommodate for uses that have a paid session throughout the month

--options:
-- if a user has any paid channel throughout the month call them performance marketing
-- if a user is active in a month use their first session within the month to determine performance vs non performance marketing
-- if a user is active in a month use their last session within the month to determine performance vs non performance marketing
-- if a user has a certain ratio of performance to non performance then act accordingly eg. if a user has at least 20% sessions performance marketing call them performance marketing


WITH user_agg AS (
    -- categorise sessions up to common grain by user
    SELECT
        mtba.attributed_user_id,
        IFF(ua.shiro_user_id IS NOT NULL, 'member', 'non member')                 AS member_status,
        DATE_TRUNC('month', COALESCE(ua.signup_tstamp, mtba.touch_start_tstamp))  AS signup_month,
        DATE_TRUNC('month', mtba.touch_start_tstamp)                              AS event_month,
        COALESCE(ua.original_affiliate_territory, mtmc.touch_affiliate_territory) AS session_territory,
        CASE
            WHEN session_territory IN ('TB_BE-FR', 'TB-BE_FR', 'BE', 'TB-BE_NL') THEN 'BE'
            WHEN session_territory IN ('DE', 'CH', 'AT') THEN 'BE'
            WHEN session_territory IN ('TB-NL', 'NL') THEN 'NL'
            WHEN session_territory IN ('CH', 'DK', 'IT', 'NO', 'SE', 'UK')
                THEN session_territory
            ELSE 'Other'
            END                                                                   AS user_session_territory,
        COALESCE(ua.original_affiliate_name, mtmc.touch_mkt_channel)              AS user_original_affiliate,
        CASE
            WHEN user_original_affiliate
                IN (
                    'PPC_NON_BRAND_CPA',
                    'AFFILIATE_PROGRAM',
                    'PAID_SOCIAL_CPA',
                    'DISPLAY_CPA',
                     --non member channels
                    'Affiliate Program',
                    'Display CPA',
                    'Paid Social CPA',
                    'PPC - Non Brand CPA'
                     ) THEN 'CPA'
            WHEN user_original_affiliate
                IN (
                    'DISPLAY_CPL',
                    'PAID_SOCIAL_CPL',
                    'PPC_NON_BRAND_CPL',
                     --non member channels
                    'Display CPL',
                    'Paid Social CPL',
                    'PPC - Non Brand CPL'
                     ) THEN 'CPL'
            ELSE 'Other'
            END                                                                   AS affiliate_category_group,
        CASE
            WHEN mtmc.touch_mkt_channel
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
            END                                                                   AS channel,
        COUNT(DISTINCT mtba.touch_id)                                             AS sessions
    FROM data_vault_mvp.single_customer_view_stg.module_touch_basic_attributes mtba
        INNER JOIN data_vault_mvp.single_customer_view_stg.module_touch_marketing_channel mtmc ON mtba.touch_id = mtmc.touch_id
        LEFT JOIN  data_vault_mvp.dwh.user_attributes ua ON TRY_TO_NUMBER(mtba.attributed_user_id) = ua.shiro_user_id
        AND mtba.stitched_identity_type = 'se_user_id'
    WHERE DATE_TRUNC(MONTH, mtba.touch_start_tstamp) = DATE_TRUNC(MONTH, CURRENT_DATE - 1)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)
     -- aggregate session data up to user by common grain and calculate more prominent channel type
SELECT
    ua.attributed_user_id,
    ua.member_status,
    ua.signup_month,
    ua.event_month,
    ua.user_session_territory,
    ua.affiliate_category_group,
    COALESCE(SUM(IFF(ua.channel = 'Performance Marketing', ua.sessions, NULL)), 0)     AS performance_marketing_sessions,
    COALESCE(SUM(IFF(ua.channel = 'Non-Performance Marketing', ua.sessions, NULL)), 0) AS non_performance_marketing_sessions,
    performance_marketing_sessions / SUM(ua.sessions)                                  AS perc_performance_marketing_sessions
FROM user_agg ua
GROUP BY 1, 2, 3, 4, 5, 6;




------------------------------------------------------------------------------------------------------------------------
