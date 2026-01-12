CREATE OR REPLACE VIEW collab.performance_analytics.ppc_leads_data AS
(
WITH opt_in_status AS (
    SELECT
        sua.shiro_user_id,
        sua.signup_tstamp :: DATE                         AS date,
        sua.original_affiliate_id                         AS affiliate_id,
        sua.original_affiliate_name                       AS affiliate_name,
        sua.original_affiliate_territory_id               AS affilaite_territory,

        SUM(CASE WHEN email_opt_in = 2 THEN 1 ELSE 0 END) AS non,
        SUM(CASE WHEN email_opt_in = 1 THEN 1 ELSE 0 END) AS weekly,
        SUM(CASE WHEN email_opt_in = 0 THEN 1 ELSE 0 END) AS both

    FROM se.data.se_user_attributes sua
    WHERE signup_tstamp :: DATE >= '2014-01-01'
    GROUP BY 1, 2, 3, 4, 5
)
SELECT
    os.date                 AS date,
    os.affiliate_id,
    os.affiliate_name,
    os.affilaite_territory,

    COUNT(os.shiro_user_id) AS signups,
    SUM(os.non)             AS non,
    SUM(os.weekly)          AS weekly,
    SUM(os.both)            AS both

FROM opt_in_status os


GROUP BY 1, 2, 3, 4 );

SELECT *
FROM se.data.se_territory st


SELECT DISTINCT
    sua.email_opt_in,
    sua.email_opt_in_status
FROM se.data.se_user_attributes sua;