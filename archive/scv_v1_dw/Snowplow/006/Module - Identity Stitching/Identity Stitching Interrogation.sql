USE WAREHOUSE PIPE_LARGE;
USE SCHEMA SCRATCH.ROBINPATEL;

SELECT *
FROM MODULE_IDENTITY_ASSOCIATIONS;

SELECT identity_fragment,
       COUNT(distinct SE_USER_ID)
FROM MODULE_IDENTITY_ASSOCIATIONS
GROUP BY 1
HAVING COUNT(distinct SE_USER_ID) > 1
ORDER BY 2 DESC;

SELECT COUNT(distinct identity_fragment)
FROM EVENT_STREAM
WHERE SE_USER_ID IS NOT NULL
  AND identity_fragment IS NOT NULL; --8,809,811

SELECT COUNT(*)
FROM MODULE_IDENTITY_ASSOCIATIONS; --8,855,367


SELECT COUNT(*)
FROM (
         SELECT identity_fragment,
                COUNT(distinct SE_USER_ID)
         FROM MODULE_IDENTITY_ASSOCIATIONS
         GROUP BY 1
         HAVING COUNT(distinct SE_USER_ID) > 1
         ORDER BY 2 DESC
     );
--31,366

--29,595 event user identifiers with multiple secret escapes user identifiers 0.3% of all event user identifiers.

SELECT * FROM SNOWPLOW.ATOMIC.EVENTS WHERE DOMAIN_USERID = '99ba9bb8-ef34-4d97-986b-74fe05f83039';


SELECT LAST_VALUE(SE_USER_ID)
                  OVER (PARTITION BY identity_fragment ORDER BY FIRST_EVENT_TSTAMP) AS ATTRIBUTED_USER_ID, --take the most recent occurrence of EVENT USER IDENTIFIER
       SE_USER_ID,
       identity_fragment,
       CURRENT_TIMESTAMP                                                                AS updated_at
FROM MODULE_IDENTITY_ASSOCIATIONS;


--to see reattributed user ids
SELECT *
FROM (
         SELECT LAST_VALUE(SE_USER_ID)
                           OVER (PARTITION BY identity_fragment ORDER BY FIRST_EVENT_TSTAMP) AS ATTRIBUTED_USER_ID, --take the most recent occurrence of EVENT USER IDENTIFIER
                SE_USER_ID,
                identity_fragment,
                CURRENT_TIMESTAMP                                                                AS updated_at
         FROM MODULE_IDENTITY_ASSOCIATIONS
     )
WHERE ATTRIBUTED_USER_ID != SE_USER_ID;


--to check there are no duplicate attributed user ids per identity_fragment
SELECT identity_fragment,
       COUNT(distinct ATTRIBUTED_USER_ID)
FROM (
         SELECT LAST_VALUE(SE_USER_ID)
                           OVER (PARTITION BY identity_fragment ORDER BY FIRST_EVENT_TSTAMP) AS ATTRIBUTED_USER_ID, --take the most recent occurrence of EVENT USER IDENTIFIER
                SE_USER_ID,
                identity_fragment,
                CURRENT_TIMESTAMP                                                                AS updated_at
         FROM MODULE_IDENTITY_ASSOCIATIONS
     )
GROUP BY 1
HAVING COUNT(distinct ATTRIBUTED_USER_ID) > 1
;

-- check how many internal ip events have been reattributed


SELECT e.identity_fragment,
       e.SE_USER_ID,
       i.ATTRIBUTED_USER_ID
FROM EVENT_STREAM e
         LEFT JOIN MODULE_IDENTITY_STITCHING i ON e.identity_fragment = i.identity_fragment
WHERE e.IS_INTERNAL_IP_ADDRESS_EVENT = TRUE
;

SELECT COUNT(distinct identity_fragment),
       count(distinct SE_USER_ID),
       count(distinct ATTRIBUTED_USER_ID)
FROM (
         SELECT e.identity_fragment,
                e.SE_USER_ID,
--                 i.SE_USER_ID,
                i.ATTRIBUTED_USER_ID
         FROM EVENT_STREAM e
                  LEFT JOIN MODULE_IDENTITY_STITCHING i ON e.identity_fragment = i.identity_fragment
--          WHERE e.IS_INTERNAL_IP_ADDRESS_EVENT = TRUE
     )

-- 3218 distinct event user identifiers, of which 1072 SE user ids, when identity stitched this moves to 847 SE user ids
-- which means 225 user journeys are technically appropriated to a different user id than they occurred on.


