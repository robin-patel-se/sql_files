SELECT stmc.affiliate,
       COUNT(*) AS sessions
FROM se.data.scv_touch_marketing_channel stmc
WHERE stmc.utm_campaign IS NULL
  AND stmc.utm_content IS NULL
  AND stmc.utm_term IS NULL
  AND stmc.utm_medium IS NULL
  AND stmc.utm_source IS NULL
  AND stmc.click_id IS NULL
  AND stmc.referrer_medium IS NOT NULL
  AND stmc.affiliate IS NOT NULL
  AND stmc.touch_mkt_channel = 'Direct'
GROUP BY 1
ORDER BY 2 DESC