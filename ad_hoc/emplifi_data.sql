SELECT
	ipm.id,
	ipm.author,
	ipm.media_type,
	ipm.content_type,
	ipm.content,
	ipm.comments,
	ipm.likes,
	ipm.created_time,
	ipm.insights_engagement,
	ipm.insights_impressions,
	ipm.insights_reach,
	ipm.insights_story_replies,
	ipm.insights_video_views,
	ipm.interactions,
	ipm.origin,
	ipm.url
FROM latest_vault.emplifi.instagram_post_metrics ipm
WHERE ipm.author['name']::VARCHAR = 'Secret Escapes'
  AND ipm.content_type = 'post'
;

SELECT *
FROM latest_vault.emplifi.instagram_post_metrics ipm
;
