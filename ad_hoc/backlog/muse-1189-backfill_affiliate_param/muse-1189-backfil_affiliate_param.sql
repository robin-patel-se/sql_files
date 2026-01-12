SELECT es.event_tstamp,
       es.is_server_side_event,
       es.page_url,
       PARSE_URL(es.page_url)                                                                        AS parsed_url,
       parsed_url['parameters']:affiliate,
       es.contexts_com_secretescapes_user_context_1,
       es.contexts_com_secretescapes_user_context_1[0]:persisted_cookie:affiliate_parameter::VARCHAR AS persisted_affiliate_param
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp::DATE = CURRENT_DATE - 10
  AND es.event_name = 'page_view';


SELECT *
FROM hygiene_vault_mvp.snowplow.event_stream es
WHERE es.event_tstamp::DATE = CURRENT_DATE - 10
  AND es.event_name IN ('page_view', 'screen_view');