-- Ensure you are using a role with CREATE AGENT privileges
USE ROLE ai_admin;

SELECT get_ddl('cortex_agent', 'se.data.trading');

create or replace agent TRADING
	profile='{"display_name":"Trading Agent"}'
	from specification
	$$
	models:
	  orchestration: "claude-3-5-sonnet"
	orchestration: {}
	instructions:
	  response: "You are a specialized trading data assistant. Use the booking_analysis\
	    \ tool to answer questions about trade bookings and volumes.\n"
	tools:
	  - tool_spec:
	      type: "cortex_analyst_text_to_sql"
	      name: "booking_analysis_semantic_view"
	      description: "Used to query booking and trading metrics from the booking_analysis\
	        \ semantic view."
	  - tool_spec:
	      type: "cortex_search"
	      name: "promos_event_calendar"
	      description: "Searches the se event calendar dataaset to return promos"
	  - tool_spec:
	      type: "cortex_search"
	      name: "product_release_event_calendar"
	      description: "Searches the se event calendar dataaset to return product releases"
	tool_resources:
	  booking_analysis_semantic_view:
	    semantic_view: "SE.DATA.BOOKING_ANALYSIS"
	  product_release_event_calendar:
	    filter:
	      '@eq':
	        CALENDAR_SOURCE: "product_release"
	    id_column: "GSHEET_LINK"
	    max_results: 4
	    search_service: "SE.DATA.SE_EVENTS_CALENDAR"
	    title_column: "EVENT_NAME"
	  promos_event_calendar:
	    filter:
	      '@eq':
	        CALENDAR_SOURCE: "promo"
	    id_column: "GSHEET_LINK"
	    max_results: 4
	    search_service: "SE.DATA.SE_EVENTS_CALENDAR"
	    title_column: "EVENT_NAME"

	$$;

SELECT get_ddl('cortex_agent', 'se.bi.sessions');

USE DATABASE SE;
USE SCHEMA BI;
create or replace agent SESSIONS
	comment='The Session Agent is your specialised AI companion for navigating Secret Escapes’ web/app behavioural data. It combines deep session-level analytics with real-world context to help you understand not just what is happening on our platforms, but why it’s happening.'
	profile='{"display_name":"Session Agent"}'
	from specification
	$$
	models:
	  orchestration: "claude-sonnet-4-5"
	orchestration: {}
	instructions:
	  response: "You are a specialized trading data assistant for Secret Escapes.\nYou\
	    \ have access to two primary tools:\n1. session_analysis: Use this for structured\
	    \ data queries regarding trade bookings, volumes, conversion funnels, and user\
	    \ behavior.\n2. event_calendar: Use this for unstructured searches regarding marketing\
	    \ events, holidays, or specific calendar notes that might impact trading metrics.\n\
	    \nWhen asked about changes in metrics, first check the session_analysis for the\
	    \ 'what' (the numbers) and then consult the event_calendar for the 'why' (contextual\
	    \ events).\n"
	  sample_questions:
	    - question: "How many sessions did we have by device over the last month"
	    - question: "How has our conversion rate for PPC - Brand changed over the last\
	        \ 12 months?"
	    - question: "How many unique users did we have last week?"
	tools:
	  - tool_spec:
	      type: "cortex_analyst_text_to_sql"
	      name: "session_analysis"
	      description: "This tool provides comprehensive session-level analytics for Secret\
	        \ Escapes.\nIt combines SESSION_METRICS (funnel data, revenue, and behavioral\
	        \ patterns),\nSE_CALENDAR (temporal hierarchies), and SE_USER_ATTRIBUTES (user\
	        \ segmentation and cohorts).\nUse this to answer structured questions about\
	        \ conversion, booking counts, and user journeys.\n"
	  - tool_spec:
	      type: "cortex_search"
	      name: "product_release_event_calendar"
	      description: "Searches the se event calendar dataaset to return product releases"
	  - tool_spec:
	      type: "cortex_search"
	      name: "promos_event_calendar"
	      description: "Searches the se event calendar dataaset to return promos or promotions"
	tool_resources:
	  product_release_event_calendar:
	    id_column: "GSHEET_LINK"
	    max_results: 10
	    search_service: "SE.DATA.SE_EVENTS_CALENDAR"
	    title_column: "EVENT_NAME"
	  promos_event_calendar:
	    filter:
	      '@eq':
	        CALENDAR_SOURCE: "promo"
	    id_column: "GSHEET_LINK"
	    max_results: 4
	    search_service: "SE.DATA.SE_EVENTS_CALENDAR"
	    title_column: "EVENT_NAME"
	  session_analysis:
	    semantic_view: "SE.BI.SESSION_ANALYSIS"

	$$;


                  GRANT SELECT ON TABLE SE.DATA.SE_EVENTS_CALENDAR TO ROLE personal_role__robinpatel;

SELECT get_ddl('cortex_agent', 'se.data.crm');

USE DATABASE SE;
USE SCHEMA DATA;
create or replace agent CRM
	profile='{"display_name":"CRM Agent"}'
	from specification
	$$
	models:
	  orchestration: "claude-sonnet-4-5"
	orchestration: {}
	tools:
	  - tool_spec:
	      type: "cortex_analyst_text_to_sql"
	      name: "CRM_ANALYSIS_SEMANTIC_VIEW"
	      description: "TABLE1: ITERABLE_CRM_REPORTING\n- Database: SE, Schema: DATA\n\
	        - Contains email marketing campaign activities and message delivery events\
	        \ from 2021 onwards across email, push, in-app and web push channels. Each\
	        \ record represents a single send event with campaign details, recipient information,\
	        \ and performance metrics.\n- Supports analysis of campaign performance through\
	        \ metrics like sends, opens, clicks, bookings and margin with both 1-day and\
	        \ 7-day attribution windows. Includes automated campaign classification and\
	        \ territory mapping.\n- LIST OF COLUMNS: AME_CALCULATED_CAMPAIGN_NAME, CAMPAIGN_GROUP,\
	        \ CAMPAIGN_ID, COMBINED_EMAIL_NAME, CRM_CHANNEL_TYPE, CURRENT_AFFILIATE_TERRITORY,\
	        \ EMAIL_TYPE, IS_AUTOMATED_CAMPAIGN, MAPPED_PLATFORM, MESSAGE_ID, RFV_SEGMENT,\
	        \ SHIRO_USER_ID (links to SE_USER_ATTRIBUTES), TERRITORY_GROUP, SEND_EVENT_DATE\
	        \ (links to SE_CALENDAR), BOOKINGS_1D_LND, BOOKINGS_7D_LND, EMAIL_CLICKS_1D,\
	        \ EMAIL_CLICKS_7D, EMAIL_OPENS_1D, EMAIL_OPENS_7D, EMAIL_SENDS, MARGIN_GBP_1D_LND,\
	        \ MARGIN_GBP_7D_LND, UNIQUE_EMAIL_CLICKS_1D, UNIQUE_EMAIL_CLICKS_7D, UNIQUE_EMAIL_OPENS_1D,\
	        \ UNIQUE_EMAIL_OPENS_7D\n\nTABLE2: SE_CALENDAR\n- Database: SE, Schema: DATA\n\
	        - Contains calendar dates with temporal attributes including year, month,\
	        \ week, and day information. Provides year-over-year comparison capabilities\
	        \ and SE-specific week numbering system.\n- Enables time-based analysis and\
	        \ period comparisons for CRM performance reporting across different time dimensions.\n\
	        - LIST OF COLUMNS: DATE_VALUE (primary key), DAY_NAME, DAY_OF_MONTH, DAY_OF_WEEK,\
	        \ MONTH_NAME, SE_WEEK, SE_YEAR, YEAR, WEEK_START\n\nTABLE3: SE_TERRITORY\n\
	        - Database: SE, Schema: DATA\n- Contains sales territory configuration with\
	        \ operational settings and localization details. Each record represents a\
	        \ territory with identification and country mapping.\n- Supports territory-based\
	        \ analysis and regional performance comparisons for CRM campaigns.\n- LIST\
	        \ OF COLUMNS: NAME (primary key, links to CURRENT_AFFILIATE_TERRITORY in ITERABLE_CRM_REPORTING),\
	        \ COUNTRY_NAME\n\nTABLE4: SE_USER_ATTRIBUTES\n- Database: SE, Schema: DATA\n\
	        - Contains user profile information including geographic details, affiliate\
	        \ relationships, and communication preferences. Tracks opt-in status for different\
	        \ channels and app installation status.\n- Enables user segmentation and personalization\
	        \ analysis for CRM campaigns based on user characteristics and preferences.\n\
	        - LIST OF COLUMNS: SHIRO_USER_ID (primary key, links to ITERABLE_CRM_REPORTING),\
	        \ APP_PUSH_OPT_IN_STATUS, EMAIL_OPT_IN_STATUS, HAS_APP_INSTALLED, IS_TEST_USER,\
	        \ MAIN_AFFILIATE_BRAND, MEMBERSHIP_ACCOUNT_STATUS, SIGNUP_TSTAMP\n\nREASONING:\n\
	        This semantic view integrates CRM campaign data with user attributes, calendar\
	        \ dimensions, and territory information to provide comprehensive marketing\
	        \ performance analysis. The core table ITERABLE_CRM_REPORTING contains campaign\
	        \ execution data that links to user profiles through SHIRO_USER_ID, calendar\
	        \ dates through SEND_EVENT_DATE, and territories through CURRENT_AFFILIATE_TERRITORY.\
	        \ This enables multi-dimensional analysis of campaign performance across time\
	        \ periods, user segments, geographic regions, and communication channels.\n\
	        \nDESCRIPTION:\nThe CRM_ANALYSIS semantic view provides comprehensive email\
	        \ marketing campaign reporting from 2021 onwards, covering email, push, in-app\
	        \ and web push communications with automatic defaulting to email channel analysis.\
	        \ It integrates campaign performance data from the SE.DATA.ITERABLE_CRM_REPORTING\
	        \ table with user attributes, calendar dimensions, and territory information\
	        \ to enable multi-dimensional analysis. The view supports key metrics including\
	        \ sends, opens, clicks, unique interactions, bookings and margin with both\
	        \ 1-day and 7-day attribution windows, defaulting to 7-day caps for campaigns\
	        \ sent over a week ago. Users can analyze performance across different time\
	        \ periods, geographic territories (DACH, UK, ROW), campaign types, and user\
	        \ segments while filtering for Secret Escapes brand users by default unless\
	        \ specifically requesting other brands."
	tool_resources:
	  CRM_ANALYSIS_SEMANTIC_VIEW:
	    execution_environment:
	      query_timeout: 120
	      type: "warehouse"
	      warehouse: ""
	    semantic_view: "SE.DATA.CRM_ANALYSIS"

	$$;

                  SELECT GET_DDL('semantic_view', 'se.data.crm_analysis')
