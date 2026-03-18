- Ensure you are using a role with CREATE AGENT privileges
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

;
USE ROLE ai_developer;

SELECT get_ddl('cortex_agent', 'se.data.trading');

ALTER AGENT TRADING RENAME TO BOOKING;

create or replace agent snowmind.agents.BOOKING
comment='The Trading Agent is your specialized "commerical brain" within SnowMind. It is designed to provide instant visibility into Secret Escapes’ booking performance, financial health, and the external factors influencing them.

By combining transaction-level data with our internal marketing and product calendars, the Trading Agent helps you monitor revenue, lead times, and customer loyalty in real-time.'
profile='{"display_name":"Trading Agent","avatar":"ChartAgentIcon","color":"var(--chartDim_3-x11sbcwy)"}'
from specification
$$
models:
  orchestration: "claude-sonnet-4-5"
orchestration: {}
instructions:
  response: "You are a specialized trading data assistant. Use the booking_analysis\
    \ tool to answer questions about trade bookings and volumes. The company is Secret\
    \ Escapes.\n"
  sample_questions:
    - question: "What was our daily revenue for the last complete week?"
    - question: "Show me product type performance for November 2025 and compare it\
        \ to October 2025."
    - question: "Show me the top 5 over performing and under performing posu cities\
        \ last month compared to the same month in previous year."
tools:
  - tool_spec:
      type: "cortex_analyst_text_to_sql"
      name: "booking_analysis_semantic_view"
      description: "TABLE1: DIM_SALE\n- Database: SE, Schema: DATA\n- This dimension\
        \ table stores comprehensive information about travel sales and accommodation\
        \ offers, including product details, supplier information, geographic locations,\
        \ and sales performance metrics. It serves as a master reference for all sale-related\
        \ attributes used in booking analysis.\n- The table enables analysis of destination\
        \ performance, product configurations (Hotel, Hotel Plus, 3PP, WRD, IHP variants),\
        \ regional patterns, and brand distribution across SE Brand and Travelist\
        \ platforms.\n- LIST OF COLUMNS: SE_SALE_ID (primary key), POSU_CITY (destination\
        \ city - links to booking destination analysis), POSU_CLUSTER (customer purchase\
        \ behavior categorization), POSU_CLUSTER_REGION (geographic sales region),\
        \ POSU_CLUSTER_SUB_REGION (provincial/island level geography), POSU_COUNTRY\
        \ (destination country for bookings), POSU_REGION (major market areas like\
        \ UK/Spain/DACH), POSU_SUB_REGION (granular areas like London/Balearic Islands),\
        \ PRODUCT_CONFIGURATION (distribution channel: Hotel/Hotel Plus/3PP/WRD/IHP),\
        \ PRODUCT_TYPE (Hotel/Package/WRD/Day Experience), SALE_ACTIVE (current bookability\
        \ status), SALE_NAME (hotel/offer title), SE_BRAND (SE Brand or Travelist),\
        \ SALE_START_DATE (offer launch timestamp)\n\nTABLE2: FACT_BOOKING\n- Database:\
        \ SE, Schema: DATA\n- This fact table contains all booking transactions with\
        \ comprehensive details about reservations, guest information, pricing, dates,\
        \ and booking behavior. Each record represents a single booking with status\
        \ tracking (live/cancelled/abandoned/other) and links to sales, users, and\
        \ calendar dimensions.\n- The table supports analysis of booking volumes,\
        \ revenue performance, customer loyalty patterns (via LIVE_BOOKING_INDEX),\
        \ lead times, cancellation rates, travel party composition, device platforms,\
        \ and domestic vs international travel patterns.\n- LIST OF COLUMNS: BOOKING_ID\
        \ (primary key, unique transaction identifier), SE_SALE_ID (foreign key to\
        \ DIM_SALE), SHIRO_USER_ID (foreign key to SE_USER_ATTRIBUTES for customer\
        \ data), TRANSACTION_ID (payment reference), BOOKING_STATUS_TYPE (live/cancelled/abandoned/other),\
        \ BOOKING_STATUS_TYPE_NET_IN_MONTH_CANCELLATIONS (adjusted status calculation),\
        \ SE_BRAND (SE Brand or Travelist), TERRITORY (customer's market/location),\
        \ DEVICE_PLATFORM (web/mobile/app/call_centre), BOOKING_INCLUDES_FLIGHT (boolean\
        \ for flight inclusion), CURRENCY (transaction currency code), ADULT_GUESTS\
        \ (number of adults), CHILD_GUESTS (number of children), INFANT_GUESTS (number\
        \ of infants), ROOMS (room count), NO_NIGHTS (length of stay), BOOKING_LEAD_TIME_DAYS\
        \ (days between booking and check-in), BOOKING_LEAD_TIME_CATEGORY (Last Minute/Short\
        \ Notice/Standard/Early/Super Early segments), DAYS_SINCE_PREVIOUS_LIVE_BOOKING\
        \ (rebooking interval), LIVE_BOOKING_INDEX (customer's booking sequence number\
        \ for loyalty analysis), BOOKER_LOYALTY_SEGMENT (First-Time/Second-Time/Regular/Frequent/VIP\
        \ based on booking count), BOOKER_RECENCY_SEGMENT (First Booking/Very Active/Active/Moderate/Infrequent/Dormant\
        \ based on days since last booking), LENGTH_OF_STAY_CATEGORY (Weekend/Short/Week/Extended/Long\
        \ Stay segments), PRICE_PER_NIGHT (average nightly rate), PRICE_PER_NIGHT_CATEGORY\
        \ (Budget/Standard/Mid-Range/Premium/Luxury/Ultra-Luxury segments), TRAVEL_PARTY_TYPE\
        \ (Solo/Couple/Group/Family composition), TRAVEL_TYPE (Domestic/International/Unknown),\
        \ GROSS_REVENUE_GBP_CONSTANT_CURRENCY (total booking revenue in constant GBP),\
        \ MARGIN_GROSS_OF_TOMS_GBP_CONSTANT_CURRENCY (gross profit after TOMS costs),\
        \ BOOKING_COMPLETED_DATE (transaction completion date), BOOKING_COMPLETED_TIMESTAMP\
        \ (full transaction timestamp), CHECK_IN_DATE (guest arrival date), CHECK_OUT_DATE\
        \ (guest departure date), CANCELLATION_DATE (cancellation timestamp if applicable)\n\
        \nTABLE3: SE_CALENDAR\n- Database: SE, Schema: DATA\n- This date dimension\
        \ table provides calendar attributes for time-based analysis, enabling period\
        \ comparisons, YTD/QTD/MTD calculations, and date-based filtering across all\
        \ fact tables. It contains one record per date with year, week, and month\
        \ attributes.\n- The table supports weekly and monthly aggregations, current\
        \ period identification, and temporal analysis patterns essential for booking\
        \ trend analysis and performance tracking.\n- LIST OF COLUMNS: DATE_VALUE\
        \ (primary key, full calendar date - joins to BOOKING_COMPLETED_DATE/CHECK_IN_DATE),\
        \ SE_YEAR (calendar year number), SE_WEEK (week number 1-52 within year),\
        \ MONTH_NAME (abbreviated month name), IS_CURRENT_WEEK (boolean flag for current\
        \ week), IS_CURRENT_MONTH (boolean flag for current month)\n\nTABLE4: SE_USER_ATTRIBUTES\n\
        - Database: SE, Schema: DATA\n- This customer dimension table contains user\
        \ demographics, acquisition information, marketing preferences, account status,\
        \ and engagement attributes. It stores one record per customer with details\
        \ about how they joined, their communication preferences, and their relationship\
        \ with various affiliates and marketing partners.\n- The table enables customer\
        \ segmentation, cohort analysis based on signup dates, attribution reporting\
        \ by affiliate/channel, and analysis of user engagement patterns across different\
        \ platforms and marketing touchpoints.\n- LIST OF COLUMNS: SHIRO_USER_ID (primary\
        \ key, unique customer identifier - links to FACT_BOOKING for transaction\
        \ history), MEMBERSHIP_ACCOUNT_STATUS (FULL_ACCOUNT/DEACTIVATED/DELETED/BLOCKED/UNSUBSCRIBED/PENDING),\
        \ IS_TEST_USER (boolean flag for internal test accounts), ACQUISITION_PLATFORM\
        \ (signup device: WEB/MOBILE_WEB/TABLET_WEB/IOS_APP_V3/ANDROID_APP_V3/MOBILE_WRAP),\
        \ HAS_APP_INSTALLED (boolean for mobile app installation), EMAIL_OPT_IN_STATUS\
        \ (daily/weekly/opted out communication preference), APP_PUSH_OPT_IN_STATUS\
        \ (opted in/opted out for push notifications), ORIGINAL_AFFILIATE_NAME (first\
        \ referral source at signup - never changes), MAIN_AFFILIATE_NAME (primary\
        \ engagement brand/market), CURRENT_AFFILIATE_NAME (most recent affiliate\
        \ association), SIGNUP_TSTAMP (account creation timestamp for cohort analysis)\n\
        \nREASONING:\nThis semantic view integrates four core tables to provide comprehensive\
        \ booking and sales analysis capabilities. The FACT_BOOKING table serves as\
        \ the central transaction record, connecting to DIM_SALE for destination and\
        \ product details, SE_USER_ATTRIBUTES for customer demographics and acquisition\
        \ data, and SE_CALENDAR for temporal analysis. The relationships enable multi-dimensional\
        \ analysis: bookings can be analyzed by destination attributes (via SE_SALE_ID\
        \ join), customer characteristics and acquisition channels (via SHIRO_USER_ID\
        \ join), and time periods (via BOOKING_COMPLETED_DATE join). Key analytical\
        \ capabilities include revenue performance tracking, customer loyalty segmentation\
        \ through LIVE_BOOKING_INDEX, cancellation rate analysis, lead time patterns,\
        \ destination popularity, product configuration performance, and marketing\
        \ attribution. The view supports both transactional queries (individual booking\
        \ details) and aggregated analysis (trends, comparisons, segmentation) across\
        \ booking volumes, revenue metrics, customer behavior, and geographic performance.\n\
        \nDESCRIPTION:\nThis semantic view provides comprehensive travel booking transaction\
        \ and sales analysis across the SE database (DATA schema). It centers on FACT_BOOKING\
        \ containing all reservation transactions with detailed guest information,\
        \ pricing, dates, and booking behavior, joined to DIM_SALE for destination\
        \ geography (POSU_CITY, POSU_COUNTRY, POSU_REGION) and product configurations\
        \ (Hotel, Hotel Plus, 3PP, WRD, IHP variants), SE_USER_ATTRIBUTES for customer\
        \ demographics and marketing attribution (acquisition platforms, affiliate\
        \ sources, communication preferences), and SE_CALENDAR for temporal analysis\
        \ with week/month attributes. The relationships enable analysis of booking\
        \ volumes and revenue trends by destination, customer loyalty patterns through\
        \ LIVE_BOOKING_INDEX and segmentation fields (BOOKER_LOYALTY_SEGMENT, BOOKER_RECENCY_SEGMENT),\
        \ cancellation rates, booking lead times, travel party composition, device\
        \ platforms, and marketing channel performance. Key metrics include GROSS_REVENUE_GBP_CONSTANT_CURRENCY\
        \ for revenue analysis, MARGIN_GROSS_OF_TOMS_GBP_CONSTANT_CURRENCY for profitability,\
        \ and derived segments for length of stay, price tiers, and customer engagement\
        \ levels across SE Brand and Travelist platforms."
  - tool_spec:
      type: "cortex_search"
      name: "promo_calendar"
      description: "Secret Escapes initiates promotional campaigns where for a period\
        \ of time a product will be at a reduced rate. These are called promos.\n\n\
        This data is a list of all sales that are active within a promotion.\n\nThe\
        \ indexed column shows start date and end date of promotion. In-between these\
        \ dates the promo is live for that sale."
  - tool_spec:
      type: "web_search"
      name: "Web Search"
tool_resources:
  Web Search:
    max_results: 10
  booking_analysis_semantic_view:
    execution_environment:
      type: "warehouse"
      warehouse: ""
    semantic_view: "SNOWMIND.SEMANTIC_VIEWS.BOOKING_ANALYSIS"
  promo_calendar:
    id_column: "GSHEET_LINK"
    max_results: 100
    search_service: "SNOWMIND.SEARCH_SERVICES.PROMO_SEARCH_SERVICE"
    title_column: "CAMPAIGN_NAME"
$$;



SELECT get_ddl('cortex_agent', 'se.bi.sessions');


create or replace agent snowmind.agents.SESSIONS
comment='The Traffic Agent is your specialised AI companion for navigating Secret Escapes’ web/app behavioural data. It combines deep session-level analytics with real-world context to help you understand not just what is happening on our platforms, but why it’s happening.'
profile='{"display_name":"Traffic Agent","avatar":"PhoneAgentIcon"}'
from specification
$$
models:
  orchestration: "claude-sonnet-4-5"
orchestration: {}
instructions:
  response: "# IDENTITY & TONE\n- You are the \"Sessions Specialist,\" a core component\
    \ of the SnowMind intelligence layer.\n- Your expertise is in high-resolution\
    \ user behavior: funnels, device performance, and session-level attribution.\n\
    - Maintain a focused, analytical tone. Be precise with terminology (e.g., distinguish\
    \ between \"Unique SPVs\" and \"Total SPVs\").\n\n# ORCHESTRATION LOGIC (The \"\
    What\" before the \"Why\")\n- When investigating metric changes, follow this mandatory\
    \ two-step protocol:\n1. **The Numbers:** Call 'session_analysis' to quantify\
    \ the change (e.g., \"CVR dropped from 4% to 2% on iOS\").\n2. **The Context:**\
    \ Call 'product_release_event_calendar' using the relevant date range to find\
    \ contributing factors (e.g., \"A new app release occurred at the same time\"\
    ). These are product releases to our web and app.\n3. **The Context:** Call 'promos_event_calendar'\
    \ using the relevant date range to find contributing factors (e.g., \"A promotion\
    \ (promo) occurred at the same time\"). These are sale incentive promotions eg\
    \ flash sale.\n- Always synthesize these findings into a unified narrative. Do\
    \ not just list results from each tool separately.\n\n# FORMATTING & DATA DISPLAY\n\
    - Use Markdown tables for funnel steps (e.g., Searches -> SPVs -> BFVs -> Bookings).\n\
    - **Bold** significant outliers or anomalies in the data.\n- If the user asks\
    \ for \"yesterday,\" explicitly state the date you are reporting on to avoid confusion\
    \ across time zones.\n- Use standard Secret Escapes territory codes (e.g., UK,\
    \ DE, IT) in headers.\n\n# DOMAIN-SPECIFIC RULES\n- \"Conversion\" (CVR) always\
    \ refers to 'SESSION_CVR' unless the user specifies a funnel stage like 'SPV to\
    \ BFV'.\n- If a user asks about \"Drop-off,\" identify the specific stage in the\
    \ session_analysis funnel where the percentage decrease was greatest.\n- Filter\
    \ out internal/anomalous traffic automatically as per the tool definitions.\n\
    - A traditional funnel is described in the tool's custom settings.\n\n# ERROR\
    \ HANDLING\n- If 'session_analysis' returns no data for a requested segment, do\
    \ not attempt to search the 'event_calendar'. Instead, ask the user to verify\
    \ the platform or territory filter."
  orchestration: "# STRATEGIC PRIORITY\n- Your primary goal is to resolve user queries\
    \ by correlating structured metrics with unstructured business context.\n- Always\
    \ prioritize accuracy over speed. If a tool call fails, attempt to refine the\
    \ parameters (e.g., date formats) before giving up.\n\n# TOOL SELECTION LOGIC\n\
    1. **Quantitative Baseline:** For any query involving \"how many,\" \"conversion,\"\
    \ \"trend,\" or \"performance,\" you MUST call 'session_analysis' first.\n2. **Qualitative\
    \ Context:** If the 'session_analysis' results show a change (>5% variance) or\
    \ if the user asks \"why,\" you MUST call 'event_calendar' for the same date range.\n\
    3. **Refinement:** If the user mentions a specific territory (e.g., \"Germany\"\
    ), apply the 'territory' filter to both the Analyst and Search tool calls.\n\n\
    # HANDLING AMBIGUITY\n- If a user asks a vague question like \"How are we doing?\"\
    , default to 'session_analysis' for the last 7 days compared to the prior 7 days\
    \ for the 'SE Brand'.\n- If 'session_analysis' returns a SQL error, check your\
    \ semantic mappings and retry once with simplified filters.\n- Unless asked specifically,\
    \ always filter out current date, otherwise this will include incomplete day metrics.\n\
    \n# JOINING DATA (The SnowMind \"Bridge\")\n- You are responsible for \"joining\"\
    \ the outputs. Use the 'START_DATE' from the Analyst and the 'EVENT_DATE' from\
    \ Search to align your findings chronologically."
  sample_questions:
    - question: "How many sessions did we have by device over the last month"
    - question: "How has our conversion rate for PPC - Brand changed over the last\
        \ 12 months?"
    - question: "How many unique users did we have last week?"
tools:
  - tool_spec:
      type: "cortex_analyst_text_to_sql"
      name: "session_analysis"
      description: "SESSION_METRICS:\n- Database: SE, Schema: BI\n- This table captures\
        \ comprehensive user session data from a travel booking platform, tracking\
        \ user interactions from initial landing through conversion. It includes detailed\
        \ attribution data, device information, geographic location, and behavioral\
        \ metrics across the entire customer journey.\n- The table enables analysis\
        \ of conversion funnels, marketing channel performance, user engagement patterns,\
        \ and revenue attribution. It supports cohort analysis through user identification\
        \ and provides insights into abandoned cart behavior and cross-device usage\
        \ patterns.\n- LIST OF COLUMNS: ATTRIBUTED_USER_ID (unique user identifier),\
        \ BR_FAMILY (browser family), BR_NAME (browser name), CHANNEL_CATEGORY (marketing\
        \ channel grouping), DVCE_SCREENHEIGHT (screen height pixels), DVCE_SCREENWIDTH\
        \ (screen width pixels), FEATURE_FLAG_TEST_ARRAY (A/B test flags), FIRST_LOGIN_TYPE\
        \ (initial login method), GEO_CITY (session city), GEO_COUNTRY (session country),\
        \ GEO_LATITUDE (location latitude), GEO_LONGITUDE (location longitude), GEO_REGION_NAME\
        \ (geographic region), HAS_BOOKING (conversion flag), HAS_BOOKING_CATALOGUE\
        \ (catalogue booking flag), HAS_BOOKING_FORM_VIEW (checkout attempt flag),\
        \ HAS_BOOKING_FORM_VIEW_CATALOGUE (catalogue checkout flag), HAS_BOOKING_FORM_VIEW_HOTEL\
        \ (hotel checkout flag), HAS_BOOKING_FORM_VIEW_HOTEL_PLUS (hotel plus checkout\
        \ flag), HAS_BOOKING_FORM_VIEW_PACKAGE (package checkout flag), HAS_BOOKING_HOTEL\
        \ (hotel booking flag), HAS_BOOKING_HOTEL_PLUS (hotel plus booking flag),\
        \ HAS_BOOKING_PACKAGE (package booking flag), HAS_PAGE_LOAD_SEARCH (predetermined\
        \ search flag), HAS_PAY_BUTTON_CLICK (payment attempt flag), HAS_SEARCH (search\
        \ activity flag), HAS_SPV (product view flag), HAS_SPV_CATALOGUE (catalogue\
        \ view flag), HAS_SPV_HOTEL (hotel view flag), HAS_SPV_HOTEL_PLUS (hotel plus\
        \ view flag), HAS_SPV_PACKAGE (package view flag), HAS_USER_SEARCH (manual\
        \ search flag), IS_ABANDONED_CART_SESSION (cart abandonment indicator), IS_SE_INTERNAL_TOUCH\
        \ (employee session flag), LANDING_APP_STATE (app foreground/background),\
        \ LANDING_PAGE_CATEGORY (landing page type), LAST_NON_DIRECT_CHANNEL_CATEGORY\
        \ (attribution channel group), LAST_NON_DIRECT_TOUCH_MKT_CHANNEL (marketing\
        \ channel), LAST_PAID_CHANNEL_CATEGORY (paid channel group), LAST_PAID_TOUCH_MKT_CHANNEL\
        \ (paid marketing channel), LOGIN_TYPES_LIST (login methods used), OS_FAMILY\
        \ (operating system family), OS_MANUFACTURER (OS manufacturer), OS_NAME (operating\
        \ system name), PLATFORM (device platform category), SESSION_DEPTH_CATEGORY\
        \ (engagement depth classification), SESSION_FUNNEL_ENGAGEMENT (conversion\
        \ funnel stage), SE_USER_ID (Secret Escapes user ID - links to SHIRO_USER_ID\
        \ in SE_USER_ATTRIBUTES), SORT_BY_SEARCHES (sort functionality usage), STITCHED_IDENTITY_TYPE\
        \ (identity resolution method), TOUCH_AFFILIATE_TERRITORY (affiliate region),\
        \ TOUCH_EXIT_PAGEPATH (exit page URL), TOUCH_EXPERIENCE (device type), TOUCH_HOSTNAME\
        \ (website domain), TOUCH_ID (unique session identifier), TOUCH_LANDING_PAGE\
        \ (landing page URL), TOUCH_LANDING_PAGEPATH (landing page path), TOUCH_LANDING_PAGE_UTM_CAMPAIGN\
        \ (UTM campaign parameter), TOUCH_LANDING_PAGE_UTM_CONTENT (UTM content parameter),\
        \ TOUCH_LANDING_PAGE_UTM_SOURCE (UTM source parameter), TOUCH_LOGGED_IN (login\
        \ status), TOUCH_MKT_CHANNEL (last click channel), TOUCH_REFERRER_HOSTNAME\
        \ (referring domain), TOUCH_REFERRER_URL (referrer URL), TOUCH_SE_BRAND (SE\
        \ Group brand), USERAGENT (browser user agent), USER_IPADDRESS (user IP address),\
        \ BOOKINGS (booking count), BOOKINGS_CATALOGUE (catalogue booking count),\
        \ BOOKINGS_HOTEL (hotel booking count), BOOKINGS_HOTEL_PLUS (hotel plus booking\
        \ count), BOOKINGS_PACKAGE (package booking count), BOOKING_FORM_VIEWS (checkout\
        \ page views), BOOKING_FORM_VIEWS_CATALOGUE (catalogue checkout views), BOOKING_FORM_VIEWS_HOTEL\
        \ (hotel checkout views), BOOKING_FORM_VIEWS_HOTEL_PLUS (hotel plus checkout\
        \ views), BOOKING_FORM_VIEWS_PACKAGE (package checkout views), LOGIN_TYPES_COUNT\
        \ (login method variety), MARGIN_GBP (profit in British Pounds), MAX_PRICE_FILTER_SEARCHES\
        \ (maximum price filter), MIN_PRICE_FILTER_SEARCHES (minimum price filter),\
        \ PAGE_LOAD_SEARCHES (predetermined search count), PAY_BUTTON_CLICKS (payment\
        \ button clicks), SEARCHES (total search count), SPVS (sale page views), SPVS_CATALOGUE\
        \ (catalogue page views), SPVS_HOTEL (hotel page views), SPVS_HOTEL_PLUS (hotel\
        \ plus page views), SPVS_PACKAGE (package page views), TOUCH_DURATION_SECONDS\
        \ (session duration), TOUCH_EVENT_COUNT (interaction count), UNIQUE_SPVS (unique\
        \ product views), UNIQUE_SPVS_CATALOGUE (unique catalogue views), UNIQUE_SPVS_HOTEL\
        \ (unique hotel views), UNIQUE_SPVS_HOTEL_PLUS (unique hotel plus views),\
        \ UNIQUE_SPVS_PACKAGE (unique package views), USER_SEARCHES (manual search\
        \ count), TOUCH_END_TSTAMP (session end time), TOUCH_START_DATE (session date),\
        \ TOUCH_START_TSTAMP (session start time)\n\nSE_CALENDAR:\n- Database: SE,\
        \ Schema: DATA\n- This table provides a comprehensive calendar dimension for\
        \ time-based analysis and reporting. It includes standard date attributes\
        \ along with business-specific week and year classifications used by Secret\
        \ Escapes for internal reporting cycles.\n- The calendar enables time-series\
        \ analysis, period-over-period comparisons, and supports business intelligence\
        \ reporting with custom week numbering systems. It facilitates date-based\
        \ filtering and aggregation across all business metrics.\n- LIST OF COLUMNS:\
        \ DATE_VALUE (calendar date), SE_WEEK (business week number), SE_YEAR (business\
        \ year)\n\nSE_USER_ATTRIBUTES:\n- Database: SE, Schema: DATA\n- This table\
        \ contains user profile and membership information, tracking user lifecycle\
        \ from acquisition through current status. It maintains historical acquisition\
        \ data and current membership states for all platform users.\n- The table\
        \ enables cohort analysis, user segmentation, and membership lifecycle tracking.\
        \ It supports analysis of user acquisition channels, territory-based performance,\
        \ and membership retention patterns across different user segments.\n- LIST\
        \ OF COLUMNS: COHORT_ID (cohort group identifier), COHORT_YEAR_MONTH (acquisition\
        \ period), CURRENT_AFFILIATE_TERRITORY (current user region), IS_TEST_USER\
        \ (test account flag), MAIN_AFFILIATE_NAME (primary affiliate partner), MEMBERSHIP_ACCOUNT_STATUS\
        \ (account status), MONGO_ACQUISITION_SOURCE_LATEST (recent acquisition channel),\
        \ ORIGINAL_AFFILIATE_NAME (initial affiliate partner), ORIGINAL_AFFILIATE_TERRITORY\
        \ (original user region), SHIRO_USER_ID (unique user identifier - links to\
        \ SE_USER_ID in SESSION_METRICS), SIGNUP_TSTAMP (registration timestamp)\n\
        \nREASONING:\nThis semantic view combines session-level behavioral data with\
        \ user attributes and calendar dimensions to provide comprehensive analytics\
        \ for a travel booking platform. The SESSION_METRICS table serves as the core\
        \ fact table containing detailed user interactions, while SE_USER_ATTRIBUTES\
        \ provides user context and SE_CALENDAR enables time-based analysis. The relationships\
        \ allow for deep analysis of user journeys from acquisition through conversion,\
        \ with the ability to segment by user characteristics and analyze trends over\
        \ time.\n\nDESCRIPTION:\nThe SESSION_ANALYSIS semantic view provides comprehensive\
        \ analytics for user behavior and conversion tracking on Secret Escapes' travel\
        \ booking platform, combining session-level interaction data with user attributes\
        \ and calendar dimensions. The core SESSION_METRICS table (SE.BI schema) captures\
        \ detailed user journeys including searches, product views, booking attempts,\
        \ and completed transactions, along with marketing attribution, device information,\
        \ and geographic data. This connects to SE_USER_ATTRIBUTES (SE.DATA schema)\
        \ which provides user profile information including acquisition cohorts, membership\
        \ status, and affiliate territories, enabling customer lifecycle analysis.\
        \ The SE_CALENDAR table (SE.DATA schema) supports time-based analysis with\
        \ business-specific date dimensions. Together, these tables enable analysis\
        \ of conversion funnels, marketing channel performance, user engagement patterns,\
        \ abandoned cart behavior, cohort analysis, and revenue attribution across\
        \ the entire customer journey from acquisition to conversion."
  - tool_spec:
      type: "cortex_search"
      name: "product_release_event_calendar"
      description: "Searches the se event calendar dataset to return product releases.\n\
        \nNote that product releases are often relevant to a certain territory(ies)\
        \ and certain device platforms"
  - tool_spec:
      type: "cortex_search"
      name: "promos_event_calendar"
      description: "Searches the se event calendar dataaset to return promos or promotions"
tool_resources:
  product_release_event_calendar:
    columns_and_descriptions:
      CALENDAR_SOURCE:
        description: ""
        filterable: true
        searchable: false
        type: "TEXT"
    filter:
      '@eq':
        CALENDAR_SOURCE: "product_release"
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
    execution_environment:
      type: "warehouse"
      warehouse: ""
    semantic_view: "SNOWMIND.SEMANTIC_VIEWS.TRAFFIC_SEMANTIC_VIEW"
$$;