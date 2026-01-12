SELECT t.id,
       t.version,
       t.body_bg_color,
       t.body_bg_img,
       t.css_location,
       t.footer_text_color,
       t.header_bg_color,
       t.header_height,
       t.logo_font,
       t.logo_font_color,
       t.logo_font_size,
       t.logo_image,
       t.logo_image_position,
       t.logo_text,
       t.name,
       t.order_summary_bg_color,
       t.order_summary_bg_image,
       t.page_button_bg_color,
       t.page_button_text_color,
       t.page_title_text_color,
       t.section_border,
       t.subheader_text,
       t.about_us,
       t.contact,
       t.faq,
       t.privacy_policy,
       t.terms_and_conditions,
       t.sidebar,
       t.email_blurb_text,
       t.email_blurb_title,
       t.email_footer_signature,
       t.email_footer_text,
       t.email_footer_text_color,
       t.email_header_logo,
       t.email_header_text,
       t.email_page_bg_color,
       t.email_page_title,
       t.email_sale_button_bg_image,
       t.email_sale_button_text_color,
       t.email_sale_destination_color,
       t.email_sale_discount_color,
       t.email_sale_time_remaining_color,
       t.email_sale_title_color,
       t.email_title_color,
       t.email_on_sale_now_color,
       t.email_sale_call_to_action_text_color,
       t.email_sale_call_to_action_bg_image,
       t.application_name,
       t.footer_url,
       t.header_url,
       t.js_location,
       t.homepage_redirect_url,
       t.favicon_location,
       t.support_email,
       t.support_number,
       t.facebook_page_name,
       t.twitter_account,
       t.facebook_og_description,
       t.facebook_og_image_url,
       t.hosted_sales_page,
       t.email_accept_invitation_button,
       t.email_change_password_button,
       t.email_complete_purchase_button,
       t.email_legal_text,
       t.email_view_credits_button,
       t.email_view_current_sales_button,
       t.email_view_sale_button,
       t.tracking_link,
       t.google_plus_page,
       t.mobile_css_location,
       t.mobile_js_location,
       t.status,
       t.partner_company_name,
       t.disclaimer,
       t.cookie_policy,
       t.privacy_policy_summary,
       t.has_logo_with_subline,
       t.front_end_theme_location
FROM theme t;

dataset_task --include 'cms_mysql.theme' --operation ProductionIngestOperation --method 'run' --upstream --start '2021-09-08 00:30:00' --end '2021-09-08 00:30:00'
;
SELECT * FROM raw_vault_mvp_dev_robin.cms_mysql.theme;
DROP TABLE raw_vault_mvp_dev_robin.cms_mysql.theme;


python biapp/bau/hygiene/gen_hygiene_files.py \
    --data_source cms_mysql \
    --name theme \
    --primary_key_cols id \



python biapp/bau/hygiene/gen_hygiene_files.py \
    --data_source travelbird_mysql \
    --name offers_hotelproductlink \
    --primary_key_cols id \
    --new_record_col updated_at_dts \
    --detect_deleted_records


self_describing_task --include 'staging/hygiene/cms_mysql/theme.py'  --method 'run' --start '2021-09-08 00:00:00' --end '2021-09-08 00:00:00'
self_describing_task --include 'staging/hygiene_snapshots/cms_mysql/theme.py'  --method 'run' --start '2021-09-08 00:00:00' --end '2021-09-08 00:00:00'

SELECT * FROM se.data.se_user_attributes sua;
SELECT * FROM se.data_pii.se_user_attributes sua;

SELECT *
FROM snowflake.account_usage.query_history qh
WHERE qh.start_time BETWEEN '2021-09-09 16:30:00' AND '2021-09-09 20:30:00'
AND LOWER(qh.query_text) like '%se.data.se_user_attributes%';

