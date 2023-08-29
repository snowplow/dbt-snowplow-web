{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro channel_group_query() %}
  {{ return(adapter.dispatch('channel_group_query', 'snowplow_unified')()) }}
{% endmacro %}


{% macro bigquery__channel_group_query() %}
case
   when lower(trim(mkt_source)) = '(direct)' and lower(trim(mkt_medium)) in ('(not set)', '(none)') then 'Direct'
   when lower(trim(mkt_medium)) like '%cross-network%' then 'Cross-network'
   when regexp_contains(trim(mkt_medium), r'(?i)^(.*cp.*|ppc|retargeting|paid.*)$') then
      case
         when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
            or regexp_contains(trim(mkt_campaign), r'(?i)^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Paid Shopping'
         when upper(source_category) = 'SOURCE_CATEGORY_SEARCH' then 'Paid Search'
         when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL' then 'Paid Social'
         when upper(source_category) = 'SOURCE_CATEGORY_VIDEO' then 'Paid Video'
         else 'Paid Other'
      end
   when lower(trim(mkt_medium)) in ('display', 'banner', 'expandable', 'interstitial', 'cpm') then 'Display'
   when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
      or regexp_contains(trim(mkt_campaign), r'(?i)^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Organic Shopping'
   when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL' or lower(trim(mkt_medium)) in ('social', 'social-network', 'sm', 'social network', 'social media') then 'Organic Social'
   when upper(source_category) = 'SOURCE_CATEGORY_VIDEO'
      or regexp_contains(trim(mkt_medium), r'(?i)^(.*video.*)$') then 'Organic Video'
   when upper(source_category) = 'SOURCE_CATEGORY_SEARCH' or lower(trim(mkt_medium)) = 'organic' then 'Organic Search'
   when lower(trim(mkt_medium)) in ('referral', 'app', 'link') then 'Referral'
   when lower(trim(mkt_source)) in ('email', 'e-mail', 'e_mail', 'e mail') or lower(trim(mkt_medium)) in ('email', 'e-mail', 'e_mail', 'e mail') then 'Email'
   when lower(trim(mkt_medium)) = 'affiliate' then 'Affiliates'
   when lower(trim(mkt_medium)) = 'audio' then 'Audio'
   when lower(trim(mkt_source)) = 'sms' or lower(trim(mkt_medium)) = 'sms' then 'SMS'
   when lower(trim(mkt_medium)) like '%push' or regexp_contains(trim(mkt_medium), r'(?i).*(mobile|notification).*') or lower(trim(mkt_source)) = 'firebase' then 'Mobile Push Notifications'
   else 'Unassigned'
end
{% endmacro %}

{% macro default__channel_group_query() %}
case
   when lower(trim(mkt_source)) = '(direct)' and lower(trim(mkt_medium)) in ('(not set)', '(none)') then 'Direct'
   when lower(trim(mkt_medium)) like '%cross-network%' then 'Cross-network'
   when regexp_like(lower(trim(mkt_medium)), '^(.*cp.*|ppc|retargeting|paid.*)$') then
      case
         when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
            or regexp_like(lower(trim(mkt_campaign)), '^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Paid Shopping'
         when upper(source_category) = 'SOURCE_CATEGORY_SEARCH' then 'Paid Search'
         when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL' then 'Paid Social'
         when upper(source_category) = 'SOURCE_CATEGORY_VIDEO' then 'Paid Video'
         else 'Paid Other'
      end
   when lower(trim(mkt_medium)) in ('display', 'banner', 'expandable', 'intersitial', 'cpm') then 'Display'
   when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
      or regexp_like(lower(trim(mkt_campaign)), '^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Organic Shopping'
   when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL' or lower(trim(mkt_medium)) in ('social', 'social-network', 'sm', 'social network', 'social media') then 'Organic Social'
   when upper(source_category) = 'SOURCE_CATEGORY_VIDEO'
      or regexp_like(lower(trim(mkt_medium)), '^(.*video.*)$') then 'Organic Video'
   when upper(source_category) = 'SOURCE_CATEGORY_SEARCH' or lower(trim(mkt_medium)) = 'organic' then 'Organic Search'
   when lower(trim(mkt_medium)) in ('referral', 'app', 'link') then 'Referral'
   when lower(trim(mkt_source)) in ('email', 'e-mail', 'e_mail', 'e mail') or lower(trim(mkt_medium)) in ('email', 'e-mail', 'e_mail', 'e mail') then 'Email'
   when lower(trim(mkt_medium)) = 'affiliate' then 'Affiliates'
   when lower(trim(mkt_medium)) = 'audio' then 'Audio'
   when lower(trim(mkt_source)) = 'sms' or lower(trim(mkt_medium)) = 'sms' then 'SMS'
   when lower(trim(mkt_medium)) like '%push' or regexp_like(lower(trim(mkt_medium)), '.*(mobile|notification).*') or lower(trim(mkt_source)) = 'firebase' then 'Mobile Push Notifications'
   else 'Unassigned'
end
{% endmacro %}

{% macro redshift__channel_group_query() %}
case
   when lower(trim(mkt_source)) = '(direct)' and lower(trim(mkt_medium)) in ('(not set)', '(none)') then 'Direct'
   when lower(trim(mkt_medium)) like '%cross-network%' then 'Cross-network'
   when regexp_instr(lower(trim(mkt_medium)), '^(.*cp.*|ppc|retargeting|paid.*)$') then
      case
         when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
            or regexp_instr(lower(trim(mkt_campaign)), '^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Paid Shopping'
         when upper(source_category) = 'SOURCE_CATEGORY_SEARCH' then 'Paid Search'
         when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL' then 'Paid Social'
         when upper(source_category) = 'SOURCE_CATEGORY_VIDEO' then 'Paid Video'
         else 'Paid Other'
      end
   when lower(trim(mkt_medium)) in ('display', 'banner', 'expandable', 'intersitial', 'cpm') then 'Display'
   when upper(source_category) = 'SOURCE_CATEGORY_SHOPPING'
      or regexp_instr(lower(trim(mkt_campaign)), '^(.*(([^a-df-z]|^)shop|shopping).*)$') then 'Organic Shopping'
   when upper(source_category) = 'SOURCE_CATEGORY_SOCIAL' or lower(trim(mkt_medium)) in ('social', 'social-network', 'sm', 'social network', 'social media') then 'Organic Social'
   when upper(source_category) = 'SOURCE_CATEGORY_VIDEO'
      or regexp_instr(lower(trim(mkt_medium)), '^(.*video.*)$') then 'Organic Video'
   when upper(source_category) = 'SOURCE_CATEGORY_SEARCH' or lower(trim(mkt_medium)) = 'organic' then 'Organic Search'
   when lower(trim(mkt_medium)) in ('referral', 'app', 'link') then 'Referral'
   when lower(trim(mkt_source)) in ('email', 'e-mail', 'e_mail', 'e mail') or lower(trim(mkt_medium)) in ('email', 'e-mail', 'e_mail', 'e mail') then 'Email'
   when lower(trim(mkt_medium)) = 'affiliate' then 'Affiliates'
   when lower(trim(mkt_medium)) = 'audio' then 'Audio'
   when lower(trim(mkt_source)) = 'sms' or lower(trim(mkt_medium)) = 'sms' then 'SMS'
   when lower(trim(mkt_medium)) like '%push' or regexp_instr(lower(trim(mkt_medium)), '.*(mobile|notification).*') or lower(trim(mkt_source)) = 'firebase' then 'Mobile Push Notifications'
   else 'Unassigned'
end
{% endmacro %}
