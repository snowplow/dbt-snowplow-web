{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro core_web_vital_results_query(suffix) %}
  {{ return(adapter.dispatch('core_web_vital_results_query', 'snowplow_unified')(suffix)) }}
{%- endmacro -%}

{% macro default__core_web_vital_results_query(suffix) %}
  case when lcp{{suffix}} is null then 'not measurable'
    when  lcp{{suffix}} < 2.5 then 'good'
    when lcp{{suffix}} < 4 then 'needs improvement'
    else 'poor' end as lcp_result,

  case when fid{{suffix}} is null then 'not measurable'
    when fid{{suffix}} < 100 then 'good'
    when fid{{suffix}} < 300 then 'needs improvement'
    else 'poor' end as fid_result,

  case when cls{{suffix}} is null then 'not measurable'
    when cls{{suffix}} < 0.1 then 'good'
    when cls{{suffix}} < 0.25 then 'needs improvement'
    else 'poor' end as cls_result,

  case when ttfb{{suffix}} is null then 'not measurable'
    when ttfb{{suffix}} < 800 then 'good'
    when ttfb{{suffix}} < 1800 then 'needs improvement'
    else 'poor' end as ttfb_result,

  case when inp{{suffix}} is null then 'not measurable'
    when inp{{suffix}} < 200 then 'good'
    when inp{{suffix}} < 500 then 'needs improvement'
    else 'poor' end as inp_result

{% endmacro %}
