{#
Copyright (c) 2023-present Snowplow Analytics Ltd. All rights reserved.
This program is licensed to you under the Snowplow Community License Version 1.0,
and you may not use this file except in compliance with the Snowplow Community License Version 1.0.
You may obtain a copy of the Snowplow Community License Version 1.0 at https://docs.snowplow.io/community-license-1.0
#}

{% macro consent_fields() %}

  {% set consent_fields = [
      {'field': 'event_type', 'dtype': 'string'},
      {'field': 'basis_for_processing', 'dtype': 'string'},
      {'field': 'consent_url', 'dtype': 'string'},
      {'field': 'consent_version', 'dtype': 'string'},
      {'field': 'consent_scopes', 'dtype': 'string'},
      {'field': 'domains_applied', 'dtype': 'string'},
      {'field': 'gdpr_applies', 'dtype': 'string'}
    ] %}

  {{ return(consent_fields) }}

{% endmacro %}
