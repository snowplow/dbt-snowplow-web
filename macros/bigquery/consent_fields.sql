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
