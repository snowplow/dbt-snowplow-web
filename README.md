[![early-release]][tracker-classification] [![License][license-image]][license] [![Discourse posts][discourse-image]][discourse]

![snowplow-logo](https://raw.githubusercontent.com/snowplow/dbt-snowplow-utils/main/assets/snowplow_logo.png)

# snowplow-web

This dbt package:

- Transforms and aggregates raw web event data collected from the [Snowplow JavaScript tracker][tracker-docs] into a set of derived tables: page views, sessions and users, plus an optional set of consent tables.
- Derives a mapping between user identifiers, allowing for 'session stitching' and the development of a single customer view.
- Processes **all web events incrementally**. It is not just constrained to page view events - any custom events you are tracking will also be incrementally processed.
- Is designed in a modular manner, allowing you to easily integrate your own custom dbt models into the incremental framework provided by the package.

Please refer to the [doc site](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-models/dbt-web-data-model/) for a full breakdown of the package.

### Getting Started

The easiest way to get started is to follow our [QuickStart guide](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-quickstart/web/), or to use our [Advanced Analytics for Web Accelerator](https://docs.snowplow.io/accelerators/web/) which includes steps for setting up tracking as well as modeling, and our [Consent Tracking for Marketing Accelerator](https://docs.snowplow.io/accelerators/consent/) specifically for our Consent Management Platform models.

### Adapter Support

The latest version of the snowplow-web package supports BigQuery, Databricks, Redshift, Snowflake & Postgres. For previous versions see our [package docs](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/).

### Requirements

- A dataset of web events from the [Snowplow JavaScript tracker][tracker-docs] must be available in the database.
- Have the [`webPage` context][webpage-context] enabled.
- dbt-core version 1.4.0 or greater
- You must be using RDB Loader v4.0.0 and above, or BigQuery Loader v1.0.0 and above, to ensure your data has the `load_tstamp` column. If you are not using these versions, or are using the Postgres loader, you will need to set `snowplow__enable_load_tstamp` to false in your` dbt_project.yml` and will not be able to use the consent models.

### Installation

Check [dbt Hub](https://hub.getdbt.com/snowplow/snowplow_web/latest/) for the latest installation instructions.

### Configuration & Operation

Please refer to the [doc site](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/) for details on how to [configure](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-configuration/web/) and [run](https://docs.snowplow.io/docs/modeling-your-data/modeling-your-data-with-dbt/dbt-quickstart/web/) the package.

### Models

The package contains multiple staging models however the output models are as follows:

| Model                             | Description                                                                                                  |
| --------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| snowplow_web_page_views           | A table of page views, including engagement metrics such as scroll depth and engaged time.                   |
| snowplow_web_sessions             | An aggregated table of session events, including conversions [Optional], grouped on `domain_sessionid`.      |
| snowplow_web_users                | An aggregated table of sessions to a user level, grouped on `domain_userid`.                                 |
| snowplow_web_user_mapping         | Provides a mapping between user identifiers, `domain_userid` and `user_id`.                                  |
| snowplow_web_consent_log          | [Optional] Incremental table showing the audit trail of consent and Consent Management Platform (cmp) events |
| snowplow_web_consent_users        | [Optional] By user consent stats                                                                             |
| snowplow_web_consent_totals       | [Optional] Summary of the latest consent status as per consent version                                       |
| snowplow_web_consent_scope_status | [Optional] Aggregate of current number of users consented to each consent scope                              |
| snowplow_web_consent_cmp_stats    | [Optional] Used for modeling cmp_visible events and related metrics                                          |
| snowplow_web_consent_versions     | [Optional] Used to keep track of each consent version and its validity                                       |

Please refer to the [dbt doc site](https://snowplow.github.io/dbt-snowplow-web/#!/overview/snowplow_web) for details on the model output tables.

# Join the Snowplow community

We welcome all ideas, questions and contributions!

For support requests, please use our community support [Discourse][discourse] forum.

If you find a bug, please report an issue on GitHub.

# Copyright and license

The snowplow-web package is Copyright 2021-2023 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0][license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[license]: http://www.apache.org/licenses/LICENSE-2.0
[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[tracker-classification]: https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/tracker-maintenance-classification/
[early-release]: https://img.shields.io/static/v1?style=flat&label=Snowplow&message=Early%20Release&color=014477&labelColor=9ba0aa&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAeFBMVEVMaXGXANeYANeXANZbAJmXANeUANSQAM+XANeMAMpaAJhZAJeZANiXANaXANaOAM2WANVnAKWXANZ9ALtmAKVaAJmXANZaAJlXAJZdAJxaAJlZAJdbAJlbAJmQAM+UANKZANhhAJ+EAL+BAL9oAKZnAKVjAKF1ALNBd8J1AAAAKHRSTlMAa1hWXyteBTQJIEwRgUh2JjJon21wcBgNfmc+JlOBQjwezWF2l5dXzkW3/wAAAHpJREFUeNokhQOCA1EAxTL85hi7dXv/E5YPCYBq5DeN4pcqV1XbtW/xTVMIMAZE0cBHEaZhBmIQwCFofeprPUHqjmD/+7peztd62dWQRkvrQayXkn01f/gWp2CrxfjY7rcZ5V7DEMDQgmEozFpZqLUYDsNwOqbnMLwPAJEwCopZxKttAAAAAElFTkSuQmCC
[tracker-docs]: https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/javascript-trackers/
[webpage-context]: https://docs.snowplow.io/docs/collecting-data/collecting-from-own-applications/javascript-trackers/javascript-tracker/javascript-tracker-v3/tracker-setup/initialization-options/#adding-predefined-contexts
[dbt-package-docs]: https://docs.getdbt.com/docs/building-a-dbt-project/package-management
[discourse-image]: https://img.shields.io/discourse/posts?server=https%3A%2F%2Fdiscourse.snowplow.io%2F
[discourse]: http://discourse.snowplow.io/
