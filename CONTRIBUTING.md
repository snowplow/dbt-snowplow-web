# Contributing to `dbt-snowplow-*`

`dbt-snowplow-*` are open source software. This means not only is the code available for you to view in it's entirety, but that you can contribute to the package in a multitude of ways. Whether you are a seasoned open source contributor or a first-time committer, we welcome and encourage you to contribute code (via Pull Request), documentation (via our docusaurus site), ideas (via Discussions for larger ideas, or issues for specific feature requests), or reporting bugs (via issues) to this project. Remember, fixing a typo makes you an Open Source Contributor. You can also contribute via topics on our [Discourse](https://discourse.snowplow.io/).

Before you start a load of work, please note that all Pull Requests (apart from cosmetic fixes like typos) should be associated with an issue that has been approved for development by a maintainer. This is to stop you doing lots of development that may not be accepted into the package for a variety of reasons. Make sure to either [raise an issue](/../../issues/new) yourself or look at the existing issues before starting any development.

1. [Contributing to `dbt-snowplow-*`](#contributing-to-dbt-snowplow-)
   1. [About this document](#about-this-document)
      1. [Notes](#notes)
   2. [Getting the code](#getting-the-code)
      1. [Installing git](#installing-git)
      2. [External contributors](#external-contributors)
      3. [Snowplow contributors](#snowplow-contributors)
   3. [Setting up an environment](#setting-up-an-environment)
   4. [Implementation guidelines](#implementation-guidelines)
   5. [Testing](#testing)
   6. [Adding CHANGELOG Entry](#adding-changelog-entry)
   7. [Submitting a Pull Request](#submitting-a-pull-request)

## About this document

This document serves as guide for contributing code changes to `dbt-snowplow-*` (this and similar repositories). It is not intended as a guide for using `dbt-snowplow-*`, and some pieces assume a level of familiarity with Python development (virtualenvs, `pip`, etc) and dbt package development. Specific code snippets in this guide assume you are using macOS or Linux and are comfortable with the command line.

### Notes

- **CLA:** If this is your first time contributing you will be asked to sign the Individual Contributor License Agreement. If you would prefer to read this in advance of submitting your Pull Request you can find it [here](https://docs.google.com/forms/d/e/1FAIpQLSd89YTDQ1XpTZbj3LpOkquV_h1Y8k9ay3iFbJsZsJrz18I23Q/viewform). If you are unable to sign the CLA, the `dbt-snowplow-*` maintainers will unfortunately be unable to merge any of your Pull Requests. We welcome you to participate in discussions, open issues, and comment on existing ones.
- **Branches:** All Pull Requests from community contributors should target the `main` branch (default) and the maintainers will create the appropriate branch to merge this into. Please let us know if you believe your changes are a breaking change or could be done as part of a patch release, if you are unsure that's fine just make that clear in your Pull Request.
- **Documentation:** The majority of the documentation for our dbt packages is in the core [Snowplow Docs](https://github.com/snowplow/documentation) and as such a Pull Request will need to be raised there to update any docs related to your change. Things such as the deployed dbt site are taken care of automatically.

## Getting the code

### Installing git

You will need `git` in order to download and modify the `dbt-snowplow-*` source code. On macOS, the best way to download git is to just install [Xcode](https://developer.apple.com/support/xcode/).

### External contributors

If you are not a member of the `snowplow` GitHub organization, you can contribute to `dbt-snowplow-*` by forking the relevant package repository. For a detailed overview on forking, check out the [GitHub docs on forking](https://help.github.com/en/articles/fork-a-repo). In short, you will need to:

1. Fork this repository
2. Clone your fork locally
3. Check out a new branch for your proposed changes
4. Push changes to your fork
5. Open a Pull Request against this repo from your forked repository

### Snowplow contributors

If you are a member of the `snowplow` GitHub organization, you will have push access to this repo. Rather than forking to make your changes, just clone the repository, check out a new branch, and push directly to that branch.

## Setting up an environment

Assuming you already have dbt installed, it will be beneficial to create a profile for any warehouse connections you have when it comes to testing the changes to your package. The easiest way to do this that will involve the least changes to the testing setup is to create an `integration_tests` profile and populate it with any connections you have to our supported warehouse types (redshift+postgres, databricks, snowflake, bigquery). 

**It is recommended you use a custom schema for integration tests.**

```yml
integration_tests:
  outputs:
    databricks:
      type: databricks
      ...
    snowflake:
      type: snowflake
      ...
    bigquery:
      type: bigquery
      ...
    redshift:
      type: redshift
      ...
    postgres:
      type: postgres
      ...
  target: postgres
```

## Implementation guidelines

In general we try to follow these rules of thumb, but there are possible exceptions:
- Dispatch any macro where it needs to support multiple warehouses. 
  - Use inheritance where possible i.e. only define a macro for `redshift` if it is different to `postgres`, the same for `databricks` and `spark`
- Where models need to be different across multiple warehouse types, ensure they are enabled based on the `target.type`
- Make use of macros (ours and dbt's) where possible to avoid duplication and to manage the differences between warehouses
  - Do not reinvent the wheel e.g. make use of [`type_*` macros](https://docs.getdbt.com/reference/dbt-jinja-functions/cross-database-macros#data-type-functions) instead of explicit datatypes
  - In the case where a macro may be useful outside of a specific package, we may make the choice to add it to `dbt-snowplow-utils` [repository](https://www.github.com/snowplow/dbt-snowplow-utils) instead
- Make use of the incremental logic as much as possible to avoid full-scanning large tables
- Where new functionality is being added, or you are touching existing functionality that does not have good/any test, add tests

## Testing

Once you're able to manually test that your code change is working as expected, it's important to run existing automated tests, as well as adding some new ones. These tests will ensure that:
- Your code changes do not unexpectedly break other established functionality
- Your code changes can handle all known edge cases
- The functionality you're adding will _keep_ working in the future

In general our packages all have similar structures, with an `integration_tests` folder that contains a `.scripts/integration_tests.sh` file. This script is run with 1 argument, the name of your `target` in the `integration_tests` profile e.g. `./integration_tests/.scripts/integration_tests.sh -d postgres` which will run all the tests on your postgres instance. This all means you don't need your own Snowplow data to run the tests.

Tests are of 1 of 2 kinds:
- Row count/equality tests; these ensure that the processed seed data from the package matches exactly an expected input seed file. If you have made no change to logic these should not fail, however if you have changed the logic you may need to edit the expected seed file, and add records to the events input seed file to cover the use case. In some cases it may make sense to add both expected and unexpected data to the test (i.e. to ensure a fix you have deployed actually fixes the issue you have seen).
- Macro based tests; these are more varied, sometimes checking the output sql from a macro or otherwise examining database objects. Look at existing tests for more details and for how to edit/create these.

To run the integration tests:
1. Ensure the `integration_tests` folder is your working directory (you may need to `cd integration_tests`)
2. Run `dbt run-operation post_ci_cleanup` to ensure a clean set of schemas (this will drop the schemas we use, so ensure your profile is only for these tests)
3. Run `./.scripts/integration_test.sh -d {target}` with your target name
4. Ensure all tests run successfully

If any tests fail, you should examine the outputs and either correct the test or correct your changes.

> If you do not have access to all warehouses do not worry, test what you can and the remainder will be run when you submit your Pull Request (once enabled by maintainers).

For specific details for running existing integration tests and adding new ones to this package see [integration_tests/README.md](integration_tests/README.md).

## Adding CHANGELOG Entry

You don't need to worry about which version your change will go into. Just create the changelog entry at the top of CHANGELOG.md, copying the style of those below, but populate the date and version numbers with `x`s and open your Pull Request against the `main` branch.

## Submitting a Pull Request

A  maintainer will review your Pull Request. They may suggest code revision for style or clarity, or request that you add unit or integration test(s). We promise these are good things and it's not personal, we all want to make sure the highest quality of work goes into the packages in a way that will be the least disruptive for our users.

Automated tests run via Github actions. If you're a first-time contributor, all tests (including code checks and unit tests) will require a maintainer to approve. You will not be able to see the output data of these tests, but we can share and explore any failures with you should there be any.

Once all tests are passing and your Pull Request has been approved, a maintainer will merge your changes into the active development branch. And that's it! You're now an Open Source Contributor!
