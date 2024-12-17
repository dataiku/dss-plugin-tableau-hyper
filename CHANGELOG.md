# Changelog
## [Version 0.2.6](https://github.com/dataiku/dss-plugin-tableau-hyper/releases/tag/v0.2.6) - Feature release - 2024-12-06

- Fix: For datasets with large numbers of columns (approximately 150 and over) and over (approximately) 90,000 rows the export/upload hyper file did not contain the table exported. This led to the failure of uploads to Tableau and the export of unusable hyper files. This was due to a wrongly ordered close statements.

## [Version 0.2.5](https://github.com/dataiku/dss-plugin-tableau-hyper/releases/tag/v0.2.5) - Feature release - 2024-06-04

- Fix: Correctly compute the total number of pages needed to query projects from a Tableau server to avoid invalid page number errors

## [Version 0.2.4](https://github.com/dataiku/dss-plugin-tableau-hyper/releases/tag/v0.2.4) - Feature release - 2024-05-24

- Chore: Removed usage of `.cache` directory, letting `tempfile` handle the location of the temporary file
- Chore: Added Jenkinsfile to trigger integration tests as part of our plugin CI

## [Version 0.2.3](https://github.com/dataiku/dss-plugin-tableau-hyper/releases/tag/v0.2.3) - Feature release - 2024-05-21

- Fix: issue with authentication selection on legacy flows

## [Version 0.2.2](https://github.com/dataiku/dss-plugin-tableau-hyper/releases/tag/v0.2.2) - Feature release - 2023-10-18

- Feature: add secure personal access token preset 
- Fix: issue with date columns containing empty cells
- Fix: properly close resources if an error occcurs while writing the last lines of the .hyper file

## [Version 0.2.1](https://github.com/dataiku/dss-plugin-tableau-hyper/releases/tag/v0.2.1) - Feature release - 2023-06-21

- Feature: add secure personal preset

## [Version 0.1.6](https://github.com/dataiku/dss-plugin-tableau-hyper/releases/tag/v0.1.6) - Feature release - 2022-11-14

- Feature: add a project selector

## Version 0.1.5 - Dependencies upgrade release - 2021-12-06

- Dependencies: upgrade packages to `tableauhyperapi-0.0.13287` and  `tableauserverclient-0.15.0`

## Version 0.1.4 - Bugfix release - 2021-03-26

- Bugfix: map DSS `bigint`, `smallint` and `tinyint`types to their valid Tableau's SQL types. 
