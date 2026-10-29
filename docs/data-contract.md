# CDC data contract

Verified on October 29, 2026 against [CDC metadata](https://data.cdc.gov/api/views/atcp-73re.json) and the [records API](https://data.cdc.gov/resource/atcp-73re.json).

## Source fields

The dataset contains site/pathogen/reporting-week observations. Anonymous site IDs support site filtering. Facility names and coordinates are not present.

| CDC field | Meaning |
| --- | --- |
| state_territory | Full jurisdiction name in observed rows |
| counties_served | Nullable service-area description |
| site | Anonymous site identifier |
| population_served | Estimated population, not infections |
| source | Original reporting-source attribution |
| site_wval | Numeric wastewater activity value |
| site_wval_category | Published activity category |
| date_included_in_wval | Source inclusion date |
| week_end | Reporting-week ending date |
| pathogen_target | Published pathogen name |
| date_updated | Source processing timestamp; timezone unverified |

JSON numeric fields arrive as strings. Parse them at the ingestion boundary. Preserve missing optional values as null, never zero.

The exact pathogen values are `SARS-CoV-2`, `Influenza A virus`, and `RSV`. The UI can label the first COVID-19.

Observed categories are Very Low, Low, Moderate, High, and Very High. Metadata also describes limited or unavailable data. Preserve unknown source categories rather than inventing a numerical interpretation.

## Identity and coverage

Use site, pathogen_target, and week_end as the natural key. The inspected complete source contained no duplicate groups for this key. Validate uniqueness again for each downloaded extract.

The demo selects up to 400 most recent eligible observations per pathogen, ordered by reporting week and site. This is a bounded extract, not complete national coverage. The manifest records exact counts and URLs.

The eligibility policy excludes source strings containing WastewaterSCAN. CDC lists additional attribution, license, and contact requirements for this partner's data in its [program description](https://www.cdc.gov/wastewater/about/index.html). Preserve source attribution and keep downloaded data out of Git.

## Revision and interpretation

CDC publishes weekly updates and may revise past observations. Its [WVAL methodology](https://www.cdc.gov/wastewater/about/wval.html) includes historical recalculation and pathogen-specific category thresholds.

The initial application loads one frozen extract and does not claim to stay current. Do not treat date_updated as a verified per-record change cursor.

Use the source category directly. WVAL is a wastewater activity indicator, not an infection count. Counted rows represent observations; one site may appear for several pathogens or weeks. Do not present the demo's grouped counts as CDC's official state activity statistics.

Do not derive a plant name or map coordinate from a county description. Keep the source's anonymous identifiers.
