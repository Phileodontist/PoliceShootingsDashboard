## Police Shootings Dataset
| Column      | Type        | Example     | Description | Possible Values |
| ----------- | ----------- | ----------- | ----------- | ----------- |
| id          | int         | 5           | a unique identifier for each victim | |
| name        | str         | John Paul Quintero | the name of the victim |  |
| date            | date    | 2015-01-03         | the date of the fatal shooting in YYYY-MM-DD format |  |
| armed_with       | str         | unarmed            | indicates that the victim was armed with some sort of implement that a police officer believed could inflict harm | `gun`, `knife`, `other` |
| age         | int         | 23                 | the age of the victim |  |
| gender      | char        | M                  | the gender of the victim. The Post identifies victims by the gender they identify with if reports indicate that it differs from their biological sex | `M`, `F` |
| race        | char        | b;h                  |  The race and ethnicity (where known) of the victim | - `W`: White<br>- `B`: Black<br>- `A`: Asian heritage<br>- `N`: Native American<br>- `H`: Hispanic <br>- `O`: Other<br>- `--`: Unknown  |
| race_source        | str         |   public_record        |  Sourcing methodology for victim race data | `public_record`, `not_available` |
| city        | str         | Wichita            | the municipality where the fatal shooting took place. Note that in some cases this field may contain a county name if a more specific municipality is unavailable or unknown |  |
| state       | str         | KS                 | two-letter postal code abbreviation |  |
| was_mental_illness_related   | bool               | FALSE           | News reports have indicated the victim had a history of mental health issues, expressed suicidal intentions or was experiencing mental distress at the time of the shooting. | `TRUE`, `FALSE` |
| threat_type    | str     | other              | The general criteria for the attack label was that there was the most direct and immediate threat to life | `shoot`, `point`, `attack` |
| flee_status        | str         | Not fleeing        | News reports have indicated the victim was moving away from officers | `foot`, `car`, `other` |
| body_camera | bool        | FALSE              | News reports have indicated an officer was wearing a body camera and it may have recorded some portion of the incident | `TRUE`, `FALSE` |
| longitude   | float       | -97.281            | The location of the shooting expressed as WGS84 coordinates |  |
| latitude    | float       | 37.695             | The location of the shooting expressed as WGS84 coordinates |  |
| location_precision    | str       | intersection              | Indicate the precision level of the location data which was geocoded to generate the record's coordinate data | `address`, `intersection`, `block`, `road` |
| agency_ids    | str       |   1;2           | List of agency ids to enable joining with the agencies csv |  |
