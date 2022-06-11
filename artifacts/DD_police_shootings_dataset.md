## Police Shootings Dataset
| Column      | Type        | Example     | Description | Possible Values |
| ----------- | ----------- | ----------- | ----------- | ----------- |
| id          | int         | 5           | a unique identifier for each victim | |
| name        | str         | John Paul Quintero | the name of the victim |  |
| date            | date    | 2015-01-03         | the date of the fatal shooting in YYYY-MM-DD format |  |
| manner_of_death | str     | shot and Tasered   | Determines whether they were tased before being shot | `shot and Tasered`, `shot` |
| armed       | str         | unarmed            | indicates that the victim was armed with some sort of implement that a police officer believed could inflict harm |  |
| age         | int         | 23                 | the age of the victim |  |
| gender      | char        | M                  | the gender of the victim. The Post identifies victims by the gender they identify with if reports indicate that it differs from their biological sex | `M`, `F` |
| race        | char        | H                  | Race of the shot individual | `W: White, non-Hispanic`, `B: Black, non-Hispanic`, `A: Asian`, `N: Native American`, `H: Hispanic`,<br/> `O: Other`, `None: unknown` |
| city        | str         | Wichita            | the municipality where the fatal shooting took place. Note that in some cases this field may contain a county name if a more specific municipality is unavailable or unknown |  |
| state       | str         | KS                 | two-letter postal code abbreviation |  |
| signs_of_mental_illness   | bool               | FALSE           | News reports have indicated the victim had a history of mental health issues, expressed suicidal intentions or was experiencing mental distress at the time of the shooting. |  |
| threat_level    | str     | other              | The general criteria for the attack label was that there was the most direct and immediate threat to life |  |
| flee        | str         | Not fleeing        | News reports have indicated the victim was moving away from officers |  |
| body_camera | bool        | FALSE              | News reports have indicated an officer was wearing a body camera and it may have recorded some portion of the incident |  |
| longitude   | float       | -97.281            | The location of the shooting expressed as WGS84 coordinates |  |
| latitude    | float       | 37.695             | The location of the shooting expressed as WGS84 coordinates |  |
| is_geocoding_exact        | bool               | True           | Reflects the accuracy of the coordinates. true means that the coordinates are for the location of the shooting (within approximately 100 meters), while false means that coordinates are for the centroid of a larger region, such as the city or county where the shooting happened |  |
