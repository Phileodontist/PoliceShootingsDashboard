# Police Shootings Dashboard ETL Pipeline
***

## Synopsis
Interested in how racial and economic disparities correlate to crime and by extension police shootings, the conception of a police shootings dashboard to display all three side by side arose to get a better picture on the trend of police shootings throughout the United States.

#### Developer's Note
The main focus of this project is to pull correlated data from different sources into an accessible format. Considering that the primary goal in mind is to integrate data and to build an orchestrated data pipeline, analytics derived from the data isn't focused on. 

### Datasets and API's
* [Police Shootings Dataset](https://github.com/washingtonpost/data-police-shootings)
* [US Demographics Dataset](https://docs.microsoft.com/en-us/azure/open-datasets/dataset-us-population-county?tabs=azureml-opendatasets)
* [US Cities/Counties Dataset](https://simplemaps.com/data/us-cities) 
* [US Unemployment API](https://www.careeronestop.org/Developers/WebAPI/Unemployment/get-unemployment-type.aspx)

## Data Warehouse Schema (Staging & Production)
***
![Staging Schema](https://github.com/Phileodontist/PoliceShootingsDashboard/blob/master/images/staging_schema.png)
![Production Schema](https://github.com/Phileodontist/PoliceShootingsDashboard/blob/master/images/prod_schema.png)

## ETL Pipeline Workflow
***