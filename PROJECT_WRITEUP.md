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


## Write Up Prompts

* **What's the goal of the project?**
> The goal of this project is to build an analytics dashboard that displays the distribution of police shootings across the United States. 
By pulling data in terms of demographics and unemployment levels by state/county, the context in which these shootings occur may give a 
clearer view of the patterns of police shootings.

* **What queries will you want to run?**
> The idea is to be able to perform roll-up statistics based on dimensions; `city`, `race`, `unemployment rate`, `etc`
>1. Pull all the data together
>2. The number of police shootings based on dimensions
>3. The number of cases of mental health related shootings based on county, state, etc

* **How would Spark or Airflow be incorporated?** 
> With the pipeline designed in using pyspark, the data that is ingested and processed will leverage the distributed processing of AWS EMR. 
This allows faster turn out in terms of updating the production version of the data for dashboard usage.
> As police shootings data is updated on a weekly process, using Airflow to orchestrate the ingestion and integration of the data into 
the data model would ensure the availability of new data within the dashboard. Having a scheduled pipeline would not only achieve updated data, 
but also ensure data quality as quality checks are done after major ingestions and transformations. 
So in the case where the pipeline has to run weekly, or even more frequently, using Airflow helps running a collection of scripts at a scheduled time 
achievable without much manual work.

* **Why did you choose the model you chose?**
> The reasoning behind the use of a standard database model is due to the nature of the police shootings dataset. 
With each record being an independent event with no values to aggregate across dimensions, defining the model as such 
proved to be the simplest and effective way of representing the heterogeneous data retrieved from a variety of sources.

* **State the rationale for the choice of tools and technologies for the project**
> * Spark
> * AWS S3
> * AWS RDS
> * AWS EMR

> In working with a large dataset, leveraging Spark helps process and transform data relatively faster as opposed to processing all the data on one machine.
Using AWS EMR in conjunction, provides a cluster of nodes for distributed processing. In terms of storage, AWS S3 serves as the centralized space for 
the various datasets used for this project, being scalable as the size of the data increases over time. Lastly, AWS RDS provides the database management system
needed to store data in both the staging and production stages of data ingestion. Like AWS S3, AWS RDS can scale accordingly as need dictates.

* **How often the data should be updated and why?**
> With the nature of how each of the individual datasets are updated, the frequency of updates varies, where in the case of police shootings and unemployment data,
weekly updates are expected. U.S. demographics on the other hand is derived from the U.S. census, which is conducted every 10 years. 
So demographics data may not be updated at all, similarly to the U.S. cities dataset where cities and counties rarely change.

### How would the current system address the following problems?
* **If the data increased by 100x**
> In the event where the data volume increases significantly, the usage of scalable tools such as AWS S3, RDS and EMR allows the pipeline the flexibility 
to increase storage and processing accordingly. This would entail increasing the storage capacity of S3, and scaling horizontally for both RDS and EMR 
by adding more nodes to the cluster, etc.

* **If the pipelines were run daily by 7am**
> Needing the pipeline to run at a specific time, Apache Airflow comes into play by providing the means of orchestrating the pipeline and scheduling it to 
run accordingly. The need to increase storage and processing may be dependent on the volume of data that is being ingested on a daily basis.

* **If the database needed to be accessed by 100+ people**
> When the time comes where 100+ people will be accessing the database/dashboard, scaling the AWS RDS instance would be crucial in sustaining an increase of users. By horizontally scaling, the amount of queries can be off balanced between nodes to prevent bottlenecks from occurring.
