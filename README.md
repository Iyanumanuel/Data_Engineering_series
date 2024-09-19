# DE_project
A repository for my data engineering project



# Implementation workflow
Data is ingested from the data source using a python script into a postgres database(staging area), the data is then sent to mariadb(data warehouse) using airbyte as the etl tool. Both the python script and Airbyte is triggered by apache airflow which was used to orchestrate the data pipeline end to end.


![Architecture](https://github.com/Iyanumanuel/DE_project/blob/main/implementation_architecture.jpg)



# Tools involved
-data source (https://data.cityofchicago.org/Public-Safety/Crimes-2001-to-present-Dashboard/5cd6-ry5g)

-python pandas (source to staging)

-postgresdb (staging area)

-dbt (transformation)

-mariadb (destination)

-airbyte (etl)

-airflow (pipeline)


