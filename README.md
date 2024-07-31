# Big Data Analysis on openDataSUS Flu Syndrome Notifications
My aim with this project is to improve my concepts and techniques handling big datasets where regular data pipelines are not enough.

I'm using the openDataSUS dataset on flu syndrome, [available here](https://opendatasus.saude.gov.br/organization/ministerio-da-saude), on the pages starting with "Notificações de Síndrome Gripal".

They have data starting 2020 on reports of flu syndrome, ranging from low to moderate suspicion of COVID-19 infections.
At time of write, all data is distributed on 458 CSV files, adding up to ~61GB of data.

## Initial ideas for the project
- crawl the website to get all files
- pre process all files into .parquet
- process files with different packages and techniques and log execution data:
  - pandas (just for fun)
  - dask
  - pyspark
  - duckdb
  - polars
- curate and enrich data
- maybe some analytics and NLP applications

I'm planning and changing as I go so everything here may change in the future 😉
