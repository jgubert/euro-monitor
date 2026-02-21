# euro-monitor
a study to obtain the euro exchange rate, store it in a database, and generate data visualizations.
the idea its get the data of euro quotation from the brasilapi, store in duckDB, and plot a graph in /img.


## Usage

to install the virtual env, you can use:
```
uv venv --python 3.13
uv pip install -r requirements.txt
```

to check all the make commands
```
make help
```

to create all the database, schemas and tables
```
make db-recreate
```

get the last X days of euro quotation, run the etl pipeline and plot the graph
```
make run last_days=X
```