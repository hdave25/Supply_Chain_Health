# Supply Chain Health

This project consists of preprocessing, transforming,
and applying analytics on supply chain data to obtain
insights and analytics ready, aggregated data for visualization.

### Some of the best practices used in the project:

1. Preprocessing & cleaning of the raw data
2. Medallion Data Architecture 
3. Incremental updates to bronze layer (delete+insert strategy)
4. Aggregation & Business KPI in silver and gold layer tables
5. Snapshotting of the analytics ready data for future usage
6. Modularized code with sufficient documentation
7. API endpoints to access the aggregated data & cleaned raw data
8. Visualization of supply chain health metrics

### Future enhancements:

1. Caching strategies for heavy queries
2. LLM-integration for natural language to SQL conversion
3. Stock-out prediction heuristic

### Here is the architecture diagram of the project:

![Project Architecture_Diagram](static/arch_diagram.PNG)

## Instructions to run:

Build and run the docker image using Dockerfile given in the root folder of 
this repository.

```
docker build -t supply_chain_project .
```

```
docker run -p 8000:8000 supply_chain_project
```

Once FastAPI server starts, go to http://127.0.0.1:8000/
If you see message like this "Welcome to Supply Chain Health Application", you are all set !!!

Read ```/docs/api_usage.md``` file to explore APIs & to get insights 
from supply chain data!

Read more about the project steps & implementation details in ```/docs``` folder.

Couple of visualization plots are also created in a jupyter notebook 
(```/code/notebook/insights```):
* Vendor reliability ranking
* Materials with highest stock‑risk frequency

