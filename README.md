# MSSQL-FAISS POC integration samples

Build a FAISS model store it in MSSQL. 

For FAISS also build a containerized REST service and expose FAISS via REST API that can be consumed by T-SQL. 

## Requirements

You need

- Visual Studio Code Code
- [Visual Studio Code Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
- Docker Desktop

## Local execution

Make sure that Docker Desktop is running. Clone the repository and open the `poc` folder in Visual Studio Code. The Dev Containers extension, after a few seconds, will ask you to open the folder in a container. If VS Code doesn't ask you to open the folder in a Dev Container, hit `F1` and then type `Dev Containers: Reopen in Container`. Do it and then open the terminal in Visual Studio Code. 

Create a new database in Azure SQL DB or use an existing one, then create and import a sample of Wikipedia data using script `sql/import-wikipedia.sql`.

Create a new file named `.env` in the `poc` folder using the `.env.sample` as a starting point. Add the connection string to the database with the Wikipedia sample data.

## FAISS Test

TDB

To get the ids of the wikipedia articles most similar to the searched text. You can use the returned in the `/sql/faiss/faiss_test.sql` to get the article titles.

### Running FAISS as a REST service

To integrate with Azure SQL DB via `sp_invoke_external_rest_endpoint` a REST service is needed. A sample REST API has been created to allow interaction with FAISS. Run the server using:

```bash
cd src
uvicorn main:api
```

to serve the FAISS index as a REST endpoint.

Once the server is started you can use 

```http
GET http://127.0.0.1:8000/index/faiss/info
```

if the index has been loaded and it is ready, and 

```http
POST http://127.0.0.1:8000/index/faiss/query
```

To send a vector to search for similarity

```http
POST http://127.0.0.1:8000/index/faiss/add
```

To add a vector to the index.

The REST API has been deployed already in an Azure Container Instance here:

```
https://dm-faiss.purpleflower-af782b2e.centralus.azurecontainerapps.io
```

A sample integration with Azure SQL DB can be test using the `/sql/faiss/faiss_sidecar_test.sql` script.

