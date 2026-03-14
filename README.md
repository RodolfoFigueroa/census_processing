# Census processing

## First steps

Copy the `.env.example` file:

```
cp .env.example .env
```

In the newly created `.env` file, fill in the missing variables as follows:

* `DATA_PATH`: Path to the data directory that was previously downloaded.
* `DAGSTER_HOME`: Path to the `dagster/` directory inside this repository.

If you haven't done so before, create the database using Docker compose:

```
docker compose up -d -f ./docker/db_compose.yaml
```

Install all packages with `uv`:

```
uv sync
```

Run the Dagster webui:

```
uv run dg dev
```

You can access it at `localhost:3000`

## Usage

On the Dagster webui, simply click **Materialize all** on the top right corner. This will read all necesary files from `DATA_PATH` and populate the database that was previously created.

## Database structure

The database has the following tables:

* `census_<year>_<level>`: Geometries and censal statistics for a certain year and census level. Possible `year` values are `1990, 2000, 2010, 2020`. Possible level values are `mza, ageb, loc, mun, ent`.

* `metropoli_2020`: Polygons derived from the 2020 version of INEGI's metropolitan zones.