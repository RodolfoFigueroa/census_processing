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

## Geographic units

`geo_unit_<year>_<level>` tables for 2010 and 2020 cover `ent`, `mun`, `loc`,
and `ageb`. They contain text identifiers, with primary keys on `cvegeo` and
foreign keys from `cve_ent`, `cve_mun`, or `cve_loc` to the corresponding parent
table for the same year. Identifier lengths are 2, 5, 9, and 13 characters.

Coverage combines geometry, census summary rows, and every SCINCE AGEB theme.
Parents implied by child identifiers are included even without their own source
row. Units without statistics or geometry are retained; locality coverage is not
limited to localities with urban AGEBs. The 2020 hierarchy also includes the
municipalities in metropolitan staging.

`geo_unit_2010_ageb.cve_met` uses the greatest positive overlap with 2020
metropolitan polygons, breaking ties by metropolitan code. Missing geometry or
no positive overlap yields null. `geo_unit_2020_mun.cve_met` instead uses the
municipality mapping in `staging.metropoli_2020_prepared`; conflicting assignments
fail the build. Other new geographic-unit tables have no `cve_met` column.

After materializing the census, geometry, SCINCE staging assets and
`metropoli_2020`, build both complete hierarchies together so parent foreign keys
are recreated in dependency order:

```sh
uv run --env-file .env dbt build --project-dir dbt --profiles-dir dbt --select 'geo_unit_2010_*' 'geo_unit_2020_*'
uv run --env-file .env dbt test --project-dir dbt --profiles-dir dbt --select geo_unit_ids_fixtures geo_unit_metropolitan_fixtures add_parent_cvegeo_fixtures
```
