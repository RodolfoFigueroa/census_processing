from cfc_dagster_utils.types import PostgresRelation

METROPOLI_2020_RELATION = PostgresRelation("metropoli_2020", schema="public")
MUN_2020_RELATION = PostgresRelation(schema="public", name="census_2020_mun")
