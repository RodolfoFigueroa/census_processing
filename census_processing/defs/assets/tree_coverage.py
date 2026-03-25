import ee

ee.Initialize()


# @dg.asset(ins={"df_agebs": dg.AssetIn()})
# def tree_coverage(df_agebs: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
#     bbox = shapely.box(*df_agebs.to_crs("EPSG:4326").total_bounds)
#     bbox_ee = ee.geometry.Geometry.Polygon(
#         list(zip(*bbox.exterior.coords.xy, strict=True))
#     )
