import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import json
import argparse

def normalizarSocialPulse(milano_grid_dir, social_pulse_dir):
    milano_grid = gpd.read_file(milano_grid_dir)

    with open(social_pulse_dir, 'r') as f:
        social_pulse_data = json.load(f)

    # Extraer los datos de la colección de features
    features = social_pulse_data.get("features", [])

    # Crear un DataFrame con features
    social_pulse = pd.DataFrame(features)

    # Seleccionar solo los campos requeridos
    social_pulse = social_pulse[['created', 'timestamp', 'user', 'municipality.name', 'language', 'geomPoint.geom']]

    # Crear una columna geometrica a partir de `geomPoint.geom`
    social_pulse['geometry'] = social_pulse['geomPoint.geom'].apply(
        lambda geom: Point(geom['coordinates']) if pd.notnull(geom) else None
    )

    social_pulse_geodf = gpd.GeoDataFrame(social_pulse, geometry='geometry', crs="EPSG:4326")

    social_pulse_cells = gpd.sjoin(
        social_pulse_geodf, milano_grid[['cellId', 'geometry']], how='left', predicate='within'
    )

    social_pulse_cells['cellId'] = social_pulse_cells['cellId'].astype('Int64')  # Int64 es tipo nullable de pandas

    # Eliminar las columnas no necesarias
    social_pulse_cells.drop(columns=['index_right', 'geometry'], inplace=True)

    social_pulse_cells.rename(columns={
        'municipality.name': 'municipality_name',
        'geomPoint.geom': 'geomPoint_geom'
    }, inplace=True)


    # Guardar el resultado como un archivo JSON
    with open("social-pulse-milano-normalized.json", 'w') as f:
        for record in social_pulse_cells.to_dict(orient='records'):
            f.write(json.dumps(record) + "\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convierte SocialPulse y asigna cellId")
    parser.add_argument(
        "milano_grid_directory",
        type=str,
        help="Ruta del archivo milano_grid"
    )
    parser.add_argument(
        "socialPulse_directory",
        type=str,
        help="Ruta del archivo socialPulse"
    )

    args = parser.parse_args()

    normalizarSocialPulse(args.milano_grid_directory, args.socialPulse_directory)
