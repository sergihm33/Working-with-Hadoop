import docker
import io
import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import argparse

datos = None

def leerDatos(directorio):
    global datos
    client = docker.from_env()

    container_name = "namenode"

    container = client.containers.get(container_name)

    command = "cat " + directorio
    output = container.exec_run(command)
    datos = pd.read_csv(io.StringIO(output.output.decode()), header=None, names=['cellId', 'dato'], delimiter='\t')
    

def generarMapaCalor(titulo):
    global datos 
    if datos is None:
        raise ValueError("La variable 'datos' está vacía. Asegúrate de ejecutar 'ejecutarConsulta' antes de generar el mapa de calor.")

    # Cargar el grid de Milan
    milano_grid = gpd.read_file("milano-grid.geojson")

    milano_grid['cellId'] = milano_grid['cellId'].astype(int)
    datos['cellId'] = datos['cellId'].astype(int)

    # Unir ambos conjuntos de datos
    datos_unidos = milano_grid.merge(datos, on='cellId', how='left')

    # Rellenar los valores faltantes con 0
    datos_unidos['dato'] = datos_unidos['dato'].fillna(0)

    # Configurar el tamaño del mapa
    fig, ax = plt.subplots(1, 1, figsize=(10, 10))

    # Crear un mapa de calor basado en event_count
    datos_unidos.plot(column='dato',
                      cmap='OrRd',  
                      legend=True,
                      edgecolor='black',
                      ax=ax)

    ax.set_title(titulo, fontsize=16)
    plt.axis('off')

    plt.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Genera mapa de calor de Milan")
    parser.add_argument(
        "directorio",
        type=str,
        help="Ruta del archivo que contiene los datos de Hadoop"
    )
    parser.add_argument(
        "titulo",
        type=str,
        help="Titulo del grafico"
    )
    args = parser.parse_args()

    leerDatos(args.directorio)

    generarMapaCalor(args.titulo)
