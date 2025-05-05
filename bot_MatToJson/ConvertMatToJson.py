import os
import json
import argparse
from scipy.io import loadmat
import re

def convertMatToJsonFiles(mat_directory):
    if not os.path.exists(mat_directory):
        raise FileNotFoundError(f"El directorio '{mat_directory}' no existe.")

    for file_name in os.listdir(mat_directory):
        if file_name.endswith(".mat"):
            mat_file_path = os.path.join(mat_directory, file_name)
            
            print(f"Procesando archivo: {mat_file_path}")
            
            try:
                match = re.search(r"(\d{4})-(\d{2})-(\d{2})", file_name)
                if match:
                    date = f"{match.group(1)}{match.group(2)}{match.group(3)}"
                else:
                    print("Advertencia: No se encontró una fecha en el", end=' ') 
                    print(f"nombre del archivo '{file_name}'.")
                    continue
                
                data = loadmat(mat_file_path)
                
                if 'grid' not in data:
                    print(f"Advertencia: El archivo '{file_name}' no ", end=' ')
                    print("contiene la variable 'grid'. Saltando este archivo.")
                    continue
                
                grid = data['grid']
                
                # Confirmar dimensiones esperadas (10000x5x144)
                if grid.ndim != 3 or grid.shape[1] != 5:
                    print(f"Advertencia: El archivo '{file_name}' no tiene ", end=' ')
                    print(f"las dimensiones esperadas: {grid.shape}. Saltando este archivo.")
                    continue
                
                # Crear el archivo JSON correspondiente
                json_file_name = os.path.splitext(file_name)[0] + ".json"
                json_file_path = os.path.join(mat_directory, json_file_name)
                
                with open(json_file_path, "w") as f:
                    cell_dict = {}
                    for i in range(grid.shape[0]): 
                        cell_id = i + 1 
                        cell_dict[cell_id] = []  
                        for j in range(grid.shape[2]):  
                            time = f"{date}{str(j+1).zfill(2)}" 
                            
                            # Construye el objeto de evento
                            event = {
                                "event_timestamp": int(time),
                                "sms_in": grid[i, 0, j],
                                "sms_out": grid[i, 1, j],
                                "call_in": grid[i, 2, j],
                                "call_out": grid[i, 3, j],
                                "internet_traffic": grid[i, 4, j]
                            }
                            
                            cell_dict[cell_id].append(event) 
                    
                    # Escribir los datos en una línea por celda
                    for cell_id, events in cell_dict.items():
                        json_line = {"cell": cell_id, "events": events}
                        f.write(json.dumps(json_line) + '\n')
                
                print(f"Archivo JSON generado: {json_file_path}")
            
            except Exception as e:
                print(f"Error procesando el archivo '{file_name}': {e}")

    print("Conversión completada para todos los archivos .mat.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convierte archivos .mat en un directorio a formato .json")
    parser.add_argument(
        "directory",
        type=str,
        help="Ruta del directorio que contiene los archivos .mat"
    )
    
    args = parser.parse_args()
    
    convertMatToJsonFiles(args.directory)
