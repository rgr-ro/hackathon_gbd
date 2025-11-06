import requests
import pandas as pd
import time
import os # Para crear nombres de archivo

# --- 1. CONFIGURACIÓN ---
# ¡PON AQUÍ LOS IDs DE LOS RECURSOS QUE QUIERES DESCARGAR!
lista_de_resource_ids = [
    "28abbf9a-0765-4466-bbc3-33ca5468dec6", # Ejemplo: Titulaciones URJC 2020-21
    "81b3b109-0b10-48d0-ab46-91cbfc94a1d9",
    "ec308751-9902-4418-8a3a-685306be261c",
    "804ff860-a5a0-4186-97b5-ca3dd2337536",
    "e22c6a6e-fd3f-4e20-a306-15738dc9f93b",
    "446102a5-cecc-46c6-a8c5-ae36affcbbc1"
]

# Endpoint de la API
base_url = "https://www.universidata.es/api/action/datastore/search.json"

# Registros a pedir en cada "página" (petición).
# Puedes subirlo (ej. 1000 o 5000) si la API lo soporta bien.
page_limit = 1000

# --- 2. LÓGICA DE DESCARGA ---

def descargar_recurso_completo(resource_id):
    """
    Descarga todos los registros de un resource_id específico usando paginación.
    """
    print(f"\n--- Iniciando descarga para Resource ID: {resource_id} ---")
    
    current_offset = 0
    all_records = [] # Lista para guardar todos los registros de ESTE recurso

    while True:
        try:
            # Definir parámetros para esta página
            params = {
                'resource_id': resource_id,
                'limit': page_limit,
                'offset': current_offset
            }
            
            # Hacer la petición GET
            response = requests.get(base_url, params=params)
            
            # Verificar si la petición falló (ej. 404 si el ID no existe)
            response.raise_for_status() 
            
            data = response.json()
            
            # Verificar si la respuesta de la API fue exitosa
            if not data.get('success'):
                print(f"Error de API para ID {resource_id}: {data.get('error', 'Error desconocido')}")
                break # Salir del bucle para este ID

            new_records = data.get('result', {}).get('records', [])
            
            if not new_records:
                # Si la API no devuelve más registros, hemos terminado con este ID.
                print("No se encontraron más registros. Descarga completada.")
                break
                
            # Añadir los registros de esta página a nuestra lista total
            all_records.extend(new_records)
            
            print(f"  Obtenidos {len(new_records)} registros. Total acumulado: {len(all_records)}")
            
            # Preparar la siguiente petición incrementando el offset
            current_offset += page_limit
            
            # Pequeña pausa para ser respetuosos con el servidor
            time.sleep(0.5) 

        except requests.exceptions.HTTPError as e:
            print(f"Error HTTP: {e}. ¿Quizás el resource_id '{resource_id}' es incorrecto?")
            break # Salir del bucle para este ID
        except requests.exceptions.RequestException as e:
            print(f"Error de conexión durante la petición: {e}")
            time.sleep(5) # Esperar 5 seg antes de reintentar (opcional)
        except KeyError:
            print("Error: No se pudo leer la respuesta JSON (quizás 'result' o 'records' falta).")
            break # Salir del bucle para este ID

    # --- 3. Guardar los datos de este recurso ---
    if all_records:
        try:
            df = pd.DataFrame(all_records)
            
            # Crear un nombre de archivo único para este recurso
            nombre_archivo = f"datos_recurso_{resource_id}.csv"
            
            df.to_csv(nombre_archivo, index=False, encoding='utf-8-sig')
            print(f"¡ÉXITO! Se guardaron {len(all_records)} registros en '{nombre_archivo}'")
        except Exception as e:
            print(f"Error al guardar en CSV: {e}")
    else:
        print(f"No se descargó ningún registro para el ID {resource_id}.")

# --- BUCLE PRINCIPAL ---
# Recorrer la lista de IDs y llamar a la función de descarga para cada uno
for rid in lista_de_resource_ids:
    descargar_recurso_completo(rid)

print("\n--- Proceso de descarga masiva finalizado. ---")