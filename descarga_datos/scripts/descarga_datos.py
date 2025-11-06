import requests
import os
from pathlib import Path

# --- 1. CONFIGURACIÓN ---
# URLs de los CSVs de UAM 2023 a descargar
urls_csvs = [
    "https://www.universidata.es/sites/default/files/uam-presupuesto-de-ingresos-cierre-2023.csv",
    "https://www.universidata.es/sites/default/files/uam-presupuesto-de-gastos-cierre-2023.csv",
    "https://www.universidata.es/sites/default/files/uam-licitaciones-contratos-mayores-2023.csv",
    "https://www.universidata.es/sites/default/files/uam-conv-ayudas-2023-24.csv",
    "https://www.universidata.es/sites/default/files/uam-ayudas-2023-24-anonimizado.csv"
]

# Directorio donde se guardarán los archivos descargados
directorio_destino = "datos_output"

# --- 2. CREAR DIRECTORIO DE DESTINO ---
Path(directorio_destino).mkdir(parents=True, exist_ok=True)
print(f"Directorio de destino: {directorio_destino}\n")

# --- 3. FUNCIÓN DE DESCARGA ---
def descargar_csv(url):
    """
    Descarga un archivo CSV desde una URL directa.
    """
    # Extraer el nombre del archivo de la URL
    nombre_archivo = url.split("/")[-1]
    ruta_destino = os.path.join(directorio_destino, nombre_archivo)
    
    print(f"Descargando: {nombre_archivo}")
    
    try:
        # Realizar la petición con streaming para archivos grandes
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        # Guardar el contenido en el archivo
        with open(ruta_destino, 'wb') as archivo:
            for chunk in response.iter_content(chunk_size=8192):
                archivo.write(chunk)
        
        # Obtener el tamaño del archivo descargado
        tamaño_mb = os.path.getsize(ruta_destino) / (1024 * 1024)
        print(f"  ✓ Descargado exitosamente: {nombre_archivo} ({tamaño_mb:.2f} MB)\n")
        return True
        
    except requests.exceptions.HTTPError as e:
        print(f"  ✗ Error HTTP al descargar {nombre_archivo}: {e}\n")
        return False
    except requests.exceptions.ConnectionError as e:
        print(f"  ✗ Error de conexión al descargar {nombre_archivo}: {e}\n")
        return False
    except requests.exceptions.Timeout as e:
        print(f"  ✗ Timeout al descargar {nombre_archivo}: {e}\n")
        return False
    except Exception as e:
        print(f"  ✗ Error inesperado al descargar {nombre_archivo}: {e}\n")
        return False

# --- 4. BUCLE PRINCIPAL ---
print("=== INICIANDO DESCARGA DE ARCHIVOS UAM 2023 ===\n")

exitosos = 0
fallidos = 0

for url in urls_csvs:
    if descargar_csv(url):
        exitosos += 1
    else:
        fallidos += 1

# --- 5. RESUMEN FINAL ---
print("=" * 50)
print(f"DESCARGA FINALIZADA")
print(f"  ✓ Archivos descargados exitosamente: {exitosos}")
print(f"  ✗ Archivos con errores: {fallidos}")
print(f"  Ubicación: {os.path.abspath(directorio_destino)}")
print("=" * 50)