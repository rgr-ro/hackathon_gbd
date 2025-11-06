import requests
import os
from pathlib import Path
import zipfile
import tempfile

# --- 1. CONFIGURACIÓN ---
# URLs de los CSVs de UAM 2023 a descargar
urls_csvs = [
    "https://www.universidata.es/sites/default/files/uam-presupuesto-de-ingresos-cierre-2023.csv",
    "https://www.universidata.es/sites/default/files/uam-presupuesto-de-gastos-cierre-2023.csv",
    "https://www.universidata.es/sites/default/files/uam-licitaciones-contratos-mayores-2023.csv",
    "https://www.universidata.es/sites/default/files/uam-conv-ayudas-2023-24.csv",
    "https://www.universidata.es/sites/default/files/uam-ayudas-2023-24-anonimizado.csv"
]

# Directorio donde se guardarán los archivos CSV descargados directamente
directorio_destino = "csv"

# URLs de ZIPs que contienen archivos variados; solo se extraerán los CSV
zip_urls = [
    "https://www.universidata.es/node/291/dataset/download",
    "https://www.universidata.es/node/297/dataset/download",
    "https://www.universidata.es/node/1614/dataset/download",
    "https://www.universidata.es/node/1208/dataset/download",
    "https://www.universidata.es/node/1230/dataset/download",
]

# Directorio donde se guardarán TODOS los CSV extraídos de los ZIPs
directorio_destino_totales = "all_csv"

# --- 2. CREAR DIRECTORIO DE DESTINO ---
Path(directorio_destino).mkdir(parents=True, exist_ok=True)
Path(directorio_destino_totales).mkdir(parents=True, exist_ok=True)
print(f"Directorio de destino (CSV directos): {directorio_destino}")
print(f"Directorio de destino (CSV de ZIPs): {directorio_destino_totales}\n")

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

# --- 4. FUNCIÓN: DESCARGAR ZIP Y EXTRAER SOLO CSVs ---
def descargar_zip_y_extraer_csvs(url, destino_csvs):
    """
    Descarga un ZIP desde la URL y extrae únicamente los archivos .csv
    hacia el directorio indicado. Descarta otros formatos.
    """
    print(f"Procesando ZIP: {url}")

    # Nombre base del ZIP (para prefijar y evitar colisiones de nombre)
    zip_slug = url.rstrip("/").split("/")[-2:]  # p.ej. ['dataset', 'download'] o ['1230', 'dataset']
    zip_slug = "-".join(zip_slug)

    try:
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()

            with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        tmp.write(chunk)
                temp_zip_path = tmp.name

        extraidos = 0
        with zipfile.ZipFile(temp_zip_path, 'r') as zf:
            for member in zf.infolist():
                name = member.filename
                # Saltar directorios y no-CSV
                if member.is_dir():
                    continue
                if not name.lower().endswith('.csv'):
                    continue

                # Asegurar nombre seguro (sin rutas) y prefijo para evitar colisiones
                base_name = os.path.basename(name)
                if not base_name:
                    continue
                prefixed_name = f"{zip_slug}__{base_name}"
                out_path = os.path.join(destino_csvs, prefixed_name)

                # Extraer como binario
                with zf.open(member, 'r') as src, open(out_path, 'wb') as dst:
                    dst.write(src.read())
                extraidos += 1

        os.unlink(temp_zip_path)
        print(f"  ✓ Extraídos {extraidos} CSV(s) desde el ZIP\n")
        return extraidos

    except requests.exceptions.HTTPError as e:
        print(f"  ✗ Error HTTP al descargar ZIP: {e}\n")
        return 0
    except requests.exceptions.RequestException as e:
        print(f"  ✗ Error de red al descargar ZIP: {e}\n")
        return 0
    except zipfile.BadZipFile:
        print("  ✗ El archivo descargado no es un ZIP válido\n")
        return 0
    finally:
        try:
            if 'temp_zip_path' in locals() and os.path.exists(temp_zip_path):
                os.unlink(temp_zip_path)
        except Exception:
            pass

# --- 5. BUCLE PRINCIPAL ---
print("=== INICIANDO DESCARGA DE CSVs DIRECTOS UAM 2023 ===\n")

exitosos = 0
fallidos = 0

for url in urls_csvs:
    if descargar_csv(url):
        exitosos += 1
    else:
        fallidos += 1

# Procesar ahora los ZIPs para extraer solo CSVs
print("=== INICIANDO DESCARGA Y EXTRACCIÓN DE ZIPs (solo CSVs) ===\n")
total_csv_extraidos = 0
for url in zip_urls:
    total_csv_extraidos += descargar_zip_y_extraer_csvs(url, directorio_destino_totales)

# --- 6. RESUMEN FINAL ---
print("=" * 50)
print("DESCARGA FINALIZADA")
print(f"  ✓ CSVs directos descargados exitosamente: {exitosos}")
print(f"  ✗ CSVs directos con errores: {fallidos}")
print(f"  📦 CSVs extraídos desde ZIPs: {total_csv_extraidos}")
print(f"  Ubicación CSV directos: {os.path.abspath(directorio_destino)}")
print(f"  Ubicación CSV de ZIPs: {os.path.abspath(directorio_destino_totales)}")
print("=" * 50)