import argparse
import csv
import os
import re
from pathlib import Path

from rdflib import Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCTERMS, FOAF, OWL, RDF, RDFS, XSD
from rdflib.term import BNode

# --- 1. Definición de Namespaces ---
DCAT = Namespace("http://www.w3.org/ns/dcat#")
SCHEMA = Namespace("https://schema.org/")
# Tu prefijo para entidades (Universidades, Empresas)
G2_UNI = Namespace("https://www.mi-master.es/proyecto/entidad/")
# Tu prefijo para datos (Licitaciones, Ayudas)
G2_DATA = Namespace("https://www.mi-master.es/proyecto/datos/")
# Tu prefijo para tu ontología/extensión
G2_ONT = Namespace("https://www.mi-master.es/proyecto/ontologia#")
# Namespaces para enlaces externos
DBR = Namespace("http://dbpedia.org/resource/")
WD = Namespace("http://www.wikidata.org/entity/")


# --- FUNCIÓN DE AYUDA PARA LIMPIAR URIs ---
def slugify(text):
    """
    Limpia un string para que sea seguro en una URI.
    Quita caracteres no alfanuméricos y reemplaza espacios por '-'.
    """
    if not text:
        return "sin-id"
    text = str(text).lower().strip()
    # Quita todo menos letras, números, espacios, guiones
    text = re.sub(r"[^a-z0-9\s-]", "", text)
    # Reemplaza espacios y guiones repetidos por un solo guion
    text = re.sub(r"[\s-]+", "-", text)
    if not text:
        return "sin-id"
    return text


# --- FIN FUNCIÓN DE AYUDA ---


# --- 2. CONFIGURACIÓN (Inputs del Proyecto) ---

# Identificadores de la UAM descubiertos en los CSVs
UAM_CODIGO = "23"
UAM_NIFOC = "Q2818013A"
# La URI única para la UAM en nuestro grafo
UAM_URI = G2_UNI["UAM-Q2818013A"]

# Ruta base a la carpeta de CSVs
BASE_CSV_PATH = Path(__file__).parent.parent / "data" / "csv" / "all_csv"


# --- FUNCIÓN PARA AUTO-DETECTAR CSVs ---
def generar_catalogo_config():
    """
    Escanea la carpeta all_csv y genera automáticamente
    la configuración para todos los CSVs encontrados.
    """
    catalogo = []

    if not BASE_CSV_PATH.exists():
        print(f"ADVERTENCIA: No se encontró la carpeta {BASE_CSV_PATH}")
        return catalogo

    # Obtener todos los archivos CSV
    csv_files = sorted(BASE_CSV_PATH.glob("*.csv"))

    # Patrón para extraer el año de los nombres de archivo
    import re

    for csv_file in csv_files:
        filename = csv_file.name

        # Extraer año del nombre del archivo
        año_match = re.search(r"(\d{4})", filename)
        año = año_match.group(1) if año_match else "2023"

        # Determinar el tipo de entidad basado en el nombre del archivo
        if "licitaciones" in filename.lower():
            catalogo.append(
                {
                    "entidad": "LICITACION",
                    "archivo_csv": str(csv_file),
                    "dataset_id": f"dataset-licitaciones-uam-{año}",
                    "titulo_dataset": f"Licitaciones de Contratos Mayores de la UAM {año}",
                    "desc_dataset": f"Datos de licitaciones adjudicadas, importes y adjudicatarios del año {año}.",
                    "keywords": ["licitaciones", "contratos", "transparencia", "gasto"],
                    "año_fiscal": año,
                    "procesar_funcion": "procesar_licitacion",
                }
            )

        elif "presupuesto-de-gastos" in filename.lower():
            catalogo.append(
                {
                    "entidad": "PRESUPUESTO_GASTOS",
                    "archivo_csv": str(csv_file),
                    "dataset_id": f"dataset-presupuestos-gastos-uam-{año}",
                    "titulo_dataset": f"Presupuestos de Gastos de la UAM {año}",
                    "desc_dataset": f"Créditos iniciales, modificaciones y créditos totales por capítulo del año {año}.",
                    "keywords": ["presupuestos", "economía", "gasto"],
                    "año_fiscal": año,
                    "procesar_funcion": "procesar_presupuesto_gastos",
                }
            )

        elif "presupuesto-de-ingresos" in filename.lower():
            catalogo.append(
                {
                    "entidad": "PRESUPUESTO_INGRESOS",
                    "archivo_csv": str(csv_file),
                    "dataset_id": f"dataset-presupuestos-ingresos-uam-{año}",
                    "titulo_dataset": f"Presupuestos de Ingresos de la UAM {año}",
                    "desc_dataset": f"Créditos iniciales, modificaciones y créditos totales por capítulo del año {año}.",
                    "keywords": ["presupuestos", "economía", "ingresos"],
                    "año_fiscal": año,
                    "procesar_funcion": "procesar_presupuesto_ingresos",
                }
            )

        elif "conv-ayudas" in filename.lower():
            catalogo.append(
                {
                    "entidad": "CONVOCATORIA_AYUDA",
                    "archivo_csv": str(csv_file),
                    "dataset_id": f"dataset-convocatorias-ayudas-uam-{año}",
                    "titulo_dataset": f"Convocatorias de Ayudas de la UAM {año}",
                    "desc_dataset": f"Convocatorias de ayudas y becas de la universidad del año {año}.",
                    "keywords": ["ayudas", "becas", "convocatorias"],
                    "año_fiscal": año,
                    "procesar_funcion": "procesar_convocatoria_ayuda",
                }
            )

        elif "ayudas" in filename.lower() and "anonimizado" in filename.lower():
            catalogo.append(
                {
                    "entidad": "AYUDA",
                    "archivo_csv": str(csv_file),
                    "dataset_id": f"dataset-ayudas-concedidas-uam-{año}",
                    "titulo_dataset": f"Ayudas Concedidas por la UAM {año} (Anonimizado)",
                    "desc_dataset": f"Datos anonimizados de las ayudas concedidas en el año {año}.",
                    "keywords": ["ayudas", "becas", "concedidas"],
                    "año_fiscal": año,
                    "procesar_funcion": "procesar_ayuda",
                }
            )

    print(f"\n✓ Se encontraron {len(catalogo)} archivos CSV para procesar")
    print(
        f"  - Licitaciones: {sum(1 for c in catalogo if c['entidad'] == 'LICITACION')}"
    )
    print(
        f"  - Presupuestos Gastos: {sum(1 for c in catalogo if c['entidad'] == 'PRESUPUESTO_GASTOS')}"
    )
    print(
        f"  - Presupuestos Ingresos: {sum(1 for c in catalogo if c['entidad'] == 'PRESUPUESTO_INGRESOS')}"
    )
    print(
        f"  - Convocatorias Ayudas: {sum(1 for c in catalogo if c['entidad'] == 'CONVOCATORIA_AYUDA')}"
    )
    print(
        f"  - Ayudas Concedidas: {sum(1 for c in catalogo if c['entidad'] == 'AYUDA')}\n"
    )

    return catalogo


# --- 3. Funciones de Procesamiento de Contenido (Basadas en el ERD) ---


def procesar_licitacion(g, config, dist_uri):
    print(f"  Procesando contenido de: {config['archivo_csv']}...")
    try:
        # Usamos latin1 encoding para este CSV
        with open(config["archivo_csv"], mode="r", encoding="latin1") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Limpiamos los IDs antes de crear la URI
                id_safe = slugify(row["identificador"])
                lote_safe = slugify(row["lote"])
                lic_id = f"licitacion-{id_safe}-{lote_safe}"
                lic_uri = G2_DATA[lic_id]

                # === MODIFICACIÓN ===
                # Usamos nuestra ontología local
                g.add((lic_uri, RDF.type, G2_ONT.Licitacion))

                # Campos de texto (Datos No Estructurados)
                # Usamos propiedades estándar (schema, dcterms)
                g.add(
                    (
                        lic_uri,
                        DCTERMS.description,
                        Literal(row["objeto_licitacion_o_lote"]),
                    )
                )
                g.add((lic_uri, SCHEMA.url, URIRef(row["link_licitacion"])))
                # Usamos nuestra ontología para campos que no existen
                g.add(
                    (
                        lic_uri,
                        G2_ONT.financiacionEuropea,
                        Literal(row["descripcion_de_la_financiacion_europea"]),
                    )
                )

                # Importes
                try:
                    presupuesto_val = float(
                        row["presupuesto_base_sin_impuestos_licitacion_o_lote"]
                    )
                    presupuesto_node = BNode()
                    g.add((presupuesto_node, RDF.type, SCHEMA.MonetaryAmount))
                    g.add(
                        (
                            presupuesto_node,
                            SCHEMA.value,
                            Literal(presupuesto_val, datatype=XSD.decimal),
                        )
                    )
                    g.add((lic_uri, SCHEMA.estimatedCost, presupuesto_node))
                except (ValueError, TypeError):
                    pass

                try:
                    adjudicacion_val = float(
                        row["importe_adjudicacion_sin_impuestos_licitacion_o_lote"]
                    )
                    adjudicacion_node = BNode()
                    g.add((adjudicacion_node, RDF.type, SCHEMA.MonetaryAmount))
                    g.add(
                        (
                            adjudicacion_node,
                            SCHEMA.value,
                            Literal(adjudicacion_val, datatype=XSD.decimal),
                        )
                    )
                    g.add((lic_uri, SCHEMA.amount, adjudicacion_node))
                except (ValueError, TypeError):
                    pass

                # Relaciones (Enlaces)
                if row["nif_oc"] == UAM_NIFOC:
                    g.add(
                        (lic_uri, SCHEMA.tenderer, UAM_URI)
                    )  # 'tenderer' = quien licita

                adj_id_safe = slugify(
                    row["identificador_adjudicatario_de_la_licitacion_o_lote"]
                )
                adj_uri = G2_UNI[f"empresa-{adj_id_safe}"]

                # === MODIFICACIÓN ===
                # Usamos nuestra ontología local
                g.add((adj_uri, RDF.type, G2_ONT.Organizacion))
                g.add(
                    (
                        adj_uri,
                        SCHEMA.name,
                        Literal(row["adjudicatario_licitacion_o_lote"]),
                    )
                )
                g.add(
                    (lic_uri, SCHEMA.awardee, adj_uri)
                )  # 'awardee' = el adjudicatario

                # Proveniencia: enlaza este dato al CSV del que salió
                g.add((lic_uri, DCTERMS.provenance, dist_uri))

    except FileNotFoundError:
        print(f"  AVISO: No se encontró el fichero {config['archivo_csv']}")
    except Exception as e:
        print(f"  Error procesando {config['archivo_csv']}: {e}")


def procesar_presupuesto_gastos(g, config, dist_uri):
    print(f"  Procesando contenido de: {config['archivo_csv']}...")
    try:
        with open(config["archivo_csv"], mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                # Limpiamos el cod_partida
                cod_partida_safe = slugify(row["cod_partida"])
                gasto_id = f"gasto-{row['anio']}-{cod_partida_safe}-{i}"
                gasto_uri = G2_DATA[gasto_id]

                # === MODIFICACIÓN ===
                g.add((gasto_uri, RDF.type, G2_ONT.PartidaGasto))

                # Usamos nuestra ontología para campos que no existen
                g.add((gasto_uri, G2_ONT.capitulo, Literal(row["des_capitulo"])))
                g.add((gasto_uri, G2_ONT.articulo, Literal(row["des_articulo"])))
                g.add((gasto_uri, DCTERMS.description, Literal(row["des_concepto"])))

                # Importes
                try:
                    inicial_val = float(row["credito_inicial"])
                    inicial_node = BNode()
                    g.add((inicial_node, RDF.type, SCHEMA.MonetaryAmount))
                    g.add(
                        (
                            inicial_node,
                            SCHEMA.value,
                            Literal(inicial_val, datatype=XSD.decimal),
                        )
                    )
                    g.add((gasto_uri, G2_ONT.creditoInicial, inicial_node))

                    total_val = float(row["credito_total"])
                    total_node = BNode()
                    g.add((total_node, RDF.type, SCHEMA.MonetaryAmount))
                    g.add(
                        (
                            total_node,
                            SCHEMA.value,
                            Literal(total_val, datatype=XSD.decimal),
                        )
                    )
                    g.add(
                        (gasto_uri, SCHEMA.amount, total_node)
                    )  # 'amount' es el valor final
                except (ValueError, TypeError):
                    pass

                # Enlace
                if row["cod_universidad"] == UAM_CODIGO:
                    g.add(
                        (gasto_uri, SCHEMA.customer, UAM_URI)
                    )  # 'customer' = quien gasta

                g.add((gasto_uri, DCTERMS.provenance, dist_uri))

    except FileNotFoundError:
        print(f"  AVISO: No se encontró el fichero {config['archivo_csv']}")
    except Exception as e:
        print(f"  Error procesando {config['archivo_csv']}: {e}")


def procesar_presupuesto_ingresos(g, config, dist_uri):
    print(f"  Procesando contenido de: {config['archivo_csv']}...")
    try:
        with open(config["archivo_csv"], mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                # Limpiamos el cod_partida (proactivamente)
                cod_partida_safe = slugify(row["cod_partida"])
                ingreso_id = f"ingreso-{row['anio']}-{cod_partida_safe}-{i}"
                ingreso_uri = G2_DATA[ingreso_id]

                # === MODIFICACIÓN ===
                g.add((ingreso_uri, RDF.type, G2_ONT.PartidaIngreso))

                # Usamos nuestra ontología para campos que no existen
                g.add((ingreso_uri, G2_ONT.capitulo, Literal(row["des_capitulo"])))
                g.add((ingreso_uri, G2_ONT.articulo, Literal(row["des_articulo"])))
                g.add((ingreso_uri, DCTERMS.description, Literal(row["des_concepto"])))

                # Importes
                try:
                    total_val = float(row["credito_total"])
                    total_node = BNode()
                    g.add((total_node, RDF.type, SCHEMA.MonetaryAmount))
                    g.add(
                        (
                            total_node,
                            SCHEMA.value,
                            Literal(total_val, datatype=XSD.decimal),
                        )
                    )
                    g.add((ingreso_uri, SCHEMA.amount, total_node))
                except (ValueError, TypeError):
                    pass

                # Enlace
                if row["cod_universidad"] == UAM_CODIGO:
                    g.add(
                        (ingreso_uri, SCHEMA.provider, UAM_URI)
                    )  # 'provider' = quien recibe el ingreso

                g.add((ingreso_uri, DCTERMS.provenance, dist_uri))

    except FileNotFoundError:
        print(f"  AVISO: No se encontró el fichero {config['archivo_csv']}")
    except Exception as e:
        print(f"  Error procesando {config['archivo_csv']}: {e}")


def procesar_convocatoria_ayuda(g, config, dist_uri):
    print(f"  Procesando contenido de: {config['archivo_csv']}...")
    try:
        with open(config["archivo_csv"], mode="r", encoding="latin1") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Limpiamos el cod_convocatoria
                cod_safe = slugify(row["cod_convocatoria"])
                conv_uri = G2_DATA[f"convocatoria-{cod_safe}"]

                # === MODIFICACIÓN ===
                g.add((conv_uri, RDF.type, G2_ONT.ConvocatoriaAyuda))

                g.add((conv_uri, SCHEMA.name, Literal(row["nombre_convocatoria"])))
                g.add((conv_uri, DCTERMS.description, Literal(row["des_categoria"])))

                try:
                    if (
                        row["fecha_inicio_solicitudes"]
                        and "-" in row["fecha_inicio_solicitudes"]
                    ):
                        g.add(
                            (
                                conv_uri,
                                SCHEMA.validFrom,
                                Literal(
                                    row["fecha_inicio_solicitudes"], datatype=XSD.date
                                ),
                            )
                        )

                    if (
                        row["fecha_fin_solicitudes"]
                        and "-" in row["fecha_fin_solicitudes"]
                    ):
                        g.add(
                            (
                                conv_uri,
                                SCHEMA.validThrough,
                                Literal(
                                    row["fecha_fin_solicitudes"], datatype=XSD.date
                                ),
                            )
                        )
                except Exception:
                    pass  # Ignora fechas mal formadas

                # Enlace
                if row["cod_universidad"] == UAM_CODIGO:
                    g.add((conv_uri, SCHEMA.provider, UAM_URI))

                g.add((conv_uri, DCTERMS.provenance, dist_uri))

    except FileNotFoundError:
        print(f"  AVISO: No se encontró el fichero {config['archivo_csv']}")
    except Exception as e:
        print(f"  Error procesando {config['archivo_csv']}: {e}")


def procesar_ayuda(g, config, dist_uri):
    print(f"  Procesando contenido de: {config['archivo_csv']}...")
    try:
        with open(config["archivo_csv"], mode="r", encoding="latin1") as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                # ID es el año + contador (no hay PK)
                ayuda_id = f"ayuda-{row['anio']}-{i}"
                ayuda_uri = G2_DATA[ayuda_id]

                # === MODIFICACIÓN ===
                g.add((ayuda_uri, RDF.type, G2_ONT.AyudaConcedida))

                try:
                    cuantia_val = float(row["cuantia_total"])
                    cuantia_node = BNode()
                    g.add((cuantia_node, RDF.type, SCHEMA.MonetaryAmount))
                    g.add(
                        (
                            cuantia_node,
                            SCHEMA.value,
                            Literal(cuantia_val, datatype=XSD.decimal),
                        )
                    )
                    g.add((ayuda_uri, SCHEMA.amount, cuantia_node))
                except (ValueError, TypeError):
                    pass

                # Enlaces
                if row["cod_universidad"] == UAM_CODIGO:
                    g.add((ayuda_uri, SCHEMA.provider, UAM_URI))

                # Limpiamos el cod_convocatoria
                cod_conv_safe = slugify(row["cod_convocatoria_ayuda"])
                conv_uri = G2_DATA[f"convocatoria-{cod_conv_safe}"]

                # === MODIFICACIÓN ===
                # Usamos nuestra propiedad de ontología
                g.add((ayuda_uri, G2_ONT.convocatoriaAsociada, conv_uri))

                g.add((ayuda_uri, DCTERMS.provenance, dist_uri))

    except FileNotFoundError:
        print(f"  AVISO: No se encontró el fichero {config['archivo_csv']}")
    except Exception as e:
        print(f"  Error procesando {config['archivo_csv']}: {e}")


# Mapeo de nombres de función
FUNCIONES_PROCESADO = {
    "procesar_licitacion": procesar_licitacion,
    "procesar_presupuesto_gastos": procesar_presupuesto_gastos,
    "procesar_presupuesto_ingresos": procesar_presupuesto_ingresos,
    "procesar_convocatoria_ayuda": procesar_convocatoria_ayuda,
    "procesar_ayuda": procesar_ayuda,
}


# --- 4. SCRIPT PRINCIPAL ---
def main():
    # Argumentos de línea de comandos
    parser = argparse.ArgumentParser(description="Generar grafo RDF desde archivos CSV")
    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Directorio donde guardar el archivo TTL (default: directorio actual)",
    )
    parser.add_argument(
        "--output-file",
        type=str,
        default="grafo_completo.ttl",
        help="Nombre del archivo de salida (default: grafo_completo.ttl)",
    )
    args = parser.parse_args()

    # Generar catálogo automáticamente desde la carpeta
    CATALOGO_CONFIG = generar_catalogo_config()

    if not CATALOGO_CONFIG:
        print("ERROR: No se encontraron archivos CSV para procesar.")
        print(f"Verifica que exista la carpeta: {BASE_CSV_PATH}")
        return

    g = Graph()
    g.bind("dcat", DCAT)
    g.bind("dcterms", DCTERMS)
    g.bind("schema", SCHEMA)
    g.bind("g2_uni", G2_UNI)
    g.bind("g2_data", G2_DATA)
    g.bind("g2_ont", G2_ONT)
    g.bind("xsd", XSD)
    g.bind("owl", OWL)
    g.bind("dbr", DBR)
    g.bind("wd", WD)

    # --- TAREA 0: Definir nuestra Ontología (Meta-esquema de Dominio) ---
    print("--- Iniciando Tarea 0: Generar meta-esquema de Dominio (Ontología) ---")

    # Definir la ontología en sí
    ont_uri = G2_ONT[""]  # La URI de la ontología es el namespace base
    g.add((ont_uri, RDF.type, OWL.Ontology))
    g.add(
        (
            ont_uri,
            RDFS.label,
            Literal("Ontología del Proyecto de Universidades", lang="es"),
        )
    )

    # --- Clases (Classes) ---
    g.add((G2_ONT.Universidad, RDF.type, OWL.Class))
    g.add((G2_ONT.Universidad, OWL.equivalentClass, SCHEMA.University))

    g.add((G2_ONT.Organizacion, RDF.type, OWL.Class))
    g.add((G2_ONT.Organizacion, OWL.equivalentClass, SCHEMA.Organization))

    g.add((G2_ONT.Licitacion, RDF.type, OWL.Class))
    g.add((G2_ONT.Licitacion, OWL.equivalentClass, SCHEMA.Tender))

    g.add((G2_ONT.ConvocatoriaAyuda, RDF.type, OWL.Class))
    g.add((G2_ONT.ConvocatoriaAyuda, OWL.equivalentClass, SCHEMA.Grant))

    g.add((G2_ONT.AyudaConcedida, RDF.type, OWL.Class))
    g.add(
        (G2_ONT.AyudaConcedida, RDFS.subClassOf, SCHEMA.MonetaryGrant)
    )  # Una subclase

    g.add((G2_ONT.PartidaGasto, RDF.type, OWL.Class))
    g.add((G2_ONT.PartidaIngreso, RDF.type, OWL.Class))

    # --- Propiedades de Datos (Datatype Properties) ---
    g.add((G2_ONT.financiacionEuropea, RDF.type, OWL.DatatypeProperty))
    g.add((G2_ONT.financiacionEuropea, RDFS.domain, G2_ONT.Licitacion))
    g.add((G2_ONT.financiacionEuropea, RDFS.range, XSD.string))

    g.add((G2_ONT.añoFiscal, RDF.type, OWL.DatatypeProperty))
    g.add((G2_ONT.añoFiscal, RDFS.domain, DCAT.Distribution))
    g.add((G2_ONT.añoFiscal, RDFS.range, XSD.gYear))

    g.add((G2_ONT.capitulo, RDF.type, OWL.DatatypeProperty))
    g.add((G2_ONT.articulo, RDF.type, OWL.DatatypeProperty))

    g.add((G2_ONT.creditoInicial, RDF.type, OWL.DatatypeProperty))
    g.add((G2_ONT.creditoInicial, RDFS.domain, G2_ONT.PartidaGasto))
    g.add((G2_ONT.creditoInicial, RDFS.range, SCHEMA.MonetaryAmount))

    # --- Propiedades de Objeto (Object Properties) ---
    g.add((G2_ONT.convocatoriaAsociada, RDF.type, OWL.ObjectProperty))
    g.add((G2_ONT.convocatoriaAsociada, RDFS.domain, G2_ONT.AyudaConcedida))
    g.add((G2_ONT.convocatoriaAsociada, RDFS.range, G2_ONT.ConvocatoriaAyuda))

    print("--- Tarea 0 Completada ---")

    # --- TAREA 1: Generar Metadatos DCAT (Lo que pide el profe) ---
    print("--- Iniciando Tarea 1: Generar meta-esquema DCAT ---")

    # Creamos el nodo de la Universidad (Publisher) y añadimos sus IDs
    # === MODIFICACIÓN ===
    g.add((UAM_URI, RDF.type, G2_ONT.Universidad))  # Usamos nuestra clase
    g.add((UAM_URI, SCHEMA.name, Literal("Universidad Autónoma de Madrid")))
    g.add(
        (UAM_URI, DCTERMS.identifier, Literal(UAM_CODIGO))
    )  # ID de los CSVs de Ayudas/Presupuestos
    g.add((UAM_URI, SCHEMA.vatID, Literal(UAM_NIFOC)))  # ID del CSV de Licitaciones

    # === MODIFICACIÓN: El "Vínculo de Oro" ===
    g.add((UAM_URI, OWL.sameAs, WD.Q233939))  # Enlace a Wikidata
    g.add(
        (UAM_URI, OWL.sameAs, DBR.Autonomous_University_of_Madrid)
    )  # Enlace a DBpedia

    # Mapa para guardar las URIs de las distribuciones
    distribuciones_uris = {}

    for config in CATALOGO_CONFIG:
        print(f"Generando metadatos DCAT para: {config['entidad']}")

        # 1. Crear el dcat:Dataset
        dataset_uri = G2_DATA[config["dataset_id"]]
        g.add((dataset_uri, RDF.type, DCAT.Dataset))
        g.add((dataset_uri, DCTERMS.title, Literal(config["titulo_dataset"])))
        g.add((dataset_uri, DCTERMS.description, Literal(config["desc_dataset"])))
        g.add((dataset_uri, DCTERMS.publisher, UAM_URI))  # Enlaza al publisher
        for kw in config["keywords"]:
            g.add((dataset_uri, DCAT.keyword, Literal(kw)))

        # 2. Crear la dcat:Distribution (el CSV)
        # Usar un ID único que incluya el año para evitar colisiones
        dist_id = f"distribucion-{config['entidad'].lower()}-{config['año_fiscal']}"
        dist_uri = G2_DATA[dist_id]
        distribuciones_uris[f"{config['entidad']}-{config['año_fiscal']}"] = (
            dist_uri  # Guardamos con clave única
        )

        g.add((dataset_uri, DCAT.distribution, dist_uri))
        g.add((dist_uri, RDF.type, DCAT.Distribution))
        g.add((dist_uri, DCTERMS.title, Literal(config["archivo_csv"])))
        g.add((dist_uri, DCAT.mediaType, Literal("text/csv")))
        # Usamos una IRI relativa válida con barras POSIX (evita backslashes de Windows)
        rel_dir = BASE_CSV_PATH.relative_to(Path(__file__).parent.parent)
        rel_download_path = (rel_dir / Path(config["archivo_csv"]).name).as_posix()
        g.add((dist_uri, DCAT.downloadURL, URIRef(rel_download_path)))

        # 3. Aplicar la EXTENSIÓN (propiedad 'año_fiscal')
        g.add(
            (
                dist_uri,
                G2_ONT.añoFiscal,
                Literal(config["año_fiscal"], datatype=XSD.gYear),
            )
        )

    print("--- Tarea 1 Completada ---")

    # --- TAREA 2: Procesar el CONTENIDO de los CSVs (Tu ERD) ---
    print("--- Iniciando Tarea 2: Procesar contenido de CSVs ---")

    for config in CATALOGO_CONFIG:
        entidad = config["entidad"]
        año = config["año_fiscal"]
        dist_uri = distribuciones_uris[
            f"{entidad}-{año}"
        ]  # Recuperamos la URI del CSV con clave única

        # Obtenemos la función de procesado correcta del diccionario
        funcion_a_llamar_str = config["procesar_funcion"]
        funcion_a_llamar = FUNCIONES_PROCESADO.get(funcion_a_llamar_str)

        if funcion_a_llamar:
            funcion_a_llamar(g, config, dist_uri)
        else:
            print(f"  AVISO: No se encontró función de procesado para '{entidad}'")

    print("--- Tarea 2 Completada ---")

    # --- 5. Guardar el grafo completo ---
    # Crear el directorio de salida si no existe
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / args.output_file
    try:
        g.serialize(destination=str(output_file), format="turtle")
        print(f"\n¡ÉXITO! Grafo RDF total guardado en {output_file}")
        print(f"Ubicación absoluta: {output_file.absolute()}")
        print(f"Tamaño: {output_file.stat().st_size / 1024:.2f} KB")
        print("Sube este único fichero a tu repositorio de GraphDB.")
    except Exception as e:
        print(f"\nERROR al guardar el fichero: {e}")


if __name__ == "__main__":
    main()
