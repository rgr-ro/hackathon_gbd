# Directorio para archivos TTL (Turtle RDF)

Este directorio contiene los grafos RDF generados en formato Turtle (.ttl).

## Archivos

- `grafo_completo.ttl`: Grafo RDF generado por el servicio `create_graph` con todos los datos de la UAM.

## Uso

Este directorio está montado como volumen en:

- **create_graph**: Escribe el grafo TTL aquí
- **graphdb**: Lee los archivos TTL desde aquí para importarlos

## Estructura

Los archivos TTL contienen triples RDF que representan:

- Universidades (UAM)
- Licitaciones y contratos
- Presupuestos (gastos e ingresos)
- Convocatorias de ayudas
- Ayudas concedidas
- Enlaces a recursos externos (DBpedia, Wikidata)

## Formato

Los archivos siguen el formato Turtle (TTL), un formato de serialización RDF legible por humanos.

Ejemplo:

```turtle
@prefix uam: <http://uam.es/ontology/> .
@prefix schema: <https://schema.org/> .

<http://example.org/universidad/UAM> a uam:Universidad ;
    schema:name "Universidad Autónoma de Madrid" .
```
