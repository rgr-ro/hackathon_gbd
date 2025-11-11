#!/bin/bash
# Script de ayuda para gestionar el servicio de carga de datos

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

function show_help() {
    cat << EOF
Uso: $0 [comando]

Comandos disponibles:
    build       - Construir la imagen Docker del servicio load_data
    run         - Ejecutar el servicio de carga de datos
    logs        - Mostrar logs del servicio
    status      - Verificar el estado del servicio
    restart     - Reiniciar el servicio de carga
    clean       - Limpiar la base de datos y recargar desde cero
    help        - Mostrar esta ayuda

Ejemplos:
    $0 build
    $0 run
    $0 logs
EOF
}

function build_service() {
    echo "🔨 Construyendo imagen load_data..."
    docker-compose build load_data
    echo "✅ Imagen construida exitosamente"
}

function run_service() {
    echo "🚀 Ejecutando servicio de carga de datos..."
    docker-compose up load_data
}

function show_logs() {
    echo "📋 Mostrando logs del servicio load_data..."
    docker-compose logs -f load_data
}

function show_status() {
    echo "📊 Estado de los servicios:"
    docker-compose ps db descarga_datos load_data
}

function restart_service() {
    echo "🔄 Reiniciando servicio de carga de datos..."
    docker-compose stop load_data
    docker-compose rm -f load_data
    docker-compose up -d load_data
    echo "✅ Servicio reiniciado"
}

function clean_and_reload() {
    echo "⚠️  ADVERTENCIA: Esto eliminará todos los datos de la base de datos"
    read -p "¿Estás seguro? (yes/no): " confirm
    if [ "$confirm" != "yes" ]; then
        echo "❌ Operación cancelada"
        exit 0
    fi
    
    echo "🧹 Limpiando base de datos..."
    docker-compose down
    docker volume rm hackathon_gbd_db_data 2>/dev/null || true
    
    echo "🚀 Reiniciando servicios..."
    docker-compose up -d db descarga_datos
    
    echo "⏳ Esperando a que los servicios estén listos..."
    sleep 10
    
    echo "📥 Cargando datos..."
    docker-compose up load_data
    
    echo "✅ Recarga completada"
}

# Main
case "${1:-help}" in
    build)
        build_service
        ;;
    run)
        run_service
        ;;
    logs)
        show_logs
        ;;
    status)
        show_status
        ;;
    restart)
        restart_service
        ;;
    clean)
        clean_and_reload
        ;;
    help|*)
        show_help
        ;;
esac
