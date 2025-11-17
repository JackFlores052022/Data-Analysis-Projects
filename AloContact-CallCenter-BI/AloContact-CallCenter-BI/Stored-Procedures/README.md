# Stored Procedures - Call Center BI

Procedimientos almacenados para la sumarización y análisis de métricas de call center.

## Arquitectura

El sistema utiliza dos stored procedures complementarios:

### 1. sp-incremental-load.sql
**Propósito:** Actualización incremental cada 5 minutos  
**Ventana de tiempo:** Últimas 2 horas  
**Uso:** Mantener dashboards en tiempo real  
**Tiempo de ejecución:** 3-5 minutos

### 2. sp-full-load.sql
**Propósito:** Carga completa histórica  
**Ventana de tiempo:** Todos los datos  
**Uso:** Inicialización o reconstrucción completa  
**Tiempo de ejecución:** 30-35 minutos

## Métricas Calculadas

- **TMO (Tiempo Medio Operativo):** Duración promedio de llamadas
- **ACW (After Call Work):** Tiempo promedio post-llamada
- **Tasas de abandono:** Segmentadas por rangos de tiempo
- **Llamadas atendidas vs. no atendidas**
- **Distribución horaria**

## Optimizaciones Implementadas

- Uso de tablas temporales con índices
- Procesamiento incremental para reducir carga
- NOLOCK hints para evitar bloqueos
- Índices estratégicos en campos clave

## Tablas Involucradas

- `ticket` - Registro de llamadas
- `client` - Información de clientes
- `SumarizadoLlamadas` - Tabla de métricas consolidadas

