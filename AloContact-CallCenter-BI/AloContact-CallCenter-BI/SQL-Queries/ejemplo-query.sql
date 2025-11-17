-- Example: Daily call metrics
-- Description: Obtiene métricas diarias de llamadas

SELECT 
    CAST(fecha AS DATE) as fecha_llamada,
    COUNT(*) as total_llamadas,
    COUNT(CASE WHEN estado = 'contestada' THEN 1 END) as llamadas_contestadas,
    AVG(duracion_segundos) as duracion_promedio
FROM 
    llamadas
WHERE 
    fecha >= DATEADD(day, -30, GETDATE())
GROUP BY 
    CAST(fecha AS DATE)
ORDER BY 
    fecha_llamada DESC;
