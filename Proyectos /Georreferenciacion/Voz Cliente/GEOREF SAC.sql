DROP TABLE WOM_AA_GEOREF_SAC ;
COMMIT;

CREATE TABLE WOM_AA_GEOREF_SAC AS 

WITH SAC AS (
SELECT DISTINCT
ORDER_ID,
A.ORDER_CODE,
F.CLIENTE_NOMBRE,
A.CLIENTE_DK CUST_ID,
F.CLIENTE_TIPO_DOCUMENTO,
F.CLIENTE_NIT_CEDULA,
F.CLIENTE_DIRECCION_CIUDAD CIUDAD,
F.CLIENTE_DIRECCION_DPTO DEPARTAMENTO,
MSISDN SERVICE_NUMBER,
SUBSTR(TIEMPO_APERTURA_DK,7,2)||'/'||substr(TIEMPO_APERTURA_DK,5,2)||'/'||SUBSTR(TIEMPO_APERTURA_DK,1,4) CREATED_DATE,
CANAL_CONTACTO SOURCE,
COMENTARIOS COMMENTS,
SERVICE_TYPE1,
SERVICE_TYPE2,
SERVICE_TYPE3,
SERVICE_TYPE4,
D.CONSULTOR_NOMBRE STAFF_CREA,
D.CONSULTOR_ALMACEN_NOMBRE ORG_CREA,
ORDER_STATE CURRENT_STATE,
CUN,
SIC_STATE,
TIPO_SOLUCION_CANCELACION,
CIUDAD_FALLA,
SERVICIO_FUE_RESTABLECIDO,
TIPO_DE_SMS_NO_RECIBE,
IMEI,
SOLUCION_RED_Y_SERVICIO,
NO_PEDIDO,
TIPO_NOVEDAD,
TIPO_SOLUCION_NOVEDAD,
PUNTO_VENTA_DIRECTO,
PUNTO_VENTA_INDIRECTO,
E.CONSULTOR_ALMACEN_NOMBRE ORG_NAME_CIERRE,
E.CONSULTOR_NOMBRE STAFF_CIERRE,
SUBSTR(TIEMPO_CIERRE_DK,7,2)||'/'||SUBSTR(TIEMPO_CIERRE_DK,5,2)||'/'||SUBSTR(TIEMPO_CIERRE_DK,1,4) CLOSE_DATE,
TIPO_CONSULTOR_ACTUAL,
B.CONSULTOR_NOMBRE CONSULTOR_NOMBRE_ACTUAL,
A.ORGANIZACION_ACTUAL ORGANIZACION_ACTUAL,
B.CONSULTOR_CANAL_NOMBRE,
FAVORABILIDAD,
NOTAS_CIERRE,
RAZON_CIERRE,
TIPO_PLAN,
G.VALOR_ATRIBUTO ESCALAMIENTO
FROM
(SELECT * FROM DWH_BODEGA_WOM.FCT_CASOS_ABIERTOS
              WHERE TIEMPO_APERTURA_DK  > '20230627' -- AJUSTAR DE ACUERDO A FECHA NECESARIA
              UNION
              SELECT * FROM DWH_BODEGA_WOM.FCT_CASOS_CERRADOS
              WHERE TIEMPO_APERTURA_DK  > '20230627'  -- AJUSTAR DE ACUERDO A FECHA NECESARIA
) A 
LEFT JOIN  DWH_BODEGA_WOM.DIM_CONSULTORES B ON A.CONSULTOR_ACTUAL_DK = B.CONSULTOR_DK
AND TIEMPO_APERTURA_DK BETWEEN TO_CHAR(CONSULTOR_FECHA_INI_VIG,'YYYYMMDD') AND TO_CHAR(CONSULTOR_FECHA_FIN_VIG,'YYYYMMDD')
LEFT JOIN (select SERVICE_TYPE1,SERVICE_TYPE2,SERVICE_TYPE3,SERVICE_TYPE4,SERVICE_TYPE_DK from DWH_BODEGA_WOM.DIM_SERVICE_TYPES) C
ON A.SERVICE_TYPE_DK =  C.SERVICE_TYPE_DK 
LEFT JOIN  DWH_BODEGA_WOM.DIM_CONSULTORES  D ON A.CONSULTOR_APERTURA_DK = D.CONSULTOR_DK
LEFT JOIN  DWH_BODEGA_WOM.DIM_CONSULTORES  E ON A.CONSULTOR_cierre_DK = E.CONSULTOR_DK
LEFT JOIN  DWH_BODEGA_WOM.DIM_CLIENTES F ON A.CLIENTE_DK = F.CLIENTE_DK
LEFT JOIN  (SELECT ORDER_CODE,CODIGO_ATRIBUTO,VALOR_ATRIBUTO FROM DWH_CONSULTA_WOM.QRY_CASOS_ST_ATRR WHERE CODIGO_ATRIBUTO = '2241') G -- valor atrubuto escalamiento
ON A.ORDER_CODE = G.ORDER_CODE
WHERE A.ORDER_STATE in ('Cerrar','Enviar')
and CANAL_CONTACTO = 'Call Center Inbound'
)

,SAC2 AS (
select 
SERVICE_NUMBER AS MSISDN,
CREATED_DATE,
TO_CHAR (TO_DATE(created_date ,'DD/MM/YY'),'IW') as SEMANA_QUEJA ,
'SAC' AS CATEGORIA , 
SERVICE_TYPE1,
SERVICE_TYPE2,
SERVICE_TYPE3
from SAC A)

SELECT /*+PARALLEL(4)*/  
CREATED_DATE,
SEMANA_QUEJA ,
CATEGORIA , 
SERVICE_TYPE1,
SERVICE_TYPE2,
SERVICE_TYPE3,
B.*
FROM SAC2 A
INNER JOIN wom_aa_top3_celdas_lite_filas B
ON A.MSISDN = B.MSISDN 
WHERE B.SEMANA = A.SEMANA_QUEJA
OR B.SEMANA = A.SEMANA_QUEJA -1
;
COMMIT
;

select * from WOM_AA_GEOREF_SAC