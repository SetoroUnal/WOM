create or replace PROCEDURE PROC_LLAMARON_A_COMPETENCIA AS 
BEGIN
    --DETECTANDO NÚMEROS EN SMS
    INSERT /*+APPEND*/ INTO CR_DL_NUMEROS_OPERADORES NOLOGGING
    WITH
        NUMEROS_SMS AS( 
            SELECT /*+PARALLEL(4)*/
                LTRIM(TELEFONO_COMPETENCIA) AS NUMERO_COMPETENCIA,
                OPERADOR
            FROM DP_TRAFICO_SMS    
        )
    
        SELECT /*+PARALLEL(4)*/
            TO_NUMBER(NUMERO_COMPETENCIA) AS NUMERO,
            OPERADOR,
            FECHA_CARGUE
        FROM (
        SELECT /*+PARALLEL(4)*/
            A.NUMERO_COMPETENCIA,
            A.OPERADOR,
            TO_NUMBER(TO_CHAR(SYSDATE -1 , 'YYYYMMDD')) AS FECHA_CARGUE,
            ROW_NUMBER() OVER (PARTITION BY A.NUMERO_COMPETENCIA ORDER BY FECHA_CARGUE DESC) AS RN
        FROM NUMEROS_SMS A
        LEFT JOIN CR_DL_NUMEROS_OPERADORES B ON TO_CHAR(A.NUMERO_COMPETENCIA) = TO_CHAR(B.NUMERO)
        WHERE B.NUMERO IS NULL
            AND REGEXP_LIKE(A.NUMERO_COMPETENCIA, '^[[:space:]]*[0-9]+[[:space:]]*$')
        ) 
        WHERE RN = 1
    ;
    COMMIT;

    --TRAFICO
    INSERT /*+APPEND*/  INTO LLAMARON_A_COMPETENCIA NOLOGGING
        WITH 
            TRAFICO_VOZ AS(
                SELECT /*+PARALLEL(4)*/
                    A.SUBSCRIBER_ID,
                    TO_DATE(A.PERIODO_PROCESO_CODIGO ,'YYYY/MM/DD') AS FECHA_LLAMADA,
                    1 AS LLAMO_A_LA_COMPETENCIA,
                    A.SERVICIO,
                    A.NUMERO_DESTINO
                FROM DWH_BODEGA_WOM.FCT_TRAFICO_VOZ A
                WHERE A.SENTIDO = 'SALIENTE'
                    AND A.PERIODO_PROCESO_CODIGO = TO_NUMBER(TO_CHAR(SYSDATE-1, 'YYYYMMDD'))
            )
        
            SELECT /*+PARALLEL(4)*/
                A.SUBSCRIBER_ID,
                A.FECHA_LLAMADA,
                A.LLAMO_A_LA_COMPETENCIA,
                A.SERVICIO
            FROM TRAFICO_VOZ A
            LEFT JOIN CR_DL_NUMEROS_OPERADORES B ON TO_CHAR(A.NUMERO_DESTINO) = TO_CHAR(B.NUMERO)
            WHERE B.OPERADOR IS NOT NULL;
    
    COMMIT;

END;
