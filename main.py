# -*- coding: utf-8 -*-
"""
Created on Wed Sep 10 17:19:13 2025

@author: Diego
"""

from fastapi import FastAPI, HTTPException
import requests
from google.cloud import bigquery
import json
from datetime import datetime
import pandas as pd
import os


# Configuración
API_LOCAL_URL = os.getenv("API_LOCAL_URL")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = os.getenv("DATASET_ID")
TABLE_ID = os.getenv("TABLE_ID")
TOKEN = os.getenv("TOKEN_CR")
TOKEN_INDUSTRY = os.getenv("TOKEN_CR_INDUSTRY")  # segunda fuente usando misma URL

def _fetch_and_process_single(api_url: str, token: str, source_name: str, empresa_label: str) -> pd.DataFrame | None:
    """Descarga y transforma datos desde una fuente específica.
    
    Retorna un DataFrame normalizado o None si no hay datos.
    """
    print(f"=== OBTENIENDO Y PROCESANDO DATOS ({source_name}) ===")

    if not api_url:
        print(f"⚠️ URL no configurada para la fuente: {source_name}")
        return None

    headers = {"method": "report", "token": token}
    print(f"[{source_name}] API URL: {api_url}")
    print(f"[{source_name}] Headers: {{'method': 'report', 'token': '***'}}")

    try:
        print(f"🔄 Iniciando llamada a fuente {source_name}...")
        response = requests.get(api_url, headers=headers, timeout=3600)
        print(f"✅ Llamada completada ({source_name})")
    except requests.exceptions.Timeout:
        error_msg = f"Timeout: La API {source_name} tardó más de 1 hora en responder"
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=504, detail=error_msg)
    except requests.exceptions.ConnectionError as e:
        error_msg = f"Error de conexión con la API {source_name}: {str(e)}"
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=502, detail=error_msg)
    except requests.exceptions.RequestException as e:
        error_msg = f"Error en la petición HTTP ({source_name}): {str(e)}"
        print(f"❌ {error_msg}")
        raise HTTPException(status_code=502, detail=error_msg)
    except Exception as e:
        error_msg = f"Error inesperado en {source_name}: {type(e).__name__}: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        print(f"❌ Stack trace: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=error_msg)
    
    print(f"[{source_name}] Status code: {response.status_code}")
    response.raise_for_status()
    data_text = response.text
    print(f"[{source_name}] Longitud de respuesta: {len(data_text)}")
    print(f"[{source_name}] Primeros 200 caracteres: {data_text[:200]}")
    
    data_json = json.loads(data_text)
    print(f"[{source_name}] Datos obtenidos: {len(data_json)} registros")
    print(f"[{source_name}] Primer registro: {data_json[0] if data_json else 'No hay datos'}")
    
    if not data_json:
        print(f"[{source_name}] No hay datos para procesar")
        return None

    # Convertir a DataFrame
    data = pd.DataFrame(data_json)
    date_columns = ['Her', 'FlogAsi','Hsr','Entrada', 'Salida']
    print(f"[{source_name}] Transformando columnas a formato datetime")
    for col in date_columns:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], format='%d-%m-%Y %H:%M:%S', errors='coerce')
            

    datetime_columns=['FechaMarcaEntrada','FechaMarcaSalida']
    print(f"[{source_name}] Transformando columnas a formato datetime")
    for col in datetime_columns:
        if col in data.columns:
            data[col] = pd.to_datetime(data[col], format='%Y-%m-%d %H:%M:%S')
    
    # Normalizar nombres de columnas
    data.columns = data.columns.str.lower()
    data.columns = data.columns.str.replace(' ', '_')
    data.columns = data.columns.str.replace('.', '')
    data.columns = data.columns.str.replace('%', '')
    data.columns = data.columns.str.replace('-', '_')
    data.columns = data.columns.str.replace('(', '')
    data.columns = data.columns.str.replace(')', '')
    data.columns = data.columns.str.replace('á', 'a')
    data.columns = data.columns.str.replace('é', 'e')
    data.columns = data.columns.str.replace('í', 'i')
    data.columns = data.columns.str.replace('ó', 'o')
    data.columns = data.columns.str.replace('ú', 'u')
    data.columns = data.columns.str.replace('ñ', 'n')
    data.columns = data.columns.str.replace('°', '')


    number_columns=['hrtotrol','hr_tot_asi']
    print(f"[{source_name}] Transformando columnas a formato float")
    for col in number_columns:
        if col in data.columns:
            data[col] = pd.to_numeric(
                data[col].astype(str)                      # asegura string
                         .str.replace(".", "", regex=False)  # quita miles
                         .str.replace(",", ".", regex=False) # coma -> punto
                         .str.strip(),                        # quita espacios
                errors="coerce"
            )

    # Agregar columna 'empresa' antes de devolver
    data['empresa'] = empresa_label

    print(f"✅ Datos procesados exitosamente ({source_name}): {len(data)} registros")
    return data

def fetch_and_process_data():
    """Obtiene y procesa datos. Si existe una segunda fuente, unifica ambas antes de retornar."""
    print("=== OBTENIENDO Y PROCESANDO DATOS (MODO MULTIFUENTE) ===")

    # Validaciones: siempre debemos tener ambas fuentes
    if not API_LOCAL_URL:
        raise HTTPException(status_code=400, detail="Falta variable de entorno API_LOCAL_URL")
    if not TOKEN:
        raise HTTPException(status_code=400, detail="Falta variable de entorno TOKEN_CR")
    if not TOKEN_INDUSTRY:
        raise HTTPException(status_code=400, detail="Falta variable de entorno TOKEN_CR_INDUSTRY")

    # Fuente 1: Security
    df1 = _fetch_and_process_single(API_LOCAL_URL, TOKEN, "Fuente 1 - Security", "Security")
    # Fuente 2: Industry (misma URL, distinto token)
    df2 = _fetch_and_process_single(API_LOCAL_URL, TOKEN_INDUSTRY, "Fuente 2 - Industry", "Industry")

    # Si alguna viene vacía, considerar esto un error ya que siempre esperamos ambas
    if df1 is None or df2 is None:
        raise HTTPException(status_code=500, detail="Alguna de las fuentes no retornó datos")

    # Unificar columnas y concatenar
    print("🔗 Unificando datasets de Fuente 1 y Fuente 2")
    all_columns = sorted(set(df1.columns).union(set(df2.columns)))
    df1_aligned = df1.reindex(columns=all_columns)
    df2_aligned = df2.reindex(columns=all_columns)
    merged = pd.concat([df1_aligned, df2_aligned], ignore_index=True, sort=False)

    # Eliminar duplicados exactos si existieran
    before = len(merged)
    merged = merged.drop_duplicates()
    after = len(merged)
    print(f"🧹 Registros antes: {before} | después de eliminar duplicados: {after}")

    print(f"✅ Datos combinados listos: {len(merged)} registros")
    return merged

def load_to_bigquery(df_bridge):
    """Función para cargar datos procesados a BigQuery"""
    if df_bridge is None:
        return {
            "success": True,
            "message": "No hay datos para cargar",
            "records_processed": 0
        }
    
    print("=== CARGANDO DATOS A BIGQUERY ===")
    
    try:
        client = bigquery.Client(project=PROJECT_ID)
        table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        )
        
        print(f"🔄 Cargando {len(df_bridge)} registros a BigQuery: {table_id}")
        job = client.load_table_from_dataframe(df_bridge, table_id, job_config=job_config)
        job.result()
        
        print(f"✅ Data cargada exitosamente. {len(df_bridge)} registros cargados a BigQuery.")
        
        return {
            "success": True,
            "message": "Data procesada y cargada exitosamente",
            "records_processed": len(df_bridge)
        }
    except Exception as e:
        error_msg = f"Error al cargar datos en BigQuery: {type(e).__name__}: {str(e)}"
        print(f"❌ {error_msg}")
        import traceback
        print(f"❌ Stack trace: {traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=error_msg)

def sync_to_bigquery():
    """Función principal para sincronizar datos con BigQuery"""
    print("=== INICIANDO SINCRONIZACIÓN COMPLETA ===")
    
    # Paso 1: Obtener y procesar datos
    df_bridge = fetch_and_process_data()
    
    # Paso 2: Cargar a BigQuery
    result = load_to_bigquery(df_bridge)
    
    return result

# Crear la aplicación FastAPI
app = FastAPI()

@app.get("/")
def root():
    return {"message": "Servicio de sincronización de rotación activo"}

@app.get("/health")
def health_check():
    """Endpoint de salud para verificar el estado del servicio"""
    return {
        "status": "healthy",
        "message": "Servicio funcionando correctamente",
        "timestamp": datetime.now().isoformat()
    }

@app.post("/fetch_data")
def fetch_data():
    """
    Endpoint para obtener y procesar datos de la API externa (sin cargar a BigQuery)
    """
    try:
        df_bridge = fetch_and_process_data()
        if df_bridge is None:
            return {
                "success": True,
                "message": "No hay datos para procesar",
                "records_processed": 0
            }
        
        return {
            "success": True,
            "message": "Datos obtenidos y procesados exitosamente",
            "records_processed": len(df_bridge),
            "columns": list(df_bridge.columns),
            "sample_data": df_bridge.head(3).to_dict('records') if len(df_bridge) > 0 else []
        }
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al obtener y procesar datos"
        }
        raise HTTPException(status_code=500, detail=error_response)

@app.post("/load_data")
def load_data():
    """
    Endpoint para cargar datos procesados a BigQuery
    """
    try:
        # Primero obtener los datos
        df_bridge = fetch_and_process_data()
        
        # Luego cargarlos a BigQuery
        result = load_to_bigquery(df_bridge)
        return result
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al cargar datos a BigQuery"
        }
        raise HTTPException(status_code=500, detail=error_response)

@app.post("/rotacion_sync")
def rotacion_sync():
    """
    Endpoint para sincronizar datos de rotación (proceso completo)
    """
    try:
        result = sync_to_bigquery()
        return result
    except Exception as e:
        error_response = {
            "success": False,
            "error": str(e),
            "message": "Error al procesar la sincronización"
        }
        raise HTTPException(status_code=500, detail=error_response)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))





