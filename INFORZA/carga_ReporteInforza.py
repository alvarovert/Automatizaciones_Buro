import pandas as pd
from sqlalchemy import create_engine, text
import urllib
from datetime import datetime
import os

# ================= CONFIGURACIÓN =================
SERVER = r'ssss.sssss.ssss.ss\ssssss'
DATABASE = 'sssssssss'
USER = 'sssssssss'      # <<<< CAMBIAR
PASSWORD = 'ssssssss'     # <<<< CAMBIAR

BASE_DIR = r'C:\Users\ssssssssss\REPORTES' # <<<< CAMBIAR

# Generación dinámica del nombre del archivo: INFORZA_EFECTIVO_YYYYMM.xlsx
fecha_actual = datetime.now()
anio_mes = fecha_actual.strftime("%Y%m") 
nombre_archivo = f"INFORZA_EFECTIVO_{anio_mes}.xlsx" # INFORZA_EFECTIVO_{anio_mes}.xlsx
ruta_completa = os.path.join(BASE_DIR, nombre_archivo)

# ================= CONEXIÓN SQL =================
print(f"--- Iniciando proceso ETL: {datetime.now()} ---")
print(f"--- Archivo objetivo: {nombre_archivo} ---")

params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD}"
)
# fast_executemany=True es VITAL para rendimiento
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

# ================= 1. LIMPIEZA (TRUNCATE) =================
print("1. Limpiando tablas de destino...")
try:
    with engine.connect() as conn:
        # Tablas de Efectivo y Electro
        conn.execute(text("TRUNCATE TABLE prueba_inforza_desembolso"))
        conn.execute(text("TRUNCATE TABLE PRUEBA_INFORZA_ELECTRO"))
        
        # Tablas de Efectinegocio
        conn.execute(text("TRUNCATE TABLE DETALLE_DESEMB_EFECTINEGOCIO"))
        conn.execute(text("TRUNCATE TABLE GAC_Efectinegocio")) 
        
        conn.commit()
        print("   Tablas limpiadas correctamente.")
except Exception as e:
    print(f"   Error crítico limpiando tablas: {e}")
    exit()

# ================= 2. FUNCIÓN DE AYUDA =================
def insertar_en_sql(df, nombre_tabla, mapeo):
    """Filtra, renombra e inserta un DataFrame en SQL."""
    try:
        # Filtrar columnas que existen en el DF
        cols_necesarias = list(mapeo.keys())
        cols_existentes = [c for c in cols_necesarias if c in df.columns]
        
        # Renombrar
        df_final = df[cols_existentes].rename(columns=mapeo)
        
        print(f"   -> Insertando {len(df_final)} filas en tabla: {nombre_tabla}")
        df_final.to_sql(
            nombre_tabla, 
            con=engine, 
            if_exists='append', 
            index=False, 
            schema='dbo',
            chunksize=1000 
        )
    except Exception as e:
        print(f"   Error insertando en {nombre_tabla}: {e}")

# ================= 3. EJECUCIÓN DE CARGAS =================

# ---------------------------------------------------------
# A. CARGA EFECTIVO 
# ---------------------------------------------------------
print("\n[A] Procesando EFECTIVO...")
try:
    # 1. Lectura con tipos forzados
    df_efectivo = pd.read_excel(ruta_completa, sheet_name='Efectivo', 
                                dtype={'DNI': str, 'PRESTAMO': str, 'COD_AGENCIA': str})
    
    # --- LIMPIEZA DE DATOS CLAVE ---
    # DNI: Sin espacios, sin nulos, sin .0
    df_efectivo['DNI'] = df_efectivo['DNI'].fillna('').astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    
    # PRESTAMO: Sin espacios, nulos se vuelven '0', sin .0
    df_efectivo['PRESTAMO'] = df_efectivo['PRESTAMO'].fillna('0').astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_efectivo['PRESTAMO'] = df_efectivo['PRESTAMO'].replace(['nan', '', 'None'], '0')
    # ---------------------------------------------------------------------

    # Transformación Fechas
    if 'FECHA_HORA_DESEMBOLSO' in df_efectivo.columns:
        df_efectivo['FECHA_DESEMBOLSO'] = pd.to_datetime(df_efectivo['FECHA_HORA_DESEMBOLSO'], errors='coerce').dt.date
    if 'COD_AGENCIA' in df_efectivo.columns:
        df_efectivo['COD_AGENCIA'] = pd.to_numeric(df_efectivo['COD_AGENCIA'], errors='coerce').fillna(0).astype(int)

    mapeo_efectivo = {
        'DNI': 'DNI', 
        'PRESTAMO': 'PRESTAMO', 
        'agencia': 'COD_AGENCIA',
        'nombre_agencia': 'AGENCIA', 
        'CANAL_CONFIRMADO': 'CANAL_CONFIRMADO',
        'FECHA_HORA_DESEMBOLSO': 'FECHA_HORA_DESEMBOLSO', 
        'MONTO_NETO': 'MONTO_NETO',
        'desbase': 'F45', 'TIPO': 'Tipo_Gestion', 
        'FECHA_GESTION': 'FECHA_GESTION',
        'CODIGO_GESTION': 'INFORZA_CG', 
        'FECHA_COMPROMISO': 'INFORZA_20_FC',
        'OBSERVACIÓN': 'INFORZA_20_OBS', 
        'PERFIL': 'PERFIL',
        'FECHA_DESEMBOLSO': 'FECHA_DESEMBOLSO'
    }
    insertar_en_sql(df_efectivo, 'prueba_inforza_desembolso', mapeo_efectivo)
    del df_efectivo 
except Exception as e:
    print(f"Error en Efectivo: {e}")

# ---------------------------------------------------------
# B. CARGA ELECTRO (CORREGIDO)
# ---------------------------------------------------------
print("\n[B] Procesando ELECTRO...")
try:
    # 1. Lectura con tipos forzados
    df_electro = pd.read_excel(ruta_completa, sheet_name='Electro',
                               dtype={'DNI': str, 'PRESTAMO': str, 'COD_AGENCIA': str})
    
    # --- LIMPIEZA DE DATOS CLAVE ---
    df_electro['DNI'] = df_electro['DNI'].fillna('').astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    
    df_electro['PRESTAMO'] = df_electro['PRESTAMO'].fillna('0').astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_electro['PRESTAMO'] = df_electro['PRESTAMO'].replace(['nan', '', 'None'], '0')

    # 2. Transformaciones de Fechas (Date)
    cols_fecha = ['FECHA_GESTION', 'FECHA_DESEMBOLSO', 'FECHA_COMPROMISO']
    for c in cols_fecha:
        if c in df_electro.columns:
            df_electro[c] = pd.to_datetime(df_electro[c], errors='coerce').dt.date
    
    # === CORRECCIÓN DE HORA (SOLUCIÓN AL ERROR DATETIME OVERFLOW) ===
    # Eliminamos los milisegundos excesivos de las columnas de hora
    cols_hora = ['HORADESEMBOLSO', 'HORA_GESTION']
    for c in cols_hora:
        if c in df_electro.columns:
            # Convierte a string, corta después del punto decimal
            df_electro[c] = df_electro[c].astype(str).str.split('.').str[0]
            # Limpia valores nulos convertidos a string 'nan'
            df_electro[c] = df_electro[c].replace('nan', None)
    # ================================================================

    if 'COD_AGENCIA' in df_electro.columns:
        df_electro['COD_AGENCIA'] = pd.to_numeric(df_electro['COD_AGENCIA'], errors='coerce').fillna(0).astype(int)

    mapeo_electro = {
        'PRESTAMO': 'PRESTAMO', 
        'PERFIL': 'PERFIL', 
        'MONTO': 'MONTO',
        'HORADESEMBOLSO': 'HORADESEMBOLSO', 
        'HORA_GESTION': 'HG_20',
        'FECHA_GESTION': 'FECHA_GESTION', 
        'FECHA_DESEMBOLSO': 'FECHA_DESEMBOLSO',
        'FECHA_COMPROMISO': 'FC_20', 
        'DNI': 'DNI', 
        'CODIGO_GESTION': 'CG_20',
        'CODIGO_AGENCIA': 'CODIGO_AGENCIA', 
        'canal_confirmado': 'canal_confirmado',
        'AGENCIA': 'TIENDA'
    }
    insertar_en_sql(df_electro, 'PRUEBA_INFORZA_ELECTRO', mapeo_electro)
    del df_electro
except Exception as e:
    print(f"Error en Electro: {e}")

# ---------------------------------------------------------
# C. CARGA EFECTINEGOCIO 
# ---------------------------------------------------------
print("\n[C] Procesando EFECTINEGOCIO...")
try:
    # 1. Leemos la hoja UNA sola vez con tipos forzados
    df_negocio = pd.read_excel(ruta_completa, sheet_name='Efectinegocios',
                               dtype={'DNI': str, 'PRESTAMO': str, 'COD_AGENCIA': str})

    # --- LIMPIEZA DE DATOS CLAVE ---
    df_negocio['DNI'] = df_negocio['DNI'].fillna('').astype(str).str.strip().str.replace(r'\.0$', '', regex=True)

    df_negocio['PRESTAMO'] = df_negocio['PRESTAMO'].fillna('0').astype(str).str.strip().str.replace(r'\.0$', '', regex=True)
    df_negocio['PRESTAMO'] = df_negocio['PRESTAMO'].replace(['nan', '', 'None'], '0')

    # 2. Transformaciones de Fechas
    cols_fecha_neg = ['FECHA_GESTION', 'FECHA_DESEMBOLSO', 'FECHA_COMPROMISO']
    for c in cols_fecha_neg:
        if c in df_negocio.columns:
            df_negocio[c] = pd.to_datetime(df_negocio[c], errors='coerce').dt.date

    # --- DESTINO 1: GAC_Efectinegocio ---
    mapeo_gac = {
        'Codigo_Empresa': 'Codigo_Empresa',
        'FECHA_GESTION': 'FECHA_GESTION',
        'CANT': 'CANT'
    }
    insertar_en_sql(df_negocio, 'GAC_Efectinegocio', mapeo_gac)

    # --- DESTINO 2: DETALLE_DESEMB_EFECTINEGOCIO ---
    mapeo_detalle = {
        'TASA_NOMINAL': 'TEA',
        'SEGURO': 'SEGURO',
        'Resolucion': 'Resolucion',
        'PROVINCIA': 'PROVINCIA',
        'PRODUCTO': 'PRODUCTO',
        'PRESTAMO': 'PRESTAMO',
        'PLAZO': 'PLAZO',
        'Plaza_Creditos': 'Plaza_Creditos',
        'PERFIL_DETALLE': 'PERFIL_DETALLE',
        'PERFIL': 'Perfil',
        'NOM_VENDEDOR': 'NOM_VENDEDOR',
        'NOM_PRODUCTO': 'NOM_PRODUCTO',
        'NOM_FDN': 'NOM_FDN',
        'MONTO': 'MONTO_NETO',      
        'FUNCIONARIO': 'FUNCIONARIO',
        'FECHA_GESTION': 'FECHA_GESTION',
        'FECHA_DESEMBOLSO': 'FECHA_DESEMBOLSO',
        'FECHA_COMPROMISO': 'FECHA_COMPROMISO',
        'EMPRESA': 'EMPRESA',
        'DNI': 'DNI',
        'DISTRITO': 'DISTRITO',
        'DEPARTAMENTO': 'DEPARTAMENTO',
        'CODUSUARIOFDN': 'CODUSUARIOFDN',
        'CODIGO_GESTION': 'CODIGO_GESTION',
        'COD_VENDEDOR': 'COD_VENDEDOR',
        'COD_AGENCIA': 'COD_AGENCIA',
        'cantcruces': 'cantcruces',
        'AGENCIA': 'AGENCIA',
        'ZONA_CREDITOS': 'ZONA_CREDITOS',
        'USUARIO_FDN': 'USUARIO_FDN'
    }
    insertar_en_sql(df_negocio, 'DETALLE_DESEMB_EFECTINEGOCIO', mapeo_detalle)
    
    del df_negocio

except Exception as e:
    print(f"Error en Efectinegocio: {e}")

# ================= 4. EJECUCIÓN SP FINAL =================
print("\n[4] Ejecutando SP Post-Carga...")
try:
    with engine.connect() as conn:
        conn.execute(text("EXEC [dbo].[AS_TABLERO_INFORZA_PRUEBA] '1'"))
        conn.commit()
        print("   SP Ejecutado correctamente.")
except Exception as e:
    print(f"   Error ejecutando SP Final: {e}")

print("\n=== PROCESO FINALIZADO ===")