import pandas as pd
from pathlib import Path
import re
from sqlalchemy import create_engine
import urllib.parse

class ETLFeedbackPYME:
    
    MAPEO_COLUMNAS = {
        'GESTIONAD': 'GEST',
        'PILOTO INCREMENTAL': 'PilotoIncremental',
        'MEJORES PERFILES CREDITICIOS': 'MejoresPerfilesCrediticios',
        'COMPRA DEUDA': 'CompraDeuda',
        'LENDING SECURED SBP': 'LendingSecuredSBP',
        'INT': 'Int'
    }
    
    def __init__(self, folder_path, server, database, username, password):
        self.folder_path = Path(folder_path)
        self.server = server
        self.database = database
        self.username = username
        self.password = password
    
    def obtener_archivo_mas_reciente(self):
        archivos = list(self.folder_path.glob("*.[xX][lL][sS]*"))
        if not archivos:
            raise FileNotFoundError(f"No se encontraron archivos en {self.folder_path}")
        
        archivo_reciente = max(archivos, key=lambda x: x.stat().st_mtime)
        print(f"📄 Archivo más reciente: {archivo_reciente.name}")
        return archivo_reciente
    
    def extraer_periodos(self, nombre_archivo):
        coincidencia = re.search(r'FEEDBACK\s+AL\s+(\d{2})[-.](\d{2})[-.](\d{2})', nombre_archivo, re.IGNORECASE)
        if not coincidencia:
            raise ValueError(f"No se pudo extraer la fecha de: {nombre_archivo}")
        
        dia, mes, anno = coincidencia.groups()
        return f"{dia}{mes}20{anno}", f"20{anno}{mes}"
    
    def procesar_datos(self, ruta_archivo, fecha_feedback, periodo_feedback):
        print("\n⏳ Leyendo y procesando datos...")
        
        # Leer Excel forzando Documento a texto preventivamente
        df = pd.read_excel(ruta_archivo, dtype={'Documento': str})
        df = df.rename(columns=self.MAPEO_COLUMNAS)
        
        # --- SOLUCIÓN PARA FECHAS (La única columna Datetime) ---
        if 'FechaDeContacto' in df.columns:
            # 1. Intentar capturar números seriales de Excel (ej: 46097)
            num_dates = pd.to_numeric(df['FechaDeContacto'], errors='coerce')
            fechas_excel = pd.to_datetime(num_dates, origin='1899-12-30', unit='D', errors='coerce')
            
            # 2. Intentar capturar textos de fecha normales (ej: '2026-03-20')
            fechas_str = pd.to_datetime(df['FechaDeContacto'], errors='coerce')
            
            # 3. Combinar ambos intentos y asignar como datetime64 puro
            df['FechaDeContacto'] = fechas_excel.fillna(fechas_str)

        # --- SOLUCIÓN GENERAL (Para las columnas nvarchar sin decimales) ---
        # Aislar todas las columnas excepto la fecha
        cols_texto = [c for c in df.columns if c != 'FechaDeContacto']
        
        for col in cols_texto:
            # 1. Convertir TODA la columna a string explícitamente (Evita el LossySetitemError)
            df[col] = df[col].astype(str)
            
            # 2. Eliminar el ".0" final de los números (ej: '80000.0' -> '80000')
            df[col] = df[col].str.replace(r'\.0$', '', regex=True)
            
            # 3. Limpiar espacios laterales
            df[col] = df[col].str.strip()
            
            # 4. Convertir la basura textual generada por Pandas en verdaderos nulos (NULL en SQL)
            df[col] = df[col].replace({'nan': None, 'None': None, '<NA>': None, '': None, 'NaT': None})

        # --- CASO ESPECIAL: DOCUMENTO ---
        if 'Documento' in df.columns:
            # Rellenar con ceros a la izquierda hasta llegar a 8 dígitos, ignorando nulos
            df['Documento'] = df['Documento'].apply(lambda x: str(x).zfill(8) if x is not None else None)

        # --- Agregar columnas estáticas ---
        df['FECHAFEEDBACK'] = fecha_feedback
        df['PERIODO_FEEDBACK'] = periodo_feedback
        df['Sede'] = 'BANCO'
        
        return df
    
    def insertar_en_sql_server(self, df, tabla_destino='HISTORICO_FEEDBACK_PYME'):
        print("\n🚀 Iniciando carga a SQL Server...")
        password_escaped = urllib.parse.quote_plus(self.password)
        
        conexion_string = (
            f"mssql+pyodbc://{self.username}:{password_escaped}@{self.server}/{self.database}"
            "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
        )
        
        engine = create_engine(conexion_string, fast_executemany=True)
        
        try:
            df.to_sql(name=tabla_destino, con=engine, if_exists='append', index=False, chunksize=1000)
            print(f"✅ {len(df)} filas insertadas exitosamente.")
        except Exception as e:
            raise Exception(f"Error durante la inserción en BD: {str(e)}")
        finally:
            engine.dispose()

    def ejecutar_etl(self):
        print("\n" + "="*70)
        print("INICIANDO PROCESO ETL - FEEDBACK PYME")
        print("="*70 + "\n")
        
        try:
            archivo = self.obtener_archivo_mas_reciente()
            fecha_feedback, periodo_feedback = self.extraer_periodos(archivo.stem)
            
            df_procesado = self.procesar_datos(archivo, fecha_feedback, periodo_feedback)
            self.insertar_en_sql_server(df_procesado)
            
            print("\n" + "="*70)
            print("🟢 PROCESO ETL COMPLETADO EXITOSAMENTE")
            print("="*70 + "\n")
            
        except Exception as e:
            print(f"\n🔴 ERROR EN EL PROCESO ETL: {str(e)}")
            raise

# ==================== CONFIGURACIÓN ====================
if __name__ == "__main__":
    
    CARPETA_FEEDBACK = r'C:\Users\Alvaro Menacho\Documents\PYME\FEEDBACK_COMPLETO'
    SERVIDOR_SQL = r'192.168.7.4\buro'
    BASE_DATOS = 'Buro_CRM'   
    USUARIO_SQL = 'vgaldos'   
    CONTRASENA_SQL = 'Alianza26'  
    
    etl = ETLFeedbackPYME(
        folder_path=CARPETA_FEEDBACK,
        server=SERVIDOR_SQL,
        database=BASE_DATOS,
        username=USUARIO_SQL,
        password=CONTRASENA_SQL
    )
    
    etl.ejecutar_etl()