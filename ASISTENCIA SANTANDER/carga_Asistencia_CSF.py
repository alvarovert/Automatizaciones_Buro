import pandas as pd
import os
import urllib.parse
from sqlalchemy import create_engine, text

# ================= CONFIGURACIÓN =================
SERVER = r'xxxxx.x6x8.xxx.4\xxxx'
DATABASE = 'xxx'
USER = 'xxx' 
PASSWORD = 'xx'
params = urllib.parse.quote_plus(
    f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};UID={USER};PWD={PASSWORD};TrustServerCertificate=yes"
)
# fast_executemany=True es VITAL para rendimiento
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

# Convertir formato de fecha de DD/MM/YYYY a DD-MM-YYYY
def convert_fecha_format(df):
    df['FECHA'] = pd.to_datetime(df['FECHA']).dt.strftime('%d-%m-%Y')
    return df

# Leer archivo CSV
def load_csv(file_path):
    df = pd.read_csv(file_path, sep=';')
    df = convert_fecha_format(df)
    return df

# Eliminar datos con la misma fecha
def delete_by_fecha(engine, fecha, table_name):
    with engine.begin() as connection:
        query = text(f"DELETE FROM {table_name} WHERE FECHA = '{fecha}'")
        connection.execute(query)
    print(f"Registros eliminados para la fecha {fecha}")

# Subir datos a SQL Server
def upload_to_sql(df, engine, table_name):
    # Obtener la fecha del dataframe
    fecha = df['FECHA'].iloc[0]
    
    # Eliminar registros existentes con esa fecha
    delete_by_fecha(engine, fecha, table_name)
    
    # Subir el dataframe a la tabla
    df.to_sql(table_name, engine, if_exists='append', index=False)
    print(f"Datos subidos exitosamente a la tabla {table_name} para la fecha {fecha}")

if __name__ == "__main__":
    folder_path = os.path.dirname(os.path.abspath(__file__))
    
    # Procesar cada archivo CSV individualmente
    csv_files = [f for f in os.listdir(folder_path) if f.endswith('.csv')]
    
    for file in csv_files:
        file_path = os.path.join(folder_path, file)
        print(f"Procesando archivo: {file}")
        
        # Cargar datos del CSV
        df = load_csv(file_path)
        
        # Subir a SQL Server
        upload_to_sql(df, engine, 'CSF_ASISTENCIA')
        
        print(f"Archivo {file} procesado correctamente\n")
