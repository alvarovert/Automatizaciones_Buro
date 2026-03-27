import pysftp
import os
from pathlib import Path
from datetime import datetime

# ============ CONFIGURACIÓN - EDITA AQUÍ CON TUS CREDENCIALES ============
SFTP_HOST = "secureftp.scotiabank.com.pe"  # Ej: sftp.empresa.com
SFTP_USER = "usftp310"             # Tu usuario SFTP
SFTP_PASSWORD = "Ly8tx61oan3J"      # Tu contraseña SFTP
SFTP_PORT = 22                        # Puerto SFTP (normalmente 22)

# Rutas locales
CARPETA_LOCAL = r"C:\Users\Alvaro Menacho\Documents\PYME\FEEDBACK\FEEDBACKS"
RUTA_SFTP = "/IN/FEEDBACK"           # Ruta donde se subirá el archivo en el SFTP

# =========================================================================

def obtener_archivo_excel(carpeta):
    """
    Busca y retorna el primer archivo Excel (.xlsx, .xls) en la carpeta.
    Si encuentra múltiples archivos, sube el más reciente.
    """
    archivos_excel = []
    
    for archivo in os.listdir(carpeta):
        if archivo.endswith(('.xlsx', '.xls')):
            ruta_completa = os.path.join(carpeta, archivo)
            archivos_excel.append((archivo, ruta_completa))
    
    if not archivos_excel:
        print(f"❌ No se encontraron archivos Excel en: {carpeta}")
        return None
    
    # Si hay múltiples, retorna el más reciente
    archivo_mas_reciente = max(archivos_excel, key=lambda x: os.path.getmtime(x[1]))
    print(f"📄 Archivo encontrado: {archivo_mas_reciente[0]}")
    return archivo_mas_reciente[1]

def subir_archivo_sftp(ruta_local, ruta_remota):
    """
    Conecta al SFTP y sube el archivo a la ruta especificada.
    """
    try:
        print(f"\n🔌 Conectando a SFTP: {SFTP_HOST}:{SFTP_PORT}...")
        
        # Configuración de conexión SFTP
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None  # Para evitar problemas con known_hosts
        
        with pysftp.Connection(
            host=SFTP_HOST,
            username=SFTP_USER,
            password=SFTP_PASSWORD,
            port=SFTP_PORT,
            cnopts=cnopts
        ) as sftp:
            print("✅ Conexión establecida\n")
            
            # Verifica que el directorio remoto existe
            try:
                sftp.cwd(ruta_remota)
                print(f"📁 Directorio remoto: {ruta_remota}")
            except IOError:
                print(f"⚠️  El directorio {ruta_remota} no existe. Creando...")
                sftp.makedirs(ruta_remota)
            
            # Sube el archivo
            nombre_archivo = os.path.basename(ruta_local)
            ruta_remota_completa = f"{ruta_remota}/{nombre_archivo}"
            
            print(f"📤 Subiendo: {nombre_archivo}...")
            sftp.put(ruta_local, ruta_remota_completa)
            
            print(f"✅ Archivo subido correctamente a: {ruta_remota_completa}\n")
            
            # Log de la acción
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"✔️  Operación completada: {timestamp}")
            
            return True
            
    except Exception as e:
        print(f"\n❌ Error en la conexión SFTP:")
        print(f"   {type(e).__name__}: {str(e)}\n")
        return False

def main():
    """
    Función principal que ejecuta el proceso.
    """
    print("=" * 60)
    print("  🚀 Script de Carga FEEDBACK a SFTP")
    print("=" * 60 + "\n")
    
    # Verificar que la carpeta local existe
    if not os.path.exists(CARPETA_LOCAL):
        print(f"❌ Error: La carpeta {CARPETA_LOCAL} no existe")
        return False
    
    # Obtener archivo Excel
    archivo_a_subir = obtener_archivo_excel(CARPETA_LOCAL)
    if not archivo_a_subir:
        return False
    
    # Subir archivo SFTP
    exito = subir_archivo_sftp(archivo_a_subir, RUTA_SFTP)
    
    if exito:
        print("=" * 60)
        print("  ✅ PROCESO COMPLETADO EXITOSAMENTE")
        print("=" * 60)
    else:
        print("=" * 60)
        print("  ❌ ERROR EN EL PROCESO")
        print("=" * 60)
    
    return exito

if __name__ == "__main__":
    main()
    # Descomenta la siguiente línea si ejecutas desde línea de comandos
    # input("\nPresiona Enter para salir...")
