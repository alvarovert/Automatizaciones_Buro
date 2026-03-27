import subprocess
import time
import sys

def ejecutar_proceso_Inforza():
    """
    Ejecuta el proceso completo de Inforza y muestra los errores si los hay.
    """
    try:
        print("Iniciando descarga del Archivo de Inforza...")
        
        # Ejecutar descarga (AÑADIMOS encoding="utf-8")
        resultado_descarga = subprocess.run(
            [sys.executable, "-X", "utf8", "descarga_reporte_inforza.py"], 
            capture_output=True, 
            text=True,
            encoding="utf-8",
            errors="replace"
        )
        
        # Validar si hubo un error en la descarga
        if resultado_descarga.returncode != 0:
            print("❌ ERROR: El script de descarga falló.")
            print(f"Detalle del error:\n{resultado_descarga.stderr}")
            return False
            
        print("✅ Ya se descargó el archivo de Inforza")
        print(f"Mensajes de descarga:\n{resultado_descarga.stdout}") # Opcional: ver los checks aquí
        
        print("Esperando 4 segundos...")
        time.sleep(4)
        
        print("Iniciando carga del Archivo a la Base de Datos...")
        
        # Ejecutar carga (AÑADIMOS "-X", "utf8")
        resultado_carga = subprocess.run(
            [sys.executable, "-X", "utf8", "carga_ReporteInforza.py"], 
            capture_output=True, 
            text=True,
            encoding="utf-8",
            errors="replace"
        )
        
        # Validar si hubo un error en la carga
        if resultado_carga.returncode != 0:
            print("❌ ERROR: El script de carga falló.")
            print(f"Detalle del error:\n{resultado_carga.stderr}")
            return False
            
        print("✅ Se subió el Archivo de hoy a la Base de Datos")
        print("🚀 Se completó el Proceso de Inforza, listo para enviar.")
        
        return True
        
    except Exception as e:
        print(f"❌ Error crítico durante el proceso principal: {str(e)}")
        return False

if __name__ == "__main__":
    ejecutar_proceso_Inforza()
    
    input("\nPresiona Enter para salir...")