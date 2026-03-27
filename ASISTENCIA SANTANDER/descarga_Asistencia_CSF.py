import imaplib
import email
from email.header import decode_header
from email.mime.multipart import MIMEMultipart
import os
from datetime import datetime, timedelta
import base64
import sys
import pandas as pd
import openpyxl

# Configuración
OUTLOOK_EMAIL = "amenachod@buro.com.pe"
OUTLOOK_PASSWORD = "72383827"  # Reemplaza con tu contraseña
IMAP_SERVER = "mail.buro.com.pe"
IMAP_PORT = 993
DOWNLOAD_FOLDER = r"C:\Users\Alvaro Menacho\Documents\SANTANDER\ASISTENCIA"
KEYWORDS = ["Asistencia", "corte", "2026"]

def conectar_outlook():
    """Conecta al servidor IMAP de Outlook"""
    try:
        mail = imaplib.IMAP4_SSL(IMAP_SERVER, IMAP_PORT)
        mail.login(OUTLOOK_EMAIL, OUTLOOK_PASSWORD)
        print(f"✓ Conectado exitosamente a {OUTLOOK_EMAIL}")
        return mail
    except imaplib.IMAP4.error as e:
        print(f"✗ Error de autenticación: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error de conexión: {e}")
        sys.exit(1)

def buscar_correos_recientes(mail):
    """Busca correos en los últimos 2 días que contengan las palabras clave"""
    try:
        # Seleccionar bandeja de entrada
        mail.select("INBOX")
        
        # Calcular fechas (hoy y hace 2 días)
        hoy = datetime.now()
        hace_dos_dias = hoy - timedelta(days=2)
        fecha_inicio = hace_dos_dias.strftime("%d-%b-%Y")
        
        # Buscar correos por criterios: palabras en el asunto y fecha
        criterios = []
        for keyword in KEYWORDS:
            criterios.append(f'SUBJECT "{keyword}"')
        
        # Buscar correos que coincidan con TODOS los criterios y que estén en el rango de fechas
        status, almacenamiento = mail.search(None, f'SINCE {fecha_inicio}')
        
        if status != "OK":
            print("✗ Error al buscar correos")
            return None
        
        email_ids = almacenamiento[0].split()
        
        if not email_ids:
            print(f"✗ No se encontraron correos en los últimos 2 días")
            return None
        
        # Filtrar correos que contengan todas las palabras clave en el asunto
        correos_validos = []
        for email_id in email_ids:
            status, msg_data = mail.fetch(email_id, "(RFC822)")
            msg = email.message_from_bytes(msg_data[0][1])
            
            asunto = msg.get("Subject", "")
            
            # Decodificar el asunto si es necesario
            if isinstance(asunto, str):
                try:
                    decoded_parts = decode_header(asunto)
                    asunto = "".join([
                        part.decode(encoding or "utf-8") if isinstance(part, bytes) else part
                        for part, encoding in decoded_parts
                    ])
                except:
                    pass
            
            # Verificar que todas las palabras clave estén en el asunto
            if all(keyword.lower() in asunto.lower() for keyword in KEYWORDS):
                correos_validos.append({
                    "id": email_id,
                    "asunto": asunto,
                    "fecha": msg.get("Date", ""),
                    "mensaje": msg
                })
        
        if not correos_validos:
            print(f"✗ No se encontraron correos con las palabras clave: {', '.join(KEYWORDS)}")
            return None
        
        # Ordenar por fecha (más reciente primero)
        correos_validos.sort(key=lambda x: email.utils.parsedate_to_datetime(x["fecha"]), reverse=True)
        
        correo_mas_reciente = correos_validos[0]
        print(f"✓ Correo encontrado:")
        print(f"  Asunto: {correo_mas_reciente['asunto']}")
        print(f"  Fecha: {correo_mas_reciente['fecha']}")
        
        return correo_mas_reciente
    
    except Exception as e:
        print(f"✗ Error al buscar correos: {e}")
        return None

def convertir_excel_a_csv(ruta_excel):
    """
    Convierte a CSV la hoja de Excel siguiendo reglas de prioridad:
    1. Nombre con fecha actual (DD-MM)
    2. Hoja activa (la que abre por defecto)
    3. Última hoja (más a la derecha)
    """
    # Verificamos que sea un archivo Excel moderno (.xlsx)
    if not ruta_excel.lower().endswith('.xlsx'):
        print(f"  - No es un archivo .xlsx, se omite conversión: {os.path.basename(ruta_excel)}")
        return False
        
    try:
        print(f"  > Iniciando conversión de: {os.path.basename(ruta_excel)}")
        
        # Cargar el Excel para leer la estructura de hojas
        wb = openpyxl.load_workbook(ruta_excel, read_only=False, data_only=True)
        nombres_hojas = wb.sheetnames
        
        hoja_objetivo = None
        fecha_hoy = datetime.now().strftime("%d-%m") # Formato: 25-03
        
        # REGLA 1: Buscar por fecha
        for nombre in nombres_hojas:
            if fecha_hoy in nombre:
                hoja_objetivo = nombre
                print(f"  > Hoja seleccionada por coincidir con fecha de hoy: '{hoja_objetivo}'")
                break
                
        # REGLA 2: Buscar hoja activa (si no funcionó la regla 1)
        if not hoja_objetivo and wb.active:
            hoja_objetivo = wb.active.title
            print(f"  > Hoja seleccionada por ser la activa/abierta por defecto: '{hoja_objetivo}'")
            
        # REGLA 3: Última hoja (si fallaron las anteriores)
        if not hoja_objetivo:
            hoja_objetivo = nombres_hojas[-1]
            print(f"  > Hoja seleccionada por ser la última a la derecha: '{hoja_objetivo}'")

        # Leer la hoja específica con pandas
        df = pd.read_excel(ruta_excel, sheet_name=hoja_objetivo, engine='openpyxl')
        
        # Generar ruta para el nuevo CSV y guardar
        ruta_csv = ruta_excel.rsplit('.', 1)[0] + '.csv'
        
        # Guardamos en CSV (utf-8-sig es importante para no perder tildes ni eñes al abrir en Windows)
        df.to_csv(ruta_csv, index=False, encoding='utf-8-sig') 
        
        print(f"  ✓ Convertido exitosamente a: {os.path.basename(ruta_csv)}")
        return True
        
    except Exception as e:
        print(f"  ✗ Error al convertir Excel a CSV: {e}")
        return False

def descargar_adjuntos(correo):
    """Descarga los adjuntos del correo más reciente"""
    try:
        msg = correo["mensaje"]
        adjuntos_descargados = []
        
        # Crear carpeta si no existe
        if not os.path.exists(DOWNLOAD_FOLDER):
            os.makedirs(DOWNLOAD_FOLDER)
            print(f"✓ Carpeta creada: {DOWNLOAD_FOLDER}")
        
        # Iterar sobre las partes del mensaje
        for part in msg.walk():
            if part.get_content_disposition() == "attachment":
                filename = part.get_filename()
                
                if filename:
                    # Decodificar el nombre del archivo si es necesario
                    if isinstance(filename, tuple):
                        filename = filename[3] or filename[2]
                    
                    try:
                        decoded_parts = decode_header(filename)
                        filename = "".join([
                            part.decode(encoding or "utf-8") if isinstance(part, bytes) else part
                            for part, encoding in decoded_parts
                        ])
                    except:
                        pass
                    
                    ruta_completa = os.path.join(DOWNLOAD_FOLDER, filename)
                    
                    # Descargar el adjunto (reemplazar si existe)
                    with open(ruta_completa, "wb") as f:
                        f.write(part.get_payload(decode=True))
                    
                    print(f"✓ Descargado: {filename}")
                    adjuntos_descargados.append(ruta_completa)
                    # Llamamos a la función de conversión inmediatamente después de descargar
                    convertir_excel_a_csv(ruta_completa)
        
        if not adjuntos_descargados:
            print("✗ El correo no contiene adjuntos")
            return False
        
        return True
    
    except Exception as e:
        print(f"✗ Error al descargar adjuntos: {e}")
        return False

def main():
    print("=" * 60)
    print("Script de Descarga de Reporte de Asistencia - SANTANDER")
    print("=" * 60)
    
    # Conectar a Outlook
    mail = conectar_outlook()
    
    # Buscar correo más reciente
    correo = buscar_correos_recientes(mail)
    
    if not correo:
        print("\n✗ No se pudo encontrar el correo solicitado")
        mail.close()
        mail.logout()
        return False
    
    # Descargar adjuntos
    exito = descargar_adjuntos(correo)
    
    # Cerrar conexión
    mail.close()
    mail.logout()
    
    print("\n" + "=" * 60)
    if exito:
        print("✓ Proceso completado exitosamente")
    else:
        print("✗ Proceso completado con errores")
    print("=" * 60)
    
    return exito

if __name__ == "__main__":
    main()
