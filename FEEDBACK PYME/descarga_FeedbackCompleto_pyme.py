import imaplib
import email
from email.header import decode_header
from email.mime.multipart import MIMEMultipart
import os
from datetime import datetime, timedelta
import base64
import sys

# Configuración
OUTLOOK_EMAIL = "amenachod@buro.com.pe"
OUTLOOK_PASSWORD = "72383827"  # Reemplaza con tu contraseña
IMAP_SERVER = "mail.buro.com.pe"
IMAP_PORT = 993
DOWNLOAD_FOLDER = r"C:\Users\Alvaro Menacho\Documents\PYME\FEEDBACK_COMPLETO"
KEYWORDS = ["FEEDBACK", "feedback"]

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
        
        if not adjuntos_descargados:
            print("✗ El correo no contiene adjuntos")
            return False
        
        return True
    
    except Exception as e:
        print(f"✗ Error al descargar adjuntos: {e}")
        return False

def main():
    print("=" * 60)
    print("Script de Descarga de FEEDBACK PYME")
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
