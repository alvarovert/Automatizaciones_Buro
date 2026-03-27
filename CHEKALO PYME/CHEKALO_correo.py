import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os

# ==================== CONFIGURACIÓN DE CREDENCIALES ====================
# Pon aquí tus credenciales
CORREO_REMITENTE = "amenachod@buro.com.pe"
CONTRASEÑA = "72383827"  # Reemplaza con tu contraseña o contraseña de aplicación

# Configuración del servidor SMTP
# Si usas Gmail, usa: smtp.gmail.com , puerto 587
# Si usas Outlook, usa: smtp-mail.outlook.com , puerto 587
# Si usas tu correo corporativo de Buro, consulta con tu proveedor IT
SERVIDOR_SMTP = "mail.buro.com.pe"  # Reemplaza con el servidor SMTP correcto
PUERTO_SMTP = 587

# ==================== CONTENIDO DEL CORREO ====================
# ASUNTO: Pon aquí el asunto del correo
ASUNTO = "Base de Filtro Chekalo"

# DESTINATARIOS (TO): Lista de emails que recibirán el correo
DESTINATARIOS = [
    "marceo@buro.com.pe",
    "dherrera@buro.com.pe",
    "gperez@buro.com.pe"
]

# COPIA (CC): Lista de emails en copia
COPIA = [
    "soporte.pyme@buro.com.pe",
    "vgaldos@buro.com.pe",
    "dhidalgoc@buro.com.pe"
    "soporte.ic@buro.com.pe"
]
# COPIA OCULTA (BCC) - Opcional: Los emails aquí no se verán en el correo
COPIA_OCULTA = [
    # "oculto@buro.com.pe"
]

# CUERPO DEL CORREO: Pon aquí el mensaje que quieres enviar
CUERPO_CORREO = """
Buenos días,
 
Se envía una base Buro de leads filtrados por el chekalo.

La base que se esta enviando esta formada por personas naturales.

Saludos cordiales.

Alvaro Menacho
IC Buro
"""

# Ruta del archivo adjunto
RUTA_ARCHIVO_ADJUNTO = r"C:\Users\Alvaro Menacho\Documents\PYME\CHEKALO\base_Chekalo_27_03_2026.xlsx"  # Reemplaza con la ruta correcta de tu archivo

# ==================== FUNCIÓN PARA ENVIAR CORREO ====================
def enviar_correo():
    """
    Envía un correo con el archivo adjunto
    """
    try:
        # Verificar que el archivo existe
        if not os.path.exists(RUTA_ARCHIVO_ADJUNTO):
            print(f"Error: El archivo {RUTA_ARCHIVO_ADJUNTO} no existe")
            return False
        
        # Crear mensaje
        mensaje = MIMEMultipart()
        mensaje['From'] = CORREO_REMITENTE
        mensaje['To'] = ", ".join(DESTINATARIOS)
        mensaje['Cc'] = ", ".join(COPIA)
        mensaje['Subject'] = ASUNTO
        
        # Agregar el cuerpo del correo
        mensaje.attach(MIMEText(CUERPO_CORREO, 'plain', 'utf-8'))
        
        # Adjuntar archivo
        print(f"Adjuntando archivo: {RUTA_ARCHIVO_ADJUNTO}")
        archivo = open(RUTA_ARCHIVO_ADJUNTO, 'rb')
        parte = MIMEBase('application', 'octet-stream')
        parte.set_payload(archivo.read())
        archivo.close()
        
        encoders.encode_base64(parte)
        nombre_archivo = os.path.basename(RUTA_ARCHIVO_ADJUNTO)
        parte.add_header('Content-Disposition', f'attachment; filename= {nombre_archivo}')
        mensaje.attach(parte)
        
        # Conectar al servidor SMTP y enviar
        print(f"Conectando a {SERVIDOR_SMTP}:{PUERTO_SMTP}...")
        servidor = smtplib.SMTP(SERVIDOR_SMTP, PUERTO_SMTP)
        servidor.starttls()
        
        print(f"Autenticando con {CORREO_REMITENTE}...")
        servidor.login(CORREO_REMITENTE, CONTRASEÑA)
        
        # Enviar a todos los destinatarios y copia
        destinatarios_totales = DESTINATARIOS + COPIA + COPIA_OCULTA
        
        print("Enviando correo...")
        servidor.sendmail(CORREO_REMITENTE, destinatarios_totales, mensaje.as_string())
        servidor.quit()
        
        print("✅ Correo enviado exitosamente!")
        print(f"   Asunto: {ASUNTO}")
        print(f"   Para: {', '.join(DESTINATARIOS)}")
        if COPIA:
            print(f"   CC: {', '.join(COPIA)}")
        print(f"   Archivo adjunto: {nombre_archivo}")
        
        return True
        
    except FileNotFoundError:
        print(f"Error: No se encontró el archivo {RUTA_ARCHIVO_ADJUNTO}")
        return False
    except smtplib.SMTPAuthenticationError:
        print("Error de autenticación: Verifica tu correo y contraseña")
        return False
    except smtplib.SMTPException as e:
        print(f"Error SMTP: {str(e)}")
        return False
    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        return False

# ==================== EJECUCIÓN ====================
if __name__ == "__main__":
    print("=" * 60)
    print("ENVIANDO CORREO CON BASE CHEKALO")
    print("=" * 60)
    enviar_correo()
