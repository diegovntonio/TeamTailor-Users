import requests

import pyodbc

from dotenv import load_dotenv
import os

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from datetime import datetime

import pandas as pd

fecha_hoy = datetime.now().replace(microsecond=0)

CONSULTA_SQL = """SELECT *
FROM [DB_PRUEBA_1].[dbo].[USERS]
WHERE CAST(search_timestamp AS DATE) = CAST(GETDATE() AS DATE);"""
NOMBRE_ARCHIVO = f"reporte_usuarios_team_tailor_{fecha_hoy.date()}.xlsx"

def cargar_variables_entorno():
    """
    Esta funcion carga el archivo donde se encuentran las variables de entorno,
    luego las agrega con su respectivo valor a un diccionario y finalmente
    retorna el diccionario
    """
    load_dotenv()
    variables = [
        "CONTRASENNA_HOST_MAIL",
        "SERVER_DB",
        "NOMBRE_DB",
        "USUARIO_DB",
        "CONTRASENNA_DB",
        "TOKEN",
        "API_VERSION",
        "REMITENTE",
        "DESTINATARIO",
        "DESTINATARIO_CC",
        "SMTP_HOST",
        "SMTP_PUERTO",
    ]
    variables_de_entorno = {}
    for var in variables:
        valor = os.getenv(var)
        if not valor:
            raise RuntimeError(f"Falta variable de entorno: {var}")
        variables_de_entorno[var] = valor

    return variables_de_entorno


def consumir_api_usuarios(url: str, token: str, api_version: str) -> dict:
    headers = {"Authorization": f"Token token={token}", "X-Api-Version": api_version}
    try:
        response = requests.get(url, headers=headers, timeout=20)
        return response.json()
    except Exception as e:
        print(f"Error en API: {e}")
        return None


def conexion_db(
    servidor_db: str, nombre_db: str, user_db: str, contrasenna_db: str
) -> pyodbc.Connection:
    conexion_db = (
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        rf"SERVER={servidor_db};"
        rf"DATABASE={nombre_db};"
        rf"UID={user_db};"
        rf"PWD={contrasenna_db};"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conexion_db)


def insertar_datos(conexion_db):
    variables_de_entorno = cargar_variables_entorno()
    conexion = None
    cursor = None
    filas_insertadas = 0
    status = None
    issue = "Sin problemas"
    url = "https://api.na.teamtailor.com/v1/users?page[size]=30"
    sql_insertar_usuarios = """INSERT INTO USERS (
    id_user,
    description,
    email,
    external_id,
    facebook_profile,
    google_profile,
    hide_email,
    hide_phone,
    instagram_profile,
    linkedin_profile,
    locale,
    login_email,
    is_merged,
    name,
    other_profile,
    phone,
    picture,
    role,
    role_addons,
    title,
    time_format,
    time_zone,
    twitter_profile,
    username,
    is_visible,
    signature,
    start_of_week_day,
    search_timestamp
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    try:
        conexion = conexion_db
        cursor = conexion.cursor()
        while url:
            data_json = consumir_api_usuarios(
                url, variables_de_entorno["TOKEN"], variables_de_entorno["API_VERSION"]
            )
            data = data_json.get("data", [])
            for registro in data:
                try:
                    if registro.get("attributes") is None:
                        continue
                    int_id = registro["id"]
                    list_role_addons = (
                        ",".join(registro["attributes"]["role-addons"])
                        if registro["attributes"]["role-addons"]
                        else ""
                    )
                    cursor.execute(
                        sql_insertar_usuarios,
                        (
                            int_id,
                            registro["attributes"]["description"],
                            registro["attributes"]["email"],
                            registro["attributes"]["external-id"],
                            registro["attributes"]["facebook-profile"],
                            "Google Profile (No en api)",  # AQUI VA GOOGLE_PROFILE PERO NO SALE EN LA API
                            registro["attributes"]["hide-email"],
                            registro["attributes"][
                                "hide-phone"
                            ],  # ESTE CAMPO NO SALIA EN LA WEB DE TT
                            registro["attributes"]["instagram-profile"],
                            registro["attributes"]["linkedin-profile"],
                            registro["attributes"]["locale"],
                            registro["attributes"]["login-email"],
                            0000000,  # AQUI VA IS_MERGED PERO NO ESTA EN LA API
                            registro["attributes"]["name"],
                            registro["attributes"]["other-profile"],
                            registro["attributes"]["phone"],
                            0000000,  # ESTE CAMPOS NO SALIA EN LA WEB DE TT
                            registro["attributes"]["role"],
                            list_role_addons,
                            registro["attributes"]["title"],
                            registro["attributes"]["time-format"],
                            registro["attributes"]["time-zone"],
                            registro["attributes"]["twitter-profile"],
                            registro["attributes"]["username"],
                            registro["attributes"]["visible"],
                            registro["attributes"]["signature"],
                            registro["attributes"][
                                "start-of-week-day"
                            ],  # ESTE CAMPO NO SALIA EN LA WEB DE TT
                            fecha_hoy,
                        ),
                    )
                    filas_insertadas += 1
                except pyodbc.IntegrityError:
                    print("Error en insertar datos ; Error: IntegrityError.")
                except Exception as e:
                    print(f"Error al insertar 1 dato ; Error: {e}")
            conexion.commit()
            url = data_json.get("links", {}).get("next")
        status = "Exitoso" if filas_insertadas > 0 else "Erroneo"
        resumen = {
            "status": status,
            "filas_insertadas": filas_insertadas,
            "issue": issue,
        }
        return resumen
    except Exception as e:
        print(f"Error en INSERTAR_DATOS(): {e}")
        return None
    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception as e:
            print(f"Error al intentar cerrar el cursor ; Error: {e}")


def enviar_correo(asunto, mensaje):
    variables_de_entorno = cargar_variables_entorno()
    remitente = variables_de_entorno["REMITENTE"]
    destinatario = variables_de_entorno["DESTINATARIO"]
    destinatario_cc = variables_de_entorno["DESTINATARIO_CC"]
    smtp_host = variables_de_entorno["SMTP_HOST"]
    smtp_puerto =variables_de_entorno["SMTP_PUERTO"]
    user_contrasenna = variables_de_entorno["CONTRASENNA_HOST_MAIL"]
    server = None

    try:
        msg = MIMEMultipart()

        msg["From"] = remitente
        msg["To"] = destinatario
        msg["Cc"] = destinatario_cc
        msg["Subject"] = asunto

        msg.attach(MIMEText(mensaje, "plain"))

        server = smtplib.SMTP(smtp_host, smtp_puerto)
        server.starttls()

        server.login(remitente, user_contrasenna)

        destinatarios = [destinatario, destinatario_cc]
        server.sendmail(remitente, destinatarios, msg.as_string())

        return True
    except Exception as e:
        print(f"Error en enviar_correo() ; Tipo error: {type(e).__name__} ; Error: {e}")
        return False
    finally:
        if server is not None:
            server.quit()


def exportar_excel(conexion_db: pyodbc.Connection) -> bool:
    try:
        df = pd.read_sql(CONSULTA_SQL, conexion_db)
        df.to_excel(NOMBRE_ARCHIVO, index=False)
        return True
    except Exception as e:
        print(f"Error en exportar_excel() ; Error: {e} ; Nombre error: {type(e).__name__}")
        return False
    finally:
        try:
            if conexion_db is not None:
                conexion_db.close()
        except Exception as e:
            print(f"Error al intentar cerrar la conexion ; Error: {e}")


def main():
    # 1. Cargar variables de entorno (devuelve un diccionario con todas las variables de entorno)
    variables_de_entorno = cargar_variables_entorno()

    # 3. Generar conexion con la base de datos (recibe el servidor, el nombre de la DB, el usuario y
    # la contraseña desde las variables de entorno y retorna la conexion con las base de datos)
    conexion_database = conexion_db(
        variables_de_entorno["SERVER_DB"],
        variables_de_entorno["NOMBRE_DB"],
        variables_de_entorno["USUARIO_DB"],
        variables_de_entorno["CONTRASENNA_DB"],
    )

    # 4. Insertar datos (recibe la api y la conexion de la db y retorna un resumen indicando usuarios
    # insertados, los duplicados y si hay errores)
    resumen_datos_insertados = insertar_datos(conexion_database)

    # 5. Informar el estado de la insertacion de los datos via correo (recibe el resumen de la insertacion de
    # los datos y retorna si el correo fue enviado)
    asunto = "Usuarios Team Tailor"
    mensaje = f"""Fecha de ejecucion: {fecha_hoy}
Status: {resumen_datos_insertados["status"]}
Registros: {resumen_datos_insertados["filas_insertadas"]}
Issue: {resumen_datos_insertados["issue"]}"""
    
    se_envio_correo = enviar_correo(asunto, mensaje)
    print(f"Se envio correo?: {se_envio_correo}")

    se_exporto_excel = exportar_excel(conexion_database)
    print(f"Se exporto a excel?: {se_exporto_excel}")


    ###################################################################
    # print(consumir_api_usuarios("https://api.na.teamtailor.com/v1/users?page[size]=30", variables_de_entorno["TOKEN"], variables_de_entorno["API_VERSION"]))
    # print(f"Filas insertadas: {resumen_insertar_datos['filas_insertadas']}")


if __name__ == "__main__":
    main()