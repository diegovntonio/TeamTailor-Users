# Flujo
# SQL Server -> pyodbc -> pandas DataFrame -> openpyxl -> archivo.xlsx
"""
import pyodbc

from dotenv import load_dotenv
import os

import pandas as pd

# CONSULTA_SQL = SELECT *
# FROM [DB_PRUEBA_1].[dbo].[USERS]
# WHERE CAST(search_timestamp AS DATE) = CAST(GETDATE() AS DATE);
# NOMBRE_ARCHIVO = "reporte_usuarios_4.xlsx" # TT (feccha)


def cargar_variables_entorno() -> dict[str, str]:
    load_dotenv()
    variables_de_entorno = {}
    variables = [
        "CONTRASENNA_HOST_MAIL",
        "SERVER_DB",
        "NOMBRE_DB",
        "USUARIO_DB",
        "CONTRASENNA_DB",
    ]
    for var in variables:
        valor = os.getenv(var)
        if not valor:
            raise RuntimeError(f"Falta variable de entorno: {var}")
        variables_de_entorno[var] = valor

    return variables_de_entorno

def conexion_db(
    server: str, nombre: str, usuario: str, contrasenna: str
) -> pyodbc.Connection:
    config_db = (
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        rf"SERVER={server};"
        rf"DATABASE={nombre};"
        rf"UID={usuario};"
        rf"PWD={contrasenna};"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(config_db)


def consulta_a_df(consulta: str, conexion_db: pyodbc.Connection) -> pd.DataFrame:
    try:
        df = pd.read_sql(consulta, conexion_db)
        return df
    except Exception as e:
        print(f"Error en consulta_a_df() ; Error: {e} ; Nombre error: {type(e).__name__}")
        return pd.DataFrame()


def exportar_excel(df: pd.DataFrame, nombre_archivo: str) -> bool:
    try:
        df.to_excel(nombre_archivo, index=False)
        return True
    except Exception as e:
        print(f"Error en exportar_excel() ; Error: {e} ; Nombre error: {type(e).__name__}")


def main():
    # 1. Cargar variables de entorno ; Recibe: ; Devuelve: variables de entorno
    variables_de_entorno = cargar_variables_entorno()

    # 2. Generar la conexion con la db ; Recibe: Datos de la configuracion ; Devuelve: Conexion
    conexion_database = conexion_db(
        variables_de_entorno["SERVER_DB"],
        variables_de_entorno["NOMBRE_DB"],
        variables_de_entorno["USUARIO_DB"],
        variables_de_entorno["CONTRASENNA_DB"],
    )

    # 3. Hacer consulta SQL y pasar a un DataFrame ; Recibe: consulta_sql, conexion a la db ; Devuelve: DataFrame de la consulta
    df = consulta_a_df(CONSULTA_SQL, conexion_database)

    # 4. Pasar DataFrame a excel y exportar ; Recibe: DataFrame, nombre del archivo ; Devuelve: Estado exportacion
    se_exporta_excel = exportar_excel(df, NOMBRE_ARCHIVO)
    print(f"Se exporta a excel?: {se_exporta_excel}")

    conexion_database.close()


if __name__ == "__main__":
    main()
"""