import requests

import pyodbc

def consumir_api_usuarios() -> dict:
    url = "https://api.na.teamtailor.com/v1/users"
    headers = {"Authorization": 'Token token=WiSQVJRl_HEkTgR-TadYJVbeaEKXviSgnCNluDbL', "X-Api-Version": '20240904'}
    try:
        response = requests.get(url, headers=headers, timeout=20)
        return response.json()
    except Exception as e:
        print(f"Error en API: {e}")
        return None


def conexion_db() -> pyodbc.Connection:
    conexion_db = (
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        rf"SERVER=172.16.0.46\INST_1;"
        rf"DATABASE=DB_PRUEBA_1;"
        rf"UID=sa;"
        rf"PWD=D@T@B@S32026;"
        "Connection Timeout=30;"
    )
    return pyodbc.connect(conexion_db)


def insertar_datos():
    datos = consumir_api_usuarios()
    sql_insertar_usuarios = """INSERT INTO """
    conexion = conexion_db()
    cursor = conexion.cursor()
    


def main():
    print(consumir_api_usuarios())


if __name__ == "__main__":
    main()