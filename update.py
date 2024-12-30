import mysql.connector
from mysql.connector import Error

def update_multiple_records(connection, query, data_list):
    try:
        cursor = connection.cursor()
        cursor.executemany(query, data_list)
        connection.commit()
        print(f"Total {cursor.rowcount} record(s) updated")
    except Error as e:
        print(f"Error: {e}")
        connection.rollback()  # Deshace todas las actualizaciones si hay un error
    finally:
        cursor.close()

def main():
    try:
        # Configura la conexión a la base de datos
        connection = mysql.connector.connect(
            host='tu_host',         # Cambia 'tu_host' por el host de tu base de datos MySQL
            user='tu_usuario',      # Cambia 'tu_usuario' por tu nombre de usuario de MySQL
            password='tu_contraseña', # Cambia 'tu_contraseña' por tu contraseña de MySQL
            database='tu_base_datos' # Cambia 'tu_base_datos' por el nombre de tu base de datos
        )

        if connection.is_connected():
            print("Conexión exitosa a la base de datos")

            # Define la consulta de actualización
            update_query = """
            UPDATE nombre_tabla
            SET columna_a_actualizar = %s
            WHERE columna_condicion = %s
            """

            # Define los datos para las múltiples actualizaciones
            data_list = [
                ('valor_nuevo1', 'condicion1'),
                ('valor_nuevo2', 'condicion2'),
                ('valor_nuevo3', 'condicion3'),
                # Añade tantas tuplas como actualizaciones necesites
            ]

            # Ejecuta las actualizaciones
            update_multiple_records(connection, update_query, data_list)

    except Error as e:
        print(f"Error de conexión: {e}")

    finally:
        if connection.is_connected():
            connection.close()
            print("Conexión cerrada")

if __name__ == "__main__":
    main()