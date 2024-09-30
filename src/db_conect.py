import pyodbc

# Conexión a SQL Server con autenticación de Windows
def connect_to_sql_server():
    try:
        conn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            r'SERVER=KARTA\MSSQL_SSIS;'  # Nombre del servidor y la instancia
            'DATABASE=FASE1;'  # Nombre de la base de datos
            'Trusted_Connection=yes;'  # Autenticación de Windows
        )
        cursor = conn.cursor()
        #print("Connection to SQL Server was successful.")
        return conn, cursor
    except pyodbc.Error as e:
        #print("Error in connection:", e)
        return None, None
    


for index, row in df.iterrows():
    # Obtener los ids de las dimensiones correspondientes
    cursor.execute("SELECT id_producto FROM dim_producto WHERE nombre_producto = ?", row['Producto'])
    id_producto = cursor.fetchone()[0]
    
    cursor.execute("SELECT id_pais FROM dim_pais WHERE nombre_pais = ?", row['País'])
    id_pais = cursor.fetchone()[0]
    
    cursor.execute("SELECT id_fecha FROM dim_tiempo WHERE fecha = ?", row['Fecha'])
    id_tiempo = cursor.fetchone()[0]
    
    if pd.isna(row['Discount Band']):
        id_discount_band = None
    else:
        cursor.execute("SELECT id_discount_band FROM dim_discount_band WHERE discount_band = ?", row['Discount Band'])
        id_discount_band = cursor.fetchone()[0]
    
    cursor.execute("SELECT id_segmento FROM dim_segmento WHERE segmento = ?", row['Segmento'])
    id_segmento = cursor.fetchone()[0]

    # Asegurar que los valores numéricos estén correctamente formateados como floats
    unidades_vendidas = float(row['Unidades vendidas'])
    ventas_brutas = float(row['Ventas brutas'])
    descuento = float(row['Descuento'])
    ventas_netas = float(row['Ventas'])  # Si este es el campo con error
    costos = float(row['Costos'])        # Si este es el campo con error
    beneficio = float(row['Beneficio'])

    # Imprimir valores para depurar si es necesario
    print(f"Fila {index}: Ventas netas = {ventas_netas}, Costos = {costos}, Tipo de ventas_netas: {type(ventas_netas)}, Tipo de costos: {type(costos)}")

    try:
        # Inserción en la tabla hecho_ventas
        cursor.execute(
            "INSERT INTO hecho_ventas (unidades_vendidas, ventas_brutas, descuento, ventas_netas, costos, beneficio, id_producto, id_pais, id_tiempo, id_discount_band, id_segmento) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            unidades_vendidas, ventas_brutas, descuento, ventas_netas, costos, beneficio, id_producto, id_pais, id_tiempo, id_discount_band, id_segmento
        )
    except Exception as e:
        # Manejo de errores
        print(f"Error en la fila {index} al insertar los datos: {e}")

# Confirmar cambios
conn.commit()
