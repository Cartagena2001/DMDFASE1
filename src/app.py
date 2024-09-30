import pandas as pd
from sqlalchemy import create_engine
import pyodbc

# Conexión a SQL Server con autenticación de Windows
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=KARTA\MSSQL_SSIS;'
    'DATABASE=FASE1;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

#Cargar la tabla de productos
df = pd.read_excel('E:\salva\Escritorio\datos_financieros.xlsx')
#print(df.head())

df['Ventas netas'] = df['Ventas brutas'] - df['Descuento']
df['Beneficio'] = df['Ventas netas'] - df['Costos']


# Insert into productos en dim_productos
df_producto = df[['Producto']].drop_duplicates()
for index, row in df_producto.iterrows():
    cursor.execute(
        "SELECT COUNT(*) FROM dim_producto WHERE nombre_producto = ?",
        row['Producto']
    )
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            "INSERT INTO dim_producto (nombre_producto) VALUES (?)",
            row['Producto']
        )
conn.commit()

# Insert into pais en dim_pais
df_pais = df[['País']].drop_duplicates()
for index, row in df_pais.iterrows():
    cursor.execute(
        "SELECT COUNT(*) FROM dim_pais WHERE nombre_pais = ?",
        row['País']
    )
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            "INSERT INTO dim_pais (nombre_pais) VALUES (?)",
            row['País']
        )
conn.commit()

# Insert into segmento en dim_segmento
df_segmento = df[['Segmento']].drop_duplicates()
for index, row in df_segmento.iterrows():
    cursor.execute(
        "SELECT COUNT(*) FROM dim_segmento WHERE segmento = ?",
        row['Segmento']
    )
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            "INSERT INTO dim_segmento (segmento) VALUES (?)",
            row['Segmento']
        )
conn.commit()

#insert into Discount band into dim_discount_band
df_discount_band = df[['Discount Band']].drop_duplicates()
for index, row in df_discount_band.iterrows():
    if pd.notna(row['Discount Band']):
        cursor.execute(
            "SELECT COUNT(*) FROM dim_discount_band WHERE discount_band = ?",
            row['Discount Band']
        )
        if cursor.fetchone()[0] == 0:
            cursor.execute(
                "INSERT INTO dim_discount_band (discount_band) VALUES (?)",
                row['Discount Band']
            )
conn.commit()

#insert into dim_tiempo
df['Año'] = pd.DatetimeIndex(df['Fecha']).year
df['Mes'] = pd.DatetimeIndex(df['Fecha']).month
df['Día'] = pd.DatetimeIndex(df['Fecha']).day

df_tiempo = df[['Fecha', 'Año', 'Mes', 'Nombre de mes']].drop_duplicates()
for index, row in df_tiempo.iterrows():
    cursor.execute(
        "SELECT COUNT(*) FROM dim_tiempo WHERE fecha = ?",
        row['Fecha']
    )
    if cursor.fetchone()[0] == 0:
        cursor.execute(
            "INSERT INTO dim_tiempo (fecha, anio, numero_mes, nombre_mes) VALUES (?, ?, ?, ?)",
            row['Fecha'], row['Año'], row['Mes'], row['Nombre de mes']
        )
conn.commit()

#insert into fact_precios que esta conformado por id_producto id_tiempo Precio manufactura y Precio de Venta
df_producto = df[['Producto']].drop_duplicates()
df_tiempo = df[['Fecha']].drop_duplicates()
for index, row in df.iterrows():
    cursor.execute(
        "SELECT id_producto FROM dim_producto WHERE nombre_producto = ?",
        row['Producto']
    )
    id_producto = cursor.fetchone()[0]
    cursor.execute(
        "SELECT id_fecha FROM dim_tiempo WHERE fecha = ?",
        row['Fecha']
    )
    id_tiempo = cursor.fetchone()[0]
    cursor.execute(
        "INSERT INTO fact_precios (id_producto, id_tiempo, precio_manofactura, precio_venta) VALUES (?, ?, ?, ?)",
        id_producto, id_tiempo, row['Precio manufactura'], row['Precio de venta']
    )
conn.commit()

#insert into hecho_ventas que tiene unidades_vendidas, ventas_brutas, descuento, ventas_netas, costos, beneficio, id_producto, id_pais, id_tiempo, id_discount_band, id_segmento
for index, row in df.iterrows():
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

    unidades_vendidas = float(row['Unidades vendidas'])
    ventas_brutas = float(row['Ventas brutas'])
    descuento = float(row['Descuento'])
    ventas_netas = float(row['Ventas'])
    costos = float(row['Costos'])
    beneficio = float(row['Beneficio'])

    #print(f"Fila {index}: Ventas netas = {ventas_netas}, Costos = {costos}, Tipo de ventas_netas: {type(ventas_netas)}, Tipo de costos: {type(costos)}")

    try:
        cursor.execute(
            "INSERT INTO hecho_ventas (unidades_vendidas, ventas_brutas, descuento, ventas_netas, costos, beneficio, id_producto, id_pais, id_tiempo, id_discount_band, id_segmento) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            unidades_vendidas, ventas_brutas, descuento, ventas_netas, costos, beneficio, id_producto, id_pais, id_tiempo, id_discount_band, id_segmento
        )
    except Exception as e:
        print(f"Error en la fila {index} al insertar los datos: {e}")

conn.commit()