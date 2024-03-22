"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""
import pandas as pd


def ingest_data():
    #df = pd.read_csv("clusters_report.txt", sep="\t", header=None, names=["Cluster", "Cantidad de palabras clave", "Porcentaje de palabras clave", "Principales palabras clave"])
    df = pd.read_fwf("clusters_report.txt", 
                     widths=[9, 16, 16, 77],
                     skiprows=lambda row_counter: row_counter < 4,
                     names = ["cluster", "cantidad_de_palabras_clave", "porcentaje_de_palabras_clave", "principales_palabras_clave"],
                    #  dtype={"cluster": int,
                    #         "cantidad_de_palabras_clave": int}, DA ERROR POR LOS NaN
                     converters={"porcentaje_de_palabras_clave": lambda x: x.strip(" %").replace(",", ".") if x is not None else x}
                     ).ffill() #Me llena los NaN con el ultimo valor anterior que no es NaN
    df = df.groupby(["cluster", "cantidad_de_palabras_clave", "porcentaje_de_palabras_clave"])["principales_palabras_clave"].agg(lambda x: ' '.join(x)).reset_index() #si no reseteo el indice, quedaria que por ejemplo el primer idnice seria:  1.0, 105.0, '15.9'
    df["principales_palabras_clave"] = df["principales_palabras_clave"].agg(lambda x: ' '.join(x.split()).replace(".", "")) #Al no especificar donde hago split, elimina cualquier cant. de espacios en blanco
    df["cantidad_de_palabras_clave"] = df["cantidad_de_palabras_clave"].astype(int)
    df["porcentaje_de_palabras_clave"] = df["porcentaje_de_palabras_clave"].astype(float)
    return df

# print(ingest_data())