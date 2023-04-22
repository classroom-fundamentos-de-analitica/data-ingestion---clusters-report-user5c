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

    #
    # Inserte su código aquí
    #
    f = open('clusters_report.txt', 'r')
    lines = f.readlines()
    f.close()

    list = []
    cols = {
        'col1': {
            'init': 0,
            'end': 9,
            'value': ''
        },
        'col2': {
            'init': 9,
            'end': 25,
            'value': ''
        },
        'col3': {
            'init': 25,
            'end': 41,
            'value': ''
        },
        'col4': {
            'init': 41,
            'end': None,
            'value': ''
        }
    }
    get_col_value = lambda line, cols, col_number: line[cols[col_number]['init']:cols[col_number]['end']]
    for line in lines:
        # Validar que aun no se ha terminado de leer una fila
        if '---------' not in line and " ".join(line.split()):
            cols['col1']['value'] += get_col_value(line, cols, 'col1')
            cols['col2']['value'] += get_col_value(line, cols, 'col2')
            cols['col3']['value'] += get_col_value(line, cols, 'col3')
            cols['col4']['value'] += get_col_value(line, cols, 'col4')

        else: # Si se termino de ller la fila entonces se agrega la nueva fila y se limpia los valores temporales en el diccionario
            if cols['col1']['value'] and cols['col2']['value'] and cols['col3']['value'] and cols['col4']['value']:
                list.append(
                    (
                        ' '.join(cols['col1']['value'].split()), # Eliminar espacios y saltos de linea
                        ' '.join(cols['col2']['value'].split()), # Eliminar espacios y saltos de linea
                        ' '.join(cols['col3']['value'].split()).replace('%', '').replace(',', '.'), # Eliminar espacios y saltos de linea
                        ' '.join(cols['col4']['value'].split()).strip('.'), # Eliminar espacios y saltos de linea
                    )
                )
                cols['col1']['value'] = ''
                cols['col2']['value'] = ''
                cols['col3']['value'] = ''
                cols['col4']['value'] = ''

    df = pd.DataFrame(list)
    # Set header like a top row
    headers = df.iloc[0].apply(lambda x: x.lower().replace(' ', '_'))
    df = df[1:]
    df.columns = headers

    # Change type of columns
    convert_dict = {
        'cluster': int,
        'cantidad_de_palabras_clave': float,
        'porcentaje_de_palabras_clave': float,
        'principales_palabras_clave': str
    }
    df = df.astype(convert_dict)

    return df
