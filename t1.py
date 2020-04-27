# -*- coding: utf-8 -*-

import csv
import sys

from dumbo import main, identitymapper, identityreducer

__author__ = "Tatan Rufino"
__doc__ = """
Este fichero de Python obtiene las comunidades autonomas que han hecho mas contratos a mujeres que a hombres.
Para ello se utilizara map&reduce donde el fichero de comunidades_autonomas-provincias se guarde en memoria
y el fichero con contratos es pasado al map.
"""


def load_provincias_comunidadesautonomas(country_files):
    """
    Metodo que trasforma las entradas en el fichero CSV a diccionario donde la clave es la provincia y
    el valor la comunidad autonoma.
    :param country_files: ruta y nombre del fichero donde estan las tuplas comunidad_autonoma;provincia en el
    que la primera linea es la cabecera.
    :return: diccionario cuyo clave es el nombre de la provincia y el valor es la comunidad autonoma
    """
    provincias_comunidadesautonomas = {}
    try:
        # Lee el fichero - comunidad_autonoma;provincia
        with open(country_files) as f:
            reader = csv.reader(f, delimiter=';', quotechar='"', doublequote=False)
            # Salta la cabecera
            reader.next()

            # Se le el fichero y se insertan en el diccionario
            for linea in reader:
                provincias_comunidadesautonomas[linea[1]] = linea[0]

    except:
        pass

    return provincias_comunidadesautonomas


class Parse_contratos_provincias_mapper:
    """
    Clase para hacer el map entre en el fichero de comunidades-provincia (en memoria) y el de contratos.
    Cada linea de entrada tiene el formato:
        codigo_mes, provincia, municipio, total_contratos, contratos_hombres, contratos_mujeres separados por ;
    Una vez leida la linea y separada en campos, se hace el match con el diccionario provincia-comunidad autonoma.
    Si esta provincia no tiene una comunidad en este diccionario, guardara una entrada en sys.stderr con el formato
    "Fila sin comunidad autonoma|key: + $key + values: + $value".
    Antes de terminar, se hace la conversion de numero de contratos hombres y mujeres a int. Si no es un numero
    se guardara una entrada en sys.stderr con el formato "Fila excepcion|key: + $key + values: + $value".
    Estas entradas en los logs se tienen que revisar.
    Se ha visto que las unicas lineas que no tienen ningun match con las comunidades autonomas son: la cabecera
    y µvila. Esta ultima se ve clarisimamente que es Avila y que ha habido algun problema en el formato de texto
    con la tilde. Ademas, es la unica provincia que acaba en 'vila', por lo que se ha decidio cambiar a A'vila'
    todo lo que esta en la columna provincia y que termine por 'vila'. Para hacerlo efectivo se le tiene que pasar
    a dumbo limpiardatos=si en el parametro -param. Despues de la sustitucion se hara el match.
    El fichero de las provincias y comunidades tiene que estar y ser nombrado como ./Comunidades_y_provincias.csv
    Este fichero al ser pequenyo se puede almacenar en memoria y hacer el match de una forma muy rapida
    (es por esta caracteristica por la que se ha decidido hacer de esta forma y no como las otras 2 vistas).
    """

    # Nombre del parametro para limpiar lod datos. Solo se ha visto el problema de Avila como se ha comentado
    NOMBRE_PARAMETRO_LIMPIAR_DATOS = 'limpiardatos'
    NOMBRE_FICHERO_COMUNIDADES_PROVINCIAS = './Comunidades_y_provincias.csv'

    def __init__(self):
        # Se lee el fichero comunidad_autonoma-provincia convirtiendolo en un diccionario
        self.provincias_comunidadesautonomas = load_provincias_comunidadesautonomas(
            Parse_contratos_provincias_mapper.NOMBRE_FICHERO_COMUNIDADES_PROVINCIAS)

    def __call__(self, key, value):
        """
        Llamada que hara el map.
        :param key: no importante ya que no existe ninguna key todavia.
        :param value: linea del fichero de contratos con cabecera con el formato:
            codigo_mes;provincia;municipio;total_contratos;contratos_hombres;contratos_mujeres
        :return: comunidad autonoma + lista de numero de contratos a mujeres y numero de contratos de hombres
            convertidos a int de esta entrada.
        """
        try:
            # Se divide la linea y se obtienen los campos
            codigo_mes, provincia, municipio, total_contratos, contratos_hombres, contratos_mujeres = value.split(';')

            # Si se ha seleccionado limpiar datos, se cambia toda provincia que termina en vila a Avila.
            if self.params[Parse_contratos_provincias_mapper.NOMBRE_PARAMETRO_LIMPIAR_DATOS] and self.params[
                Parse_contratos_provincias_mapper.NOMBRE_PARAMETRO_LIMPIAR_DATOS] == 'si':
                if provincia.endswith('vila'):
                    provincia = 'Avila'

            # Se obtiene la comunidad autonoma de esta provincia
            comunidadautonoma = self.provincias_comunidadesautonomas[provincia]

            # Si una un match provincia-comunidad_autonoma, se hace un yield con el key comunidad_autonoma y
            # value una lista con numero de contratos de mujeres y contratos de hombres en este orden.
            if comunidadautonoma:
                yield comunidadautonoma, (int(contratos_mujeres), int(contratos_hombres))
            else:
                # Se insertan en stderr para analizarlo posteriormente
                print >> sys.stderr, 'Fila sin comunidad autonoma|key:' + str(key) + '|values:' + str(value)
        except:
            # Error: se insertan en stderr para analizarlo posteriormente
            print >> sys.stderr, 'Fila excepcion|key:' + str(key) + '|values:' + str(value)


def join_contratos_hombres_mujeres_comunidad_autonoma(key, values):
    """
    Funcion que hara el reduce para obtener el numero de contratos de mujeres y hombres por comunidad autonoma.
    Solo se devolvera solo los que el numero de mujeres es mayor que el de hombres.
    :param key: nombre de la comunidad autonoma
    :param values: lista con listas donde cada elemento tiene el numero de contratos a mujeres y de hombres
    :return: comunidad autonnoma + lista con el total de contratos a mujeres y hombres (en este orden) en esta
        comunidad autonoma siempre y cuando el numero del de mujeres es mayor que el de hombres.
    """

    total_contratos_mujeres = 0
    total_contratos_hombres = 0
    

    # Se recorren la lista con los pares numero de contratos a mujeres, numero de contratos a hombres procedentes
    # del map
    for value in values:
        contratos_mujeres, contratos_hombres = value[:]

        # Se suman los actuales contratos a los que habia antes
        total_contratos_mujeres += int(contratos_mujeres)
        total_contratos_hombres += int(contratos_hombres)
    # Solo se devuelve si el numero de contratos a mujeres es mayor que el de hombres
    if total_contratos_hombres < total_contratos_mujeres:
        yield key, (total_contratos_mujeres, total_contratos_hombres)


def runner(job):
    # Se crea la llamada al map&reduce
    inout_opts = [("inputformat", "text"), ("outputformat", "text")]
    o1 = job.additer(Parse_contratos_provincias_mapper, join_contratos_hombres_mujeres_comunidad_autonoma,
                     opts=inout_opts)


if __name__ == "__main__":
    main(runner)


































 












# import csv

# from dumbo import main, MultiMapper, primary, secondary, JoinReducer

# def parse_regions(key, value):
#     """
#     Parse table - cities|regions
#     """
#     try:
#         Comunidad_Autonoma, Provincia = value.split(';')
#         yield (provincia), (region)
#     except:
#         pass

# def parse_contracts(key, value):
#     """
#     Parse table - Provincia | Contratos hombres | Contratos mujeres
#     """
#     try:
#         codigo_mes,provincia,municipio,total_contratos,contratos_hombres,contratos_mujeres = value.split(';')
#         yield ((provincia)), (int(contratos_hombres), int(contratos_mujeres))
#     except:
#         pass


# class Join_region_contracts_reduce(JoinReducer):
#     def __init__(self):
#         super(Join_region_contracts_reduce, self).__init__()

#     def primary(self, key, values):
#         self.regions_cache = {}
#         for v in values:
#             self.regions_cache[(key[0])] = v[0]

#     def secondary(self, key, values):
#         total_men_contracts = 0
#         total_women_contracts = 0
#         for v in values:
#             contratos_hombres, contratos_mujeres = v[:]

#             if contratos_hombres > 0 and (key[0]) in self.regions_cache:
#                 total_men_contracts += int(contratos_hombres)

#             if contratos_mujeres > 0 and (key[0]) in self.regions_cache:
#                 total_women_contracts += int(contratos_mujeres)

#         # Emit values
#         yield key, (total_men_contracts, total_women_contracts)

#     def secondary_blocked(self, b):
#         if self._key != b:
#             self.regions_cache = {}
#         return False

# def runner(job):
#     inout_opts = [("inputformat", "text"), ("outputformat", "text")]
#     multimap = MultiMapper()
#     multimap.add("region", primary(parse_regions))
#     multimap.add("Athlete", secondary(parse_contracts))
#     o1 = job.additer(multimap, Join_region_contracts_reduce, opts=inout_opts)


# if __name__ == "__main__":
#     main(runner)






# def load_regions(region_cities):
#     cities = {}
#     try:
#         # Read table - medal|prize|country|year
#         with open(region_cities) as f:
#             reader = csv.reader(f, delimiter=';', quotechar='"', doublequote=False)
#             reader.next()
#             for line in reader:
#                 cities[(line[1])] = line[0]

#     except:
#         pass

#     return cities