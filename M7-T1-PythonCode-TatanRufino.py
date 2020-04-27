
import csv
import sys

from dumbo import main, identitymapper, identityreducer

__author__ = "Tatan Rufino"

###Utilizaremos un sistema MapReduce al estilo del Map19 visto en el primer manual del Módulo 7,
###Dado que el archivo es pequeño y el trabajo de reduce es un trabajo de sustitución de variables, 
###Es probablemente la forma más eficiente de hacerlo, dejando la tarea de "join" para el reduce. 

def load_provincias_comunidadesautonomas(country_files):

    provincias_comunidadesautonomas = {}
    try:

        with open(country_files) as f:
            reader = csv.reader(f, delimiter=';', quotechar='"', doublequote=False)

            reader.next()


            for linea in reader:
                provincias_comunidadesautonomas[linea[1]] = linea[0]

    except:
        pass

    return provincias_comunidadesautonomas


class Parse_contratos_provincias_mapper:

    NOMBRE_PARAMETRO_LIMPIAR_DATOS = 'limpiardatos'
    NOMBRE_FICHERO_COMUNIDADES_PROVINCIAS = './Comunidades_y_provincias.csv'

    def __init__(self):

        self.provincias_comunidadesautonomas = load_provincias_comunidadesautonomas(
            Parse_contratos_provincias_mapper.NOMBRE_FICHERO_COMUNIDADES_PROVINCIAS)

    def __call__(self, key, value):

        try:

            codigo_mes, provincia, municipio, total_contratos, contratos_hombres, contratos_mujeres = value.split(';')

            # En caso de escoger limpiar los datos de Ávila, se procede

            if self.params[Parse_contratos_provincias_mapper.NOMBRE_PARAMETRO_LIMPIAR_DATOS] and self.params[
                Parse_contratos_provincias_mapper.NOMBRE_PARAMETRO_LIMPIAR_DATOS] == 'si':
                if provincia.endswith('vila'):
                    provincia = 'Avila'


            comunidadautonoma = self.provincias_comunidadesautonomas[provincia]

            # Se añade en el generador las lineas mapeadas

            if comunidadautonoma:
                yield comunidadautonoma, (int(contratos_mujeres), int(contratos_hombres))
            else:

                print >> sys.stderr, 'Sin comunidad autonoma|key:' + str(key) + '|values:' + str(value)
        except:
            # Error: se insertan en stderr para analizarlo posteriormente
            print >> sys.stderr, 'Fila excepcion|key:' + str(key) + '|values:' + str(value)


def join_contratos_hombres_mujeres_comunidad_autonoma(key, values):

    total_contratos_mujeres = 0
    total_contratos_hombres = 0
    
    for value in values:
        contratos_mujeres, contratos_hombres = value[:]


        total_contratos_mujeres += int(contratos_mujeres)
        total_contratos_hombres += int(contratos_hombres)

    if total_contratos_hombres < total_contratos_mujeres:
        yield key, (total_contratos_mujeres, total_contratos_hombres)


def runner(job):

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