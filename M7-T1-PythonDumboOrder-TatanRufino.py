DUMBO ORDERS in LINUX CONSOLE

dumbo start t1.py \
  -hadoop /usr/hdp/2.6.5.0-292/hadoop \
  -hadooplib /usr/hdp/2.6.5.0-292/hadoop-mapreduce \
  -python /usr/bin/python \
  -input /master/m7/input/Contratos_por_municipio.csv \
  -output /master/m7/output/t1/ \
  -file ./data/Comunidades_y_provincias.csv \
  -overwrite yes \
  -param limpiardatos=si




















 












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