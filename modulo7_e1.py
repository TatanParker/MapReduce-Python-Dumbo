from dumbo import main, MultiMapper, primary, secondary, JoinReducer


def parse_country_medals_map(key, value):
    """
    Parse table - medal|prize|country|year
    """
    try:
        medal, prize, country, year = value.split('|')
        yield (country, int(year)), (medal, int(prize))
    except:
        pass


def parse_athletes_medals_map(key, value):
    """
    Parse table - Athlete|Age|Country|Year|Ceremony|Sport|Gold|Silver|Bronze|Total
    """
    try:
        athlete, age, country, year, ceremony, sport, gold, silver, bronze, total = value.split('|')
        yield (country, int(year)), (gold, silver, bronze, total)
    except:
        pass


class Join_athlete_country_medals_reduce(JoinReducer):
    def __init__(self):
        super(Join_athlete_country_medals_reduce, self).__init__()

    def primary(self, key, values):
        self.country_cache = {}
        for v in values:
            self.country_cache[(key[1], v[0])] = v[1]

    def secondary(self, key, values):
        total_price = 0
        total = 0
        for v in values:
            gold, silver, bronze, medals = v[:]
            total += int(medals)

            if gold > 0 and (key[1], 'Gold') in self.country_cache:
                total_price += int(self.country_cache[(key[1], 'Gold')]) * int(gold)

            if silver > 0 and (key[1], 'Silver') in self.country_cache:
                total_price += int(self.country_cache[(key[1], 'Silver')]) * int(silver)

            if bronze > 0 and (key[1], 'Bronze') in self.country_cache:
                total_price += int(self.country_cache[(key[1], 'Bronze')]) * int(bronze)

        # Emit values
        yield key, (total, total_price)

    def secondary_blocked(self, b):
        if self._key != b:
            self.country_cache = {}
        return False


def runner(job):
    inout_opts = [("inputformat", "text"), ("outputformat", "text")]
    multimap = MultiMapper()
    multimap.add("Country", primary(parse_country_medals_map))
    multimap.add("Athlete", secondary(parse_athletes_medals_map))
    o1 = job.additer(multimap, Join_athlete_country_medals_reduce, opts=inout_opts)


if __name__ == "__main__":
    main(runner)

dumbo start modulo7_e1.py \
-hadoop /usr/hdp/2.6.5.0-292/hadoop \
-hadooplib /usr/hdp/2.6.5.0-292/hadoop-mapreduce \
-python /usr/bin/python \
-input /master/m7/input/CountryMedals.txt \
-input /master/m7/input/OlympicAthletes.txt \
-output /master/m7/output/e1/ \
-overwrite yes