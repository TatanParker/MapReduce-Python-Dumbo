import csv

from dumbo import main


def load_country_medals(country_files):
    countries = {}
    try:
        # Read table - medal|prize|country|year
        with open(country_files) as f:
            reader = csv.reader(f, delimiter='|', quotechar='"', doublequote=False)
            reader.next()
            for line in reader:
                countries[(line[2], line[3], line[0])] = line[1]

    except:
        pass

    return countries


def parse_athletes_medals_mapper(key, value):
    """
    Parse table - Athlete|Age|Country|Year|Ceremony|Sport|Gold|Silver|Bronze|Total
    """
    try:
        athlete, age, country, year, ceremony, sport, gold, silver, bronze, total = value.split('|')
        yield ((athlete, sport, country)), (year, gold, silver, bronze, int(total))
    except:
        pass


class Join_athlete_country_medals_reduce:
    def __init__(self):
        self.medal_price = load_country_medals('./CountryMedals.txt')

    def __call__(self, key, values):
        try:
            total_price = 0
            acc_total = 0
            athlete, sport, country = key[:]

            for v in values:
                year, gold, silver, bronze, total = v[:]
                acc_total += int(total)

                if gold > 0 and (country, year, 'Gold') in self.medal_price:
                    total_price += int(self.medal_price[(country, year, 'Gold')]) * int(gold)

                if silver > 0 and (country, year, 'Silver') in self.medal_price:
                    total_price += int(self.medal_price[(country, year, 'Silver')]) * int(silver)

                if bronze > 0 and (country, year, 'Bronze') in self.medal_price:
                    total_price += int(self.medal_price[(country, year, 'Bronze')]) * int(bronze)

            yield athlete, (country, sport, acc_total, total_price)

        except:
            pass


def runner(job):
    inout_opts = [("inputformat", "text"), ("outputformat", "text")]
    o1 = job.additer(parse_athletes_medals_mapper, Join_athlete_country_medals_reduce, opts=inout_opts)


if __name__ == "__main__":
    main(runner)
