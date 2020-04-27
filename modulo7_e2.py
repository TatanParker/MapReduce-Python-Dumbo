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


class Parse_athletes_medals_mapper:
    def __init__(self):
        self.medal_price = load_country_medals('./CountryMedals.txt')

    def __call__(self, key, value):
        try:
            total_price = 0
            athlete, age, country, year, ceremony, sport, gold, silver, bronze, total = value.split('|')
            int(total)

            if gold > 0 and (country, year, 'Gold') in self.medal_price:
                total_price += int(self.medal_price[(country, year, 'Gold')]) * int(gold)

            if silver > 0 and (country, year, 'Silver') in self.medal_price:
                total_price += int(self.medal_price[(country, year, 'Silver')]) * int(silver)

            if bronze > 0 and (country, year, 'Bronze') in self.medal_price:
                total_price += int(self.medal_price[(country, year, 'Bronze')]) * int(bronze)

            yield (athlete, sport, country), (total, total_price)

        except:
            pass


def join_athlete_country_medals_reduce(key, values):
    acc_price = 0
    acc_total = 0

    athlete, sport, country = key[:]

    for v in values:
        total, total_price = v[:]
        acc_price += int(total_price)
        acc_total += int(total)

    yield athlete, (country, sport, acc_total, acc_price)


def runner(job):
    inout_opts = [("inputformat", "text"), ("outputformat", "text")]
    o1 = job.additer(Parse_athletes_medals_mapper, join_athlete_country_medals_reduce, opts=inout_opts)


if __name__ == "__main__":
    main(runner)
