from mrjob.job import MRJob
import csv
import io

class AvgSongsPerMonth(MRJob):

  def mapper(self, _, line):

    # reading rows from csv file
    f = io.StringIO(line)
    reader = csv.reader(f)
    fields = next(reader)

    association_array = dict()
#   (July: {2007: 100}, {2008: 34} over the course of 5 Julys
    if fields[0] != 'track_name':
      year = fields[3].strip()
      month = fields[4].strip()

      if month not in association_array:
        association_array[month] = dict()

        if year not in association_array[month]:
          association_array[month][year] = 0

        association_array[month][year] += 1

      for key in association_array.keys():
        yield key, association_array[key]

  def combiner(self, key, data):
    final_dict = dict()

    for years_list in data:
      for year in years_list:
        if not year in final_dict:
          final_dict[year] = 0
        final_dict[year] += years_list[year]

    yield key, final_dict

  def reducer(self, key, data):
    final_dict = dict()

    for years_list in data:
      for year in years_list:
        if not year in final_dict:
          final_dict[year] = 0
        final_dict[year] += years_list[year]

    avg = sum(final_dict.values())/len(final_dict.keys())
    yield key, avg

if __name__ == '__main__':
    AvgSongsPerMonth.run()