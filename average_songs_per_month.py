from mrjob.job import MRJob
import csv
import io

class AvgSongsPerMonth(MRJob):

  def mapper(self, _, line):

    # reading rows from csv file
    f = io.StringIO(line)
    reader = csv.reader(f)
    fields = next(reader)

    if fields[0] != 'track_name':
      yield "average song releases per month", 1


  def combiner(self, key, values_list):
    yield key, sum(values_list)


  def reducer(self, key, values_list):
    yield key, sum(values_list)/12

if __name__ == '__main__':
    AvgSongsPerMonth.run()