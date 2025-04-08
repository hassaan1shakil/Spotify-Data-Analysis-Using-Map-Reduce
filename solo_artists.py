from mrjob.job import MRJob
from mrjob.step import  MRStep
import csv
import io

class SoloArtists(MRJob):

  def mapper(self, _, line):

    # reading rows from csv file
    f = io.StringIO(line)
    reader = csv.reader(f)
    fields = next(reader)

    association_array = dict()

    if fields[0] != 'track_name':
      artists = fields[1].split(',')

      for artist1 in artists:
        artist1 = artist1.strip()

        for artist2 in artists:
          artist2 = artist2.strip()

          if artist1 != artist2:
            if artist1 not in association_array:
              association_array[artist1] = True   # Solo Artists
            else:
              association_array[artist1] = False  # Non-Solo Artists

    for key in association_array.keys():
      yield key, association_array[key]

  def combiner(self, key, bool_flag):
    yield key, all(bool_flag)

  def reducer(self, key, bool_flag):
    if all(bool_flag):
      yield key, "Solo Performer"

  def steps(self):
    return [
    MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer),
    ]

if __name__ == '__main__':
    SoloArtists.run()