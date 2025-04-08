from mrjob.job import MRJob
from mrjob.step import  MRStep
import csv
import io

class ArtistsWithMaxReleases(MRJob):

  def mapper(self, _, line):

    # reading rows from csv file
    f = io.StringIO(line)
    reader = csv.reader(f)
    fields = next(reader)

    association_array = dict()

    if fields[0] != 'track_name':
      artists = fields[1].split(',')

      for artist in artists:
        artist = artist.strip()
        if artist not in association_array:
          association_array[artist] = 1
        else:
          association_array[artist] += 1

    for key in association_array.keys():
      yield key, association_array[key]

  def combiner(self, key, no_of_releases):
    yield key, sum(no_of_releases)

  def reducer_count_releases(self, key, no_of_releases):
    yield None, (sum(no_of_releases), key)

  def reducer_find_max(self, _, artist_count_pair):
    yield max(artist_count_pair)

  def steps(self):
    return [
    MRStep(mapper=self.mapper, combiner=self.combiner, reducer=self.reducer_count_releases),
    MRStep(reducer=self.reducer_find_max)
    ]

if __name__ == '__main__':
    ArtistsWithMaxReleases.run()