from mrjob.job import MRJob
from mrjob.step import  MRStep
import csv
import io

class ArtistsWithSingleRelease(MRJob):

  def mapper(self, _, line):

    # reading rows from csv file
    f = io.StringIO(line)
    reader = csv.reader(f)
    fields = next(reader)

    association_array = dict()

    if fields[0] != 'track_name':
      artists = fields[1].split(',')
      year = fields[3].strip()

      for artist in artists:
        artist = artist.strip()
        if year not in association_array:
          association_array[year] = dict()

          if artist not in association_array[year]:
            association_array[year][artist] = 0

          association_array[year][artist] += 1

    for key in association_array.keys():
      yield key, association_array[key]

  def combiner_reducer(self, key, data):
    final_dict = dict()

    for artists_list in data:
      for artist in artists_list:
        if not artist in final_dict:
          final_dict[artist] = 0
        final_dict[artist] += artists_list[artist]

    yield key, final_dict

  def reducer_count_releases(self, key, data):
    final_dict = dict()

    for artists_list in data:
      for artist in artists_list:
        if not artist in final_dict:
          final_dict[artist] = 0
        final_dict[artist] += artists_list[artist]

    sorted_dict = sorted(final_dict.items(), key=lambda item: item[1])
    filtered_dict = [item for item in sorted_dict if item[1] == 1]
    yield key, filtered_dict

  def steps(self):
    return [
      MRStep(mapper=self.mapper, combiner=self.combiner_reducer, reducer=self.reducer_count_releases),
    ]

if __name__ == '__main__':
    ArtistsWithSingleRelease.run()