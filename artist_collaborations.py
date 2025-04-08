from mrjob.job import MRJob
from mrjob.step import  MRStep
import csv
import io

class ArtistCollaborations(MRJob):

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
              association_array[artist1] = dict()

            if artist2 not in association_array[artist1]:
              association_array[artist1][artist2] = 0

            association_array[artist1][artist2] += 1

    for key in association_array.keys():
      yield key, association_array[key]

  def combiner_reducer(self, key, artists_dicts):
    final_dict = dict()

    for sub_list in artists_dicts:
      for k in sub_list.keys():
        if k not in final_dict:
          final_dict[k] = 0
        final_dict[k] += 1

    yield key, final_dict

  def steps(self):
    return [
    MRStep(mapper=self.mapper, combiner=self.combiner_reducer, reducer=self.combiner_reducer),
    ]

if __name__ == '__main__':
    ArtistCollaborations.run()