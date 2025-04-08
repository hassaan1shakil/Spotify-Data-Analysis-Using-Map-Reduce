from mrjob.job import MRJob
from mrjob.step import  MRStep
import csv
import io

class ArtistsSongs(MRJob):

  def mapper(self, _, line):

    # reading rows from csv file
    f = io.StringIO(line)
    reader = csv.reader(f)
    fields = next(reader)

    association_array = dict()

    if fields[0] != 'track_name':
      track_name = fields[0].strip()
      year = fields[3].strip()
      artists = fields[1].split(',')

      for artist in artists:
        artist = artist.strip()

        if artist not in association_array:
          association_array[artist] = list()

        association_array[artist].append(f'{track_name}({year})')

    for key in association_array.keys():
      yield key, association_array[key]

  def combiner_reducer(self, key, tracks_details):
    final_songs = []
    for tracks in tracks_details:
      for track in tracks:
        final_songs.append(track)

    yield key, final_songs

  def steps(self):
    return [
    MRStep(mapper=self.mapper, combiner=self.combiner_reducer, reducer=self.combiner_reducer),
    ]

if __name__ == '__main__':
    ArtistsSongs.run()