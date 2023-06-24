import os
import csv
import glob

import s3fs
import tables
from tqdm.auto import tqdm

def _create_song_dict(except_columns=[]):
    columns = set(["artist_terms", "mode", "danceability", "loudness", "bars_confidence", "title", "track_id", "mode_confidence", "track_7digitalid", "sections_confidence", "artist_location", "release", "beats_confidence", "time_signature_confidence", "artist_playmeid", "artist_name", "artist_mbtags_count", "artist_latitude", "key_confidence", "tatums_confidence", "segments_start", "analysis_sample_rate", "artist_terms_freq", "end_of_fade_in", "energy", "artist_hotttnesss", "segments_confidence", "song_id", "song_hotttnesss", "artist_mbid", "artist_7digitalid", "release_7digitalid", "duration", "similar_artists", "segments_timbre", "artist_mbtags", "sections_start", "artist_id", "artist_familiarity", "segments_loudness_start", "start_of_fade_out", "key", "artist_longitude", "tempo", "bars_start", "artist_terms_weight", "beats_start", "segments_pitches", "segments_loudness_max", "tatums_start", "year", "audio_md5", "time_signature", "segments_loudness_max_time"]) - set(except_columns)
    return dict.fromkeys(columns, None)
   

def _get_data(f, except_columns=[]):
    result = []
    
    h5 = tables.open_file(f, mode='r')

    num_songs = h5.root.metadata.songs.nrows
    for i in range(num_songs):
        song = _create_song_dict(except_columns)

        # artist_familiarity
        if "artist_familiarity" in song:
            song["artist_familiarity"] = float(h5.root.metadata.songs.cols.artist_familiarity[i])

        # artist_hotttnesss
        if "artist_hotttnesss" in song:
            song["artist_hotttnesss"] = float(h5.root.metadata.songs.cols.artist_hotttnesss[i])

        # artist_id
        if "artist_id" in song:
            song["artist_id"] = (h5.root.metadata.songs.cols.artist_id[i]).decode("utf-8")

        # artist_mbid
        if "artist_mbid" in song:
            song["artist_mbid"] = (h5.root.metadata.songs.cols.artist_mbid[i]).decode("utf-8")

        # artist_playmeid
        if "artist_playmeid" in song:
            song["artist_playmeid"] = int(h5.root.metadata.songs.cols.artist_playmeid[i])

        # artist_7digitalid
        if "artist_7digitalid" in song:
            song["artist_7digitalid"] = int(h5.root.metadata.songs.cols.artist_7digitalid[i])

        # artist_latitude
        if "artist_latitude" in song:
            song["artist_latitude"] = float(h5.root.metadata.songs.cols.artist_latitude[i])

        # artist_longitude
        if "artist_longitude" in song:
            song["artist_longitude"] = float(h5.root.metadata.songs.cols.artist_longitude[i])

        # artist_location
        if "artist_location" in song:
            song["artist_location"] = (h5.root.metadata.songs.cols.artist_location[i]).decode("utf-8")

        # artist_name
        if "artist_name" in song:
            song["artist_name"] = (h5.root.metadata.songs.cols.artist_name[i]).decode("utf-8")

        # release
        if "release" in song:
            song["release"] = (h5.root.metadata.songs.cols.release[i]).decode("utf-8")

        # release_7digitalid
        if "release_7digitalid" in song:
            song["release_7digitalid"] = int(h5.root.metadata.songs.cols.release_7digitalid[i])

        # song_id
        if "song_id" in song:
            song["song_id"] = (h5.root.metadata.songs.cols.song_id[i]).decode("utf-8")

        # song_hotttnesss
        if "song_hotttnesss" in song:
            song["song_hotttnesss"] = float(h5.root.metadata.songs.cols.song_hotttnesss[i])

        # title
        if "title" in song:
            song["title"] = (h5.root.metadata.songs.cols.title[i]).decode("utf-8")

        # track_7digitalid
        if "track_7digitalid" in song:
            song["track_7digitalid"] = int(h5.root.metadata.songs.cols.track_7digitalid[i])

        # similar_artists
        if "similar_artists" in song:
            start = h5.root.metadata.songs.cols.idx_similar_artists[i]
            end = h5.root.metadata.songs.cols.idx_similar_artists[i + 1] if i < num_songs - 1 else None
            
            song["similar_artists"] = h5.root.metadata.similar_artists[start:end]
            song["similar_artists"] = [x.decode("utf-8") for x in song["similar_artists"]]
        
        # artist_terms
        if "artist_terms" in song:
            start = h5.root.metadata.songs.cols.idx_artist_terms[i]
            end = h5.root.metadata.songs.cols.idx_artist_terms[i + 1] if i < num_songs - 1 else None
            
            song["artist_terms"] = h5.root.metadata.artist_terms[start:end]
            song["artist_terms"] = [x.decode("utf-8") for x in song["artist_terms"]]

        # artist_terms_freq
        if "artist_terms_freq" in song:
            start = h5.root.metadata.songs.cols.idx_artist_terms[i]
            end = h5.root.metadata.songs.cols.idx_artist_terms[i + 1] if i < num_songs - 1 else None
            
            song["artist_terms_freq"] = h5.root.metadata.artist_terms_freq[start:end]
            song["artist_terms_freq"] = [float(x) for x in song["artist_terms_freq"]]

        # artist_terms_weight
        if "artist_terms_weight" in song:
            start = h5.root.metadata.songs.cols.idx_artist_terms[i]
            end = h5.root.metadata.songs.cols.idx_artist_terms[i + 1] if i < num_songs - 1 else None
            
            song["artist_terms_weight"] = h5.root.metadata.artist_terms_weight[start:end]
            song["artist_terms_weight"] = [float(x) for x in song["artist_terms_weight"]]

        # analysis_sample_rate
        if "analysis_sample_rate" in song:
            song["analysis_sample_rate"] = float(h5.root.analysis.songs.cols.analysis_sample_rate[i])

        # audio_md5
        if "audio_md5" in song:
            song["audio_md5"] = (h5.root.analysis.songs.cols.audio_md5[i]).decode("utf-8")
        
        # danceability
        if "danceability" in song:
            song["danceability"] = float(h5.root.analysis.songs.cols.danceability[i])
        
        # duration
        if "duration" in song:
            song["duration"] = float(h5.root.analysis.songs.cols.duration[i])

        # end_of_fade_in
        if "end_of_fade_in" in song:
            song["end_of_fade_in"] = float(h5.root.analysis.songs.cols.end_of_fade_in[i])
        
        # energy
        if "energy" in song:
            song["energy"] = float(h5.root.analysis.songs.cols.energy[i])
        
        # key
        if "key" in song:
            song["key"] = int(h5.root.analysis.songs.cols.key[i])
        
        # key_confidence
        if "key_confidence" in song:
            song["key_confidence"] = float(h5.root.analysis.songs.cols.key_confidence[i])

        # loudness
        if "loudness" in song:
            song["loudness"] = float(h5.root.analysis.songs.cols.loudness[i])

        # mode
        if "mode" in song:
            song["mode"] = int(h5.root.analysis.songs.cols.mode[i])

        # mode_confidence
        if "mode_confidence" in song:
            song["mode_confidence"] = float(h5.root.analysis.songs.cols.mode_confidence[i])

        # start_of_fade_out
        if "start_of_fade_out" in song:
            song["start_of_fade_out"] = float(h5.root.analysis.songs.cols.start_of_fade_out[i])

        # tempo
        if "tempo" in song:
            song["tempo"] = float(h5.root.analysis.songs.cols.tempo[i])

        # time_signature
        if "time_signature" in song:
            song["time_signature"] = int(h5.root.analysis.songs.cols.time_signature[i])
        
        # time_signature_confidence
        if "time_signature_confidence" in song:
            song["time_signature_confidence"] = float(h5.root.analysis.songs.cols.time_signature_confidence[i])

        # track_id
        if "track_id" in song:
            song["track_id"] = (h5.root.analysis.songs.cols.track_id[i]).decode("utf-8")

        # segments_start
        if "segments_start" in song:
            start = h5.root.analysis.songs.cols.idx_segments_start[i]
            end = h5.root.analysis.songs.cols.idx_segments_start[i + 1] if i < num_songs - 1 else None
            
            song["segments_start"] = h5.root.analysis.segments_start[start:end]
            song["segments_start"] = [float(x) for x in song["segments_start"]]

        # segments_confidence
        if "segments_confidence" in song:
            start = h5.root.analysis.songs.cols.idx_segments_confidence[i]
            end = h5.root.analysis.songs.cols.idx_segments_confidence[i + 1] if i < num_songs - 1 else None
            
            song["segments_confidence"] = h5.root.analysis.segments_confidence[start:end]
            song["segments_confidence"] = [float(x) for x in song["segments_confidence"]]

        # segments_pitches
        if "segments_pitches" in song:
            start = h5.root.analysis.songs.cols.idx_segments_pitches[i]
            end = h5.root.analysis.songs.cols.idx_segments_pitches[i + 1] if i < num_songs - 1 else None
            
            song["segments_pitches"] = h5.root.analysis.segments_pitches[start:end]
            song["segments_pitches"] = [[float(y) for y in x] for x in song["segments_pitches"]]

        # segments_timbre
        if "segments_timbre" in song:
            start = h5.root.analysis.songs.cols.idx_segments_timbre[i]
            end = h5.root.analysis.songs.cols.idx_segments_timbre[i + 1] if i < num_songs - 1 else None
            
            song["segments_timbre"] = h5.root.analysis.segments_timbre[start:end]
            song["segments_timbre"] = [[float(y) for y in x] for x in song["segments_timbre"]]

        # segments_loudness_max
        if "segments_loudness_max" in song:
            start = h5.root.analysis.songs.cols.idx_segments_loudness_max[i]
            end = h5.root.analysis.songs.cols.idx_segments_loudness_max[i + 1] if i < num_songs - 1 else None
            
            song["segments_loudness_max"] = h5.root.analysis.segments_loudness_max[start:end]
            song["segments_loudness_max"] = [float(x) for x in song["segments_loudness_max"]]

        # segments_loudness_max_time
        if "segments_loudness_max_time" in song:
            start = h5.root.analysis.songs.cols.idx_segments_loudness_max_time[i]
            end = h5.root.analysis.songs.cols.idx_segments_loudness_max_time[i + 1] if i < num_songs - 1 else None
            
            song["segments_loudness_max_time"] = h5.root.analysis.segments_loudness_max_time[start:end]
            song["segments_loudness_max_time"] = [float(x) for x in song["segments_loudness_max_time"]]

        # segments_loudness_start
        if "segments_loudness_start" in song:
            start = h5.root.analysis.songs.cols.idx_segments_loudness_start[i]
            end = h5.root.analysis.songs.cols.idx_segments_loudness_start[i + 1] if i < num_songs - 1 else None
            
            song["segments_loudness_start"] = h5.root.analysis.segments_loudness_start[start:end]
            song["segments_loudness_start"] = [float(x) for x in song["segments_loudness_start"]]

        # sections_start
        if "sections_start" in song:
            start = h5.root.analysis.songs.cols.idx_sections_start[i]
            end = h5.root.analysis.songs.cols.idx_sections_start[i + 1] if i < num_songs - 1 else None
            
            song["sections_start"] = h5.root.analysis.sections_start[start:end]
            song["sections_start"] = [float(x) for x in song["sections_start"]]

        # sections_confidence
        if "sections_confidence" in song:
            start = h5.root.analysis.songs.cols.idx_sections_confidence[i]
            end = h5.root.analysis.songs.cols.idx_sections_confidence[i + 1] if i < num_songs - 1 else None
            
            song["sections_confidence"] = h5.root.analysis.sections_confidence[start:end]
            song["sections_confidence"] = [float(x) for x in song["sections_confidence"]]

        # beats_start
        if "beats_start" in song:
            start = h5.root.analysis.songs.cols.idx_beats_start[i]
            end = h5.root.analysis.songs.cols.idx_beats_start[i + 1] if i < num_songs - 1 else None
            
            song["beats_start"] = h5.root.analysis.beats_start[start:end]
            song["beats_start"] = [float(x) for x in song["beats_start"]]
        
        # beats_confidence
        if "beats_confidence" in song:
            start = h5.root.analysis.songs.cols.idx_beats_confidence[i]
            end = h5.root.analysis.songs.cols.idx_beats_confidence[i + 1] if i < num_songs - 1 else None
            
            song["beats_confidence"] = h5.root.analysis.beats_confidence[start:end]
            song["beats_confidence"] = [float(x) for x in song["beats_confidence"]]
        
        # bars_start
        if "bars_start" in song:
            start = h5.root.analysis.songs.cols.idx_bars_start[i]
            end = h5.root.analysis.songs.cols.idx_bars_start[i + 1] if i < num_songs - 1 else None
            
            song["bars_start"] = h5.root.analysis.bars_start[start:end]
            song["bars_start"] = [float(x) for x in song["bars_start"]]
        
        # bars_confidence
        if "bars_confidence" in song:
            start = h5.root.analysis.songs.cols.idx_bars_confidence[i]
            end = h5.root.analysis.songs.cols.idx_bars_confidence[i + 1] if i < num_songs - 1 else None
            
            song["bars_confidence"] = h5.root.analysis.bars_confidence[start:end]
            song["bars_confidence"] = [float(x) for x in song["bars_confidence"]]
        
        # tatums_start
        if "tatums_start" in song:
            start = h5.root.analysis.songs.cols.idx_tatums_start[i]
            end = h5.root.analysis.songs.cols.idx_tatums_start[i + 1] if i < num_songs - 1 else None
            
            song["tatums_start"] = h5.root.analysis.tatums_start[start:end]
            song["tatums_start"] = [float(x) for x in song["tatums_start"]]
        
        # tatums_confidence
        if "tatums_confidence" in song:
            start = h5.root.analysis.songs.cols.idx_tatums_confidence[i]
            end = h5.root.analysis.songs.cols.idx_tatums_confidence[i + 1] if i < num_songs - 1 else None
            
            song["tatums_confidence"] = h5.root.analysis.tatums_confidence[start:end]
            song["tatums_confidence"] = [float(x) for x in song["tatums_confidence"]]
        
        # artist_mbtags
        if "artist_mbtags" in song:
            start = h5.root.musicbrainz.songs.cols.idx_artist_mbtags[i]
            end = h5.root.musicbrainz.songs.cols.idx_artist_mbtags[i + 1] if i < h5.root.musicbrainz.songs.nrows - 1 else None

            song["artist_mbtags"] = h5.root.musicbrainz.artist_mbtags[start:end]
            song["artist_mbtags"] = [tag.decode("utf-8") for tag in song["artist_mbtags"]]

        # artist_mbtags_count
        if "artist_mbtags_count" in song:
            start = h5.root.musicbrainz.songs.cols.idx_artist_mbtags[i]
            end = h5.root.musicbrainz.songs.cols.idx_artist_mbtags[i + 1] if i < h5.root.musicbrainz.songs.nrows - 1 else None

            song["artist_mbtags_count"] = h5.root.musicbrainz.artist_mbtags_count[start:end]
            song["artist_mbtags_count"] = [int(x) for x in song["artist_mbtags_count"]]

        # year
        if "year" in song:
            song["year"] = int(h5.root.musicbrainz.songs.cols.year[i])

        result.append(song)

    h5.close()
    return result
   

def _write_csv(output_dir, csv_idx, songs, csv_delim="comma", aws_access_key=None, aws_secret_key=None):
    ext = "csv" if csv_delim == "comma" else "tsv"
    delimiter = "," if csv_delim == "comma" else "\t"

    print(f"Writing {len(songs)} songs to {output_dir}")

    if output_dir.startswith("s3://"):
        fs = s3fs.S3FileSystem(key=aws_access_key, secret=aws_secret_key)
        with fs.open(os.path.join(output_dir, f"msd_{csv_idx}.{ext}"), "w") as f:
            writer = csv.DictWriter(f, fieldnames=songs[0].keys(), delimiter=delimiter)
            writer.writeheader()
            writer.writerows(songs)
    else:
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, f"msd_{csv_idx}.{ext}"), "w") as f:
            writer = csv.DictWriter(f, fieldnames=songs[0].keys(), delimiter=delimiter)
            writer.writeheader()
            writer.writerows(songs)


def build_csv(root_dir, output_dir, max_rows=1000, except_columns=[], csv_delim="comma", aws_access_key=None, aws_secret_key=None):
    """
    Build a CSV file from the MSD HDF5 files.

    Parameters
    ----------
    root_dir : str
        The root directory of the MSD HDF5 files.
        (probably in format "/<mount-directory>/data",
        as MSD in EBS volume(snap-5178cf30) is in the "data" directory)
    output_dir : str
        The output directory of the CSV file(s).
        If the value starts with "s3://", the CSV file(s) will be uploaded to the S3 bucket.
        To upload the CSV file(s) to the S3 bucket, the AWS credentials(s3_access_key, s3_secret_key) must be provided.
    max_rows : int
        The maximum number of rows in each CSV file.
        If the value is None, the function will create a single CSV file containing all the rows.
        (default: 1000000)
    except_columns : list
        The columns to be excluded from the CSV file.
        (default: [])
    csv_delim : str
        The delimiter of the CSV file. 
        Either "comma" or "tab".
        If the value is "comma", the delimiter is ",", and the CSV file will be named as "msd_<index>.csv".
        If the value is "tab", the delimiter is "\t", and the CSV file will be named as "msd_<index>.tsv".
        (default: "comma")
    aws_access_key : str
        The AWS access key to upload the CSV file(s) to the S3 bucket.
        If output_dir does not start with "s3://", this value is ignored.
        If output_dir starts with "s3://", this value must be provided.
    aws_secret_key : str
        The AWS secret key to upload the CSV file(s) to the S3 bucket.
        If output_dir does not start with "s3://", this value is ignored.
        If output_dir starts with "s3://", this value must be provided.
    """
    h5_files = glob.glob(os.path.join(root_dir, "**/*.h5"), recursive=True)
    bucket = []
    csv_idx = 0

    for file in tqdm(h5_files):
        bucket += _get_data(file, except_columns)

        while max_rows is not None and len(bucket) >= max_rows:
            _write_csv(output_dir, csv_idx, bucket[:max_rows], csv_delim=csv_delim, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)
            
            bucket = bucket[max_rows:]
            csv_idx += 1
    
    # Write the remaining rows
    if len(bucket) > 0:
        _write_csv(output_dir, csv_idx, bucket, csv_delim=csv_delim, aws_access_key=aws_access_key, aws_secret_key=aws_secret_key)
