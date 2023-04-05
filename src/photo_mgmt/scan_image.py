#!/usr/bin/env python3
import cv2
import exifread
import os
import hashlib
import imghdr
import re
import argparse
import multiprocessing as mp
import sqlite3 as sqlite
import psycopg
import psycopg.rows
import pytz
import json
import base64
from collections.abc import Collection, Reversible
from typing import Union, TextIO, Generator, Tuple
from getpass import getpass
from datetime import datetime
from tqdm import tqdm
from fuzzywuzzy import fuzz
from create_db import init_db_sqlite, init_db_pg, get_pg_con, get_sqlite_con, create_tables, create_triggers

# Necessary to fix bad detections of jpegs in imghdr.
# See https://stackoverflow.com/questions/36870661/imghdr-python-cant-detec-type-of-some-images-image-extension
from imghdr import tests


def test_jpeg1(h, f):
    """JPEG data in JFIF format"""
    if b'JFIF' in h[:23]:
        return 'jpeg'


JPEG_MARK = b'\xff\xd8\xff\xdb\x00C\x00\x08\x06\x06' \
            b'\x07\x06\x05\x08\x07\x07\x07\t\t\x08\n\x0c\x14\r\x0c\x0b\x0b\x0c\x19\x12\x13\x0f'


def test_jpeg2(h, f):
    """JPEG with small header"""
    if len(h) >= 32 and 67 == h[5] and h[:32] == JPEG_MARK:
        return 'jpeg'


def test_jpeg3(h, f):
    """JPEG data in JFIF or Exif format"""
    if h[6:10] in (b'JFIF', b'Exif') or h[:2] == b'\xff\xd8':
        return 'jpeg'


tests.append(test_jpeg1)
tests.append(test_jpeg2)
tests.append(test_jpeg3)

# can use this list to restrict to only raster images, but currently using imghdr.what() just in case the extension
# is missing/incorrect
raster_list = ['.jpeg', '.jpg', '.jp2', '.tif', '.tiff', '.png', '.gif', '.bmp', '.ppm', '.pgm', '.pbm', '.pnm',
               '.webp', '.hdr', '.dib', '.heif', '.heic', '.bpg', '.iff', '.lbm', '.drw', '.ecw', '.fit', '.fits',
               '.fts', '.flif', '.img', '.jxr', '.hdp', '.wdp', '.liff', '.nrrd', '.pam', '.pcx', '.pgf', '.rgb',
               '.sgi', '.sid', '.ras', '.sun', '.ico', '.tga', '.icb', '.vda', '.vst', '.vicar', '.vic', '.xisf']


def convert_snum_array(arg: str) -> list[float]:
    """
    Converts an array stored as a string (e.g. from metadata) into array.

    :param arg: character string representation of a rational list (e.g. '[37, 25, 438923/10000]')
    :return: a list of floats where fractions have been converted to type float.
    """
    # print('arg:', arg, 'type_arg:', type(arg))
    arg_float = []
    arg_split1 = re.sub(r'[\[\]]', '', arg).split(',')
    arg_split2 = [x.strip().split(r'/') for x in arg_split1]
    for a in arg_split2:
        if len(a) == 1:
            arg_float.append(int(a[0]))
        elif len(a) == 2:
            try:
                float_a = float(a[0])/float(a[1])
            except ZeroDivisionError:
                float_a = float(a[0])
            arg_float.append(float_a)
        else:
            arg_float.append(None)
    return arg_float


def convert_gps_array(arg: Reversible, coordref: str) -> float:
    """
    Converts GPS data stored as a degree-minute-second list to decimal degrees.

    :param arg: list. 3 float/int values e.g. [degrees, minutes, seconds].
    :param coordref: char. One of ('N', 'S', 'E', 'W') which tells the function if the return value should be negative.
    :return: float. DMS converted to decimal degrees.
    """
    arg_rev = [x for x in reversed(arg)]
    arg_dd = 0
    dec = 0
    for x in arg_rev:
        arg_dd = x + dec
        dec = arg_dd/60
    if coordref in ['W', 'S', '1']:
        arg_dd *= -1
    return arg_dd


def chunks(full_list: Collection, n: int) -> Generator:
    """
    Yield successive n-sized chunks from a list.

    :param full_list: A list of size > n.
    :param n: int. The number of values to be returned to the yielded sublist
    :return: A list of size n.
    """
    if 0 < n < len(full_list):
        for i in range(0, len(full_list), n):
            yield full_list[i:i + n]
    else:
        yield full_list


def read_file(path: str, thumb: bool = False, maker: bool = False, update: bool = False) -> \
        dict[str, str, str, str, datetime, str, dict]:
    """
    Reads a file and extracts the metadata, md5 hash, and file info.

    :param path: character string. The path of the file to read.
    :param thumb: Boolean. Read thumbnail data for storage as blob.
    :param maker: Boolean. Read MakerNote EXIF tags for storage.
    :param update: Boolean. Don't re-read exif data, just update file paths.
    :return: dictionary of photo info.
    """
    if not maker and not thumb:
        details = False
    else:
        details = True
    error = False
    tags, md5checksum, uid, ftype, msg, kv_list, dt_mod, width, height = [None]*9
    try:
        ftype = imghdr.what(path)
    except IOError:
        print("image type identification (imghdr.what) failed on", path)
        msg = '|'.join((path, "imghdr.what() failure")) + '\n'
    else:
        if ftype is not None:
            if not update:
                with open(path, 'rb') as file:
                    try:
                        tags = exifread.process_file(file, details=details)
                    except AttributeError:
                        msg = '|'.join((path, "exifread() failure"))
                        print(msg)
                        error = True
                im = cv2.imread(path)
                if im is not None:
                    height, width = im.shape[:2]
            with open(path, 'rb') as file:  # closing and reopening prevents hash inconsistencies
                try:
                    data = file.read()
                except (IOError, OSError, FileNotFoundError):
                    msg = '|'.join((path, "read() failure"))
                    print(msg)
                    error = True
                else:
                    try:
                        md5checksum = hashlib.md5(data).hexdigest()
                    except TypeError:
                        msg = '|'.join((path, "hashlib.md5() failure"))
                        print(msg)
                        md5checksum = None
                        error = True
            if not error:
                msg = '|'.join((path, 'read success'))
                # print(msg)
            ts_mod = os.path.getmtime(path)
            dt_mod = datetime.fromtimestamp(ts_mod).strftime('%Y-%m-%d %H:%M:%S')
    return {'root': os.path.dirname(path), 'fname': os.path.basename(path), 'ftype': ftype, 'hash': md5checksum,
            'dt_mod': dt_mod, 'msg': msg, 'tags': tags, 'width': width, 'height': height}


def make_serializable(tags: dict) -> dict:
    """
    This function converts objects stored as exifread dictionary to a json serializable dictionary.

    :param tags: dictionary. An object produced by the exifread.process_file function.
    :return: A json serializable dictionary.
    """
    newdict = dict()
    if tags:
        for key, value in tags.items():
            if type(value) != bytes:
                # print(key, value, type(value), value.values, type(value.values))
                v = value.printable
                t = exifread.classes.FIELD_TYPES[value.field_type]
                if key == 'EXIF MakerNote' in key:
                    n = value.values
                elif t[1] in ['B', 'S', 'L', 'SB', 'SS', 'SL', 'SR', 'R']:
                    try:
                        n = int(v)
                    except ValueError:
                        n = v
                elif t[1] in ['F32', 'F64']:
                    try:
                        n = float(v)
                    except ValueError:
                        n = v
                else:
                    n = v
            else:
                # print(key, value, type(value))
                n = base64.b64encode(value).decode()
            newdict[key] = n
    return newdict


def process_results(results: list[dict], path: str, import_date: str, tz: pytz.BaseTzInfo,
                    local: bool = False) -> list[dict]:
    """
    Writes a list of  dictionary result from read_file function to the database.

    :param results: dictionary. Produced from the read_file function.
    :param path: character string. The directory path which was scanned for photos.
    :param import_date: character string. The datetime in UTC string iso format (e.g. YYYY-MM-DDTHH:MM:SSZ). This should
    be the datetime the import started, so it is the same for every record from the same import.
    :param tz: The pytz timezone to use when storing EXIF datetimes.
    :param local: Boolean. Should file paths be stored relative to the scanned directory?
    :return: A list of dictionaries in the format of {'old_path': character string, 'new_path': character string}.
    """
    convert = []
    print("Post-processing EXIF data...")
    for i in tqdm(range(len(results)), leave=False):
        r = results[i]
        if r['ftype'] is not None:
            ins_path = os.path.join(r['root'], r['fname'])
            if local:
                ins_path = re.sub(r'^([\\/])', '', ins_path.replace(path, ''))
            ins_path = ins_path.replace('\\', '/')  # standardizes path output across multiple os's
            if r.get('tags'):
                serial_tags = make_serializable(r.get('tags'))
                json_tags = json.dumps(serial_tags)
                dt_exif = serial_tags.get('EXIF DateTimeOriginal')
                if dt_exif:
                    if dt_exif != '0000:00:00 00:00:00':
                        try:
                            dt_strip = datetime.strptime(dt_exif, '%Y:%m:%d %H:%M:%S')
                        except ValueError:
                            dt_strip = None
                        if tz:
                            dt_orig = tz.localize(dt_strip)
                        else:
                            dt_orig = dt_strip
                    else:
                        dt_orig = None
                else:
                    dt_orig = None
            else:
                json_tags = None
                dt_orig = None
            convert.append({'path': ins_path, 'fname': r['fname'], 'ftype': r['ftype'], 'md5hash': r['hash'],
                            'dt_orig': dt_orig, 'dt_mod': r['dt_mod'], 'dt_import': import_date, 'tags': json_tags,
                            'width': r['width'], 'height': r['height']})
    return convert


def write_results(results: list[dict], con: Union[sqlite.Connection, psycopg.Connection],
                  update_path: bool = False, updated: list[dict[str, str]] = None) -> (list[dict[str, str]], int):
    """
    Writes a list of  dictionary result from read_file function to the database.

    :param results: dictionary. Produced from the read_file function.
    :param con: Either a sqlite3 or a psycopg2 connection object.
    :param update_path: Boolean. Should the function only update the photo table with new paths if matched photo hashes
    are found?
    :param updated: An optional list returned by this function. Primarily used to keep track of which duplicate photos
    (photos with the same hash) have been updated.
    :return: A list of dictionaries in the format of {'old_path': character string, 'new_path': character string} and
    an integer of rows affected in the photo table by insert or update commands.
    """
    if isinstance(con, psycopg.Connection):
        ph = '%s'  # placeholder for sql parameter substitution
        nh = '%({})s'
        ignore = ''
        conflict = 'ON CONFLICT DO NOTHING'
        text = 'VARCHAR'
    elif isinstance(con, sqlite.Connection):
        ph = '?'
        nh = ':{}'
        ignore = 'OR IGNORE'
        conflict = ''
        text = 'TEXT'
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")
    if not updated:
        updated = []
    rows_affected = 0
    updated_paths = [u.get('old_path') for u in updated]
    print("\nwriting results to database...")
    c = con.cursor()
    # results.sort(key=itemgetter('root', 'fname'))
    hash_sql = f'INSERT {ignore} INTO hash (md5hash) VALUES ({nh}) {conflict};'
    hash_sql = hash_sql.format('md5hash')
    photo_sql = '\n'.join((f'INSERT {ignore} INTO photo (path, fname, ftype, md5hash, dt_orig, dt_mod, dt_import) ',
                           f'VALUES ({nh},{nh},{nh},{nh},{nh},{nh},{nh}) {conflict};'))
    photo_sql = photo_sql.format('path', 'fname', 'ftype', 'md5hash', 'dt_orig', 'dt_mod', 'dt_import')
    tag_sql = f'INSERT {ignore} INTO tag (md5hash, meta, width, height) VALUES ({nh},{nh},{nh},{nh}) {conflict};'
    tag_sql = tag_sql.format('md5hash', 'tags', 'width', 'height')
    photo_updt_simp = '\n'.join((
        f"UPDATE photo SET path = {nh}, fname = {nh}, ftype = {nh}, dt_import = {nh} WHERE md5hash = {nh};",
    ))
    photo_updt_simp = photo_updt_simp.format('path', 'fname', 'ftype', 'dt_import', 'md5hash')
    photo_updt_hard = '\n'.join((
        f"UPDATE photo SET path = {ph}, fname = {ph}, ftype = {ph}, dt_import = {ph}",
        f"  WHERE md5hash = {ph} AND path = {ph};"
    ))
    if not update_path:
        c.executemany(hash_sql, results)
        c.executemany(photo_sql, results)
        rows_affected += c.rowcount
        c.executemany(tag_sql, results)
    else:
        c.execute("DROP TABLE IF EXISTS temp_hashes;")
        c.execute(f"CREATE TEMPORARY TABLE temp_hashes (md5hash {text} PRIMARY KEY);")
        isql = f"INSERT INTO {ignore} temp_hashes (md5hash) VALUES ({nh}) {conflict};"
        c.executemany(isql.format('md5hash'),
                      results)
        count_sql = '\n'.join((
            "WITH p AS (",
            "SELECT a.md5hash, a.path",
            "  FROM photo a",
            "  INNER JOIN temp_hashes b ON a.md5hash = b.md5hash",
            ")",
            "",
            "SELECT md5hash::varchar, count(path) n",
            "  FROM p",
            " GROUP BY md5hash;"
        ))
        c.execute(count_sql)
        rows = c.fetchall()
        simple = [r['md5hash'] for r in rows if r['n'] == 1]
        simple_results = [x for x in results if x['md5hash'] in simple]
        c.executemany(photo_updt_simp, simple_results)
        rows_affected += c.rowcount
        hard = [{'md5hash': r['md5hash'], 'n': r['n']} for r in rows if r['n'] != 1]
        hard_results = [x for x in results if x['md5hash'] in hard]

        # in the case of duplicate hashes, selects only one path to update based on best fuzzy match of paths.
        # not a perfect solution but will work better than nothing.
        if hard:  # ;)
            select_hard = f"SELECT * FROM photo WHERE md5hash = {ph};"
            for h in hard_results:
                hard_rows = c.execute(select_hard, (h['md5hash'],)).fetchall()
                # guh, doing this with pandas would have been way less of a mind fuck
                pratio = [{'path': r['path'], 'ratio': fuzz.partial_ratio(h['path'], r['path'])} for r in hard_rows]
                ratio_not_updated = [p for p in pratio if p['path'] not in updated_paths]
                max_path = max(pratio, key=lambda x: x['ratio'])[0]
                if h['path'] != max_path:
                    c.execute(photo_updt_hard, (h['path'], h['fname'], h['ftype'], h['dt_import'],
                                                h['md5hash'], max_path))
                    rows_affected += c.rowcount
                    updated.append({'old_path': max_path, 'new_path': h['path']})
    con.commit()
    return updated, rows_affected


def get_existing(con):
    c = con.cursor()
    sql = '\n'.join((
        "SELECT CASE WHEN local = true THEN base_path || '/' || path",
        "            ELSE path END path",
        "  FROM photo a",
        " INNER JOIN import b ON a.dt_import = b.import_date;"
    ))
    c.execute(sql)
    existing = [x['path'] for x in c.fetchall()]
    return existing


def capture_meta(path: str, con: Union[sqlite.Connection, psycopg.Connection], tz: pytz.BaseTzInfo, log: TextIO = None,
                 threads: int = mp.cpu_count(), chunk_size: int = None, local: bool = False, multi: bool = False,
                 thumb: bool = False, maker: bool = False, update: bool = False, skip_check: bool = False) -> \
        Tuple[list[dict[str, str, str, str, datetime, str, dict]],
              list[dict[str, str]], dict[Union[float, int], Union[float, int], int]]:
    """
    Scans a path for photos and extracts EXIF metadata as well as other file information.

    :param path: character string. The directory path which was scanned for photos.
    :param con: Either a sqlite3 or a psycopg2 connection object.
    :param tz: The pytz timezone to use when storing EXIF datetimes.
    :param log: text I/O stream. A text file opened in write mode.
    :param threads: integer. The number of cpu cores to use when multiprocessing.
    :param chunk_size: integer.  The number of photos to process at one time (read then write).
    :param local: Boolean. Should file paths be stored relative to the scanned directory
    :param multi: Boolean. Should multiprocessing be used?
    :param thumb: Boolean. Should thumbnails blobs be captured from the metadata?
    :param maker: Boolean. Should MakerNote tags be captured from the metadata?
    :param update: Boolean. Should the function only update the photo table with new paths if matched photo hashes are
    found?
    :param skip_check: Boolean. Should the function skip the detection of existing paths in the database?
    :return: A list of the 'read' results, a list of the 'write' results, and the execution time of the function.
    """
    if isinstance(con, psycopg.Connection):
        ph = '%s'  # placeholder for sql parameter substitution
        ignore = ''
        conflict = 'ON CONFLICT DO NOTHING'
    elif isinstance(con, sqlite.Connection):
        ph = '?'
        ignore = 'OR IGNORE'
        conflict = ''
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")
    # insert import data
    import_date = datetime.utcnow().isoformat(sep='T', timespec='seconds') + 'Z'
    c = con.cursor()
    if update:
        import_type = 'update'
    else:
        import_type = 'import'
    isql = '\n'.join((f"INSERT {ignore} INTO import (import_date, base_path, local, type) VALUES ",
                      f"({ph},{ph},{ph},{ph}) {conflict};"))
    c.execute(isql, (import_date, re.sub('/$', '', re.sub(r'\\', '/', path)), local, import_type))
    con.commit()

    # insert photo data
    updated = []
    all_results = []
    exec_time = {'read': 0.0, 'write': 0.0, 'files': 0, 'photo_rows': 0}
    if log:
        log.write('\nstarting capture_meta function at: ' + str(datetime.now()) + 'with multi=' + str(multi) + '\n')
    print("Reading in file paths...")
    scan_start = datetime.now()
    all_files = set()
    inputs = []
    all_n = 0
    for root, dirs, files in os.walk(path):
        # all_inputs += [(root, f, thumb, maker, update) for f in files]
        for f in files:
            all_files.add(os.path.join(root, f))
        all_n = len(all_files)
    scan_end = datetime.now()
    print("Found", f'{all_n:,}', "files in", round((scan_end - scan_start).total_seconds(), 1), "seconds.")
    if not skip_check:
        discard_start = datetime.now()
        print("getting file paths from the database...")
        existing = get_existing(con=con)
        print(len(existing), "files found in database.")
        print("Removing existing files from scan list.")
        for i in tqdm(existing):
            all_files.discard(i.replace('/', os.path.sep))
        # inputs = [i for i in tqdm(all_inputs) if '/'.join((i[0], i[1])).replace('\\', '/') not in existing]
        removed = all_n - len(all_files)
        discard_end = datetime.now()
        print("Removed", f'{removed:,}', "files from scan list in",
              round((discard_end - discard_start).total_seconds(), 1), "seconds.")

    inputs += [(f, thumb, maker, update) for f in all_files]
    if not chunk_size:
        chunk_size = len(inputs)
    chunked = chunks(inputs, chunk_size)  # create smaller lists to feed into the processor
    file_length = len(inputs)
    chunk_count = 0
    if multi:
        print('beginning scan loop with multiprocessing enabled...')
    else:
        print('beginning scan loop...')
    pbar = tqdm(range(len(inputs)))
    while True:
        try:
            for chunk in chunked:
                if not chunk:
                    break
                results = []
                read_start = datetime.now()
                chunk_count += len(chunk)
                print("current chunk @", chunk[0][0])
                if multi:
                    with mp.Pool(processes=threads) as pool:
                        results = pool.starmap(read_file, chunk)
                        pbar.update(len(chunk))
                        pbar.refresh()
                else:
                    for fpath, t, m, u in chunk:
                        # print("Processing ", os.path.join(root, f))
                        if os.path.splitext(fpath)[1] not in ['.sqlite-journal']:
                            res = read_file(fpath, t, m, u)
                            results.append(res)
                        pbar.update(1)
                        pbar.refresh()
                # insert results into the database. SQLite concurrency locks do not allow this during multiprocessing.
                # WAL logging can be enabled but currently inserts are very fast so multiprocessing with inserts was not
                # pursued
                read_time = datetime.now() - read_start
                write_start = datetime.now()
                processed = process_results(results=results, path=path, import_date=import_date, tz=tz,
                                            local=local)
                updated, new_affected = write_results(results=processed, con=con, update_path=update, updated=updated)
                write_time = datetime.now() - write_start
                pct_complete = round((chunk_count / file_length) * 100, 1)
                print("database writing finished in:", round(write_time.total_seconds(), 1), "seconds.",
                      pct_complete, "% complete.")
                exec_time['read'] = exec_time['read'] + read_time.total_seconds()
                exec_time['write'] = exec_time['write'] + write_time.total_seconds()
                exec_time['files'] = exec_time['files'] + chunk_count
                exec_time['photo_rows'] = exec_time['photo_rows'] + new_affected
                all_results.extend(results)
        except KeyboardInterrupt:
            print('Breaking scan loop and quitting...')
            raise
        break
    if exec_time['read'] > 0:
        read_fps = round(exec_time['files']/exec_time['read'], 2)
    else:
        read_fps = 0
    if exec_time['write'] > 0:
        write_fps = round(exec_time['files']/exec_time['write'], 2)
    else:
        write_fps = 0
    print('\nRead rate:', read_fps, 'files/sec')
    print('Write rate: ', write_fps, 'files/sec')
    if log:
        log.write('capture_meta function finished at: ' + str(datetime.now()) + '\n')
        log.write('Total files: ' + str(exec_time['files']) + ', Read time (s): ' + 
                  str(exec_time['read']) + ', Write time (s): ' + str(exec_time['write']) + '\n')
        log.write('Read: ' + str(read_fps) + ' files/sec\n')
        log.write('Write: ' + str(write_fps) + ' files/sec\n')
    pbar.close()

    # remove record in import if nothing is imported
    test_sql = f"SELECT count(*) n FROM photo WHERE dt_import = {ph};"
    c.execute(test_sql, (import_date,))
    rows = c.fetchone()
    if rows['n'] == 0:
        c.execute(f"DELETE FROM import WHERE import_date = {ph};", (import_date,))
        con.commit()
    return all_results, updated, exec_time


def convert_gis(con: Union[sqlite.Connection, psycopg.Connection], log: TextIO = None):
    """
    Converts gis data stored in EXIF metadata tags to database geometry records.

    :param con: Either a sqlite3 or a psycopg2 connection object.
    :param log: text I/O stream. A text file opened in write mode.
    """
    gis_start = datetime.now()
    if isinstance(con, psycopg.Connection):
        ph = '%s'
        geom = 'geom'
        p = 'ST_PointZ'
        ignore = ''
        conflict = 'ON CONFLICT DO NOTHING'
    elif isinstance(con, sqlite.Connection):
        ph = '?'
        geom = 'geometry'
        p = 'MakePointZ'
        ignore = 'OR IGNORE'
        conflict = ''
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")
    c = con.cursor()
    i = con.cursor()

    if log:
        log.write('\n' + 'starting convert_gis function at: ' + str(datetime.now()) + '\n')
    # json operators -> and ->> only available in sqlite 3.38.0+
    # json support not compiled in by default in sqlite < 3.38.0
    sql = '\n'.join((
        "WITH gps AS (",
        "SELECT md5hash,",
        """meta ->> 'GPS GPSLongitude' "GPSLongitude", meta ->> 'GPS GPSLongitudeRef' "GPSLongitudeRef", """,
        """meta ->> 'GPS GPSLatitude' "GPSLatitude", meta ->> 'GPS GPSLatitudeRef' "GPSLatitudeRef", """,
        """meta ->> 'GPS GPSAltitude' "GPSAltitude", meta ->> 'GPS GPSAltitudeRef' "GPSAltitudeRef" """,
        "  FROM tag)",
        ""
        "SELECT a.ftype, a.md5hash, a.dt_orig,",
        """b."GPSLongitude", b."GPSLongitudeRef", b."GPSLatitude",""",
        """b."GPSLatitudeRef", b."GPSAltitude", b."GPSAltitudeRef" """,
        "  FROM photo AS a",
        "LEFT JOIN gps b ON a.md5hash = b.md5hash;"
    ))
    c.execute(sql)
    rows = c.fetchall()
    isql = '\n'.join((f'INSERT {ignore} INTO location (md5hash, lat, long, elev_m, {geom})',
                      f'VALUES ({ph},{ph},{ph},{ph},{p}({ph},{ph},{ph},4326)) {conflict};'))
    for row in rows:
        if row['GPSLongitude'] is not None:
            x = convert_gps_array(convert_snum_array(str(row['GPSLongitude'])), str(row['GPSLongitudeRef']))
        else:
            x = None
        if row['GPSLatitude'] is not None:
            y = convert_gps_array(convert_snum_array(str(row['GPSLatitude'])), str(row['GPSLatitudeRef']))
        else:
            y = None
        if row['GPSAltitude'] is not None:
            z = convert_gps_array(convert_snum_array(str(row['GPSAltitude'])), str(row['GPSAltitudeRef']))
        else:
            z = 0
        if x and y:  # will skip insert if either value is exactly 0 or None
            if log:
                log.write('|'.join((row['path'], "GPS coordinate found")) + '\n')
            # print('|'.join((row['path'], "GPS coordinate found")))
            i.execute(isql,
                      (row['md5hash'], x, y, z, x, y, z))
    con.commit()
    gis_time = (datetime.now() - gis_start).total_seconds()
    print('Spatial conversion finished in', round(gis_time, 2), 'seconds.')
    if log:
        log.write('convert_gis function finished in: ' + str(round(gis_time, 2)) + '\n')


if __name__ == "__main__":
    startTime = datetime.now()
    my_args = None
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a folder and import EXIF metadata '
                                     'into a SQLite/SpatiaLite or PostgreSQL/PostGIS database.')
    parser.add_argument('scanpath', help='path to recursively scan for image files')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--dbpath',
                       help='the path of the spatialite database to be created. Default: '
                            'scanpath/images.sqlite')
    group.add_argument('--db', help='the PostgreSQL database to which to connect.')
    args_pg = parser.add_argument_group('PostgreSQL')
    args_pg.add_argument('--host', default='localhost')
    args_pg.add_argument('--user', default='postgres')
    args_pg.add_argument('--port', default=5432, type=int)
    args_pg.add_argument('--passwd', help="Password for user.")
    args_pg.add_argument('--noask', action='store_true',
                         help="User will not be prompted for password if none given.")
    parser.add_argument('-l', '--logpath',
                        help='the path of the log file to be generated.')
    parser.add_argument('-c', '--threads', type=int,
                        help='the number of cpu threads to use in multiprocessing. This can be more than the number of '
                             'cores, but increasing thread count does not scale linearly due to read bottlenecks.')
    parser.add_argument('-k', '--chunk_size', type=int,
                        help='the number of files to process simultaneously before writing results to the database.')
    parser.add_argument('-p', '--local', action='store_true',
                        help='store the local path from the scan directory instead of the full path')
    parser.add_argument('-m', '--multi', action='store_true',
                        help='use multiprocessing to spread the load across multiple cores.')
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='overwrite an existing database given with --dbpath')
    parser.add_argument('-u', '--update', action='store_true',
                        help='update the file paths in a database given by --dbpath with photos found in scanpath')
    parser.add_argument('-w', '--wipe', action='store_true',
                        help='wipe out values in an existing database before insert')
    parser.add_argument('-t', '--thumb', action='store_true',
                        help='store EXIF thumbnail (as BLOB) and related tags in the database.')
    parser.add_argument('-M', '--maker_note', action='store_true',
                        help='store makernote tags in the database')
    parser.add_argument('-s', '--skip_check', action='store_true',
                        help='skips checking the database for existing paths in scanpath.')
    parser.add_argument('-g', '--geo', action='store_true',
                        help='store lat/long data in EXIF metadata in a geometry enabled table. Requires that '
                             'PostGIS is installed (PostgreSQL) or that the SpatiaLite extension module be loadable '
                             '(SQLite).')
    parser.add_argument('-z', '--timezone',
                        help='a timezone available from pytz.all_timezones used to localize EXIF DateTimeOriginal. '
                             'A list of available timezones can also be found at '
                             'https://en.wikipedia.org/wiki/List_of_tz_database_time_zones')

    args = parser.parse_args(my_args)

    if args.timezone:
        try:
            tz = pytz.timezone(args.timezone)
        except pytz.exceptions.UnknownTimeZoneError:
            print(args.timezone, "is an unknown time zone. Quitting.")
            tz = None
            quit()
    else:
        tz = None

    if args.logpath:
        my_log = open(args.logpath, "w")
        my_log.write('starting script at: ' + str(startTime) + '\n')
    else:
        my_log = None
    # initializes new sqlite database as spatialite database
    if args.dbpath:
        if args.geo:
            init_db_sqlite(dbpath=args.dbpath, overwrite=args.overwrite)
        conn = get_sqlite_con(dbpath=args.dbpath, geo=args.geo)
    else:
        if args.passwd is None and not args.noask:
            args.passwd = getpass()
        init_db_pg(user=args.user, database=args.db, password=args.passwd, host=args.host, port=args.port, geo=args.geo)
        conn = get_pg_con(user=args.user, database=args.db, password=args.passwd, host=args.host, port=args.port)
    if not args.update:
        create_tables(con=conn, wipe=args.wipe, geo=args.geo)
        create_triggers(con=conn)
    my_results, updt, etime = capture_meta(path=args.scanpath, con=conn, log=my_log,
                                           threads=args.threads, chunk_size=args.chunk_size, local=args.local,
                                           multi=args.multi, thumb=args.thumb, maker=args.maker_note,
                                           update=args.update, tz=tz, skip_check=args.skip_check)
    if my_results and args.geo and not args.update:
        print("Writing spatial features from EXIF metadata.")
        convert_gis(con=conn, log=my_log)
    if isinstance(conn, sqlite.Connection):
        # turn off Write Ahead Logging (WAL)
        print(r'disabling WAL ...')
        conn.execute('pragma journal_mode=DELETE;')
    conn.close()
    conn = None
    script_time = (datetime.now() - startTime).total_seconds()
    print('Script finished in:', script_time, 'seconds.')
    if my_log:
        my_log.write('\nfinished script at: ' + str(datetime.now()) + '\n')
        my_log.write('total script execution time: ' + str(script_time) + " seconds.")
        my_log.close()
