#!/usr/bin/env python3
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
from operator import itemgetter
from fuzzywuzzy import fuzz
from photo_mgmt.create_db import init_db, get_pg_con, get_sqlite_con, create_tables

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
    for i in range(0, len(full_list), n):
        yield full_list[i:i + n]


def read_file(root: str, f: str, thumb: bool = False, maker: bool = False, update: bool = False) -> \
        dict[str, str, str, str, datetime, str, dict]:
    """
    Reads a file and extracts the metadata, md5 hash, and file info.

    :param root: character string. The root directory of the file to read.
    :param f: character string. The filename.
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
    tags, md5checksum, uid, ftype, msg, kv_list, dt_mod = [None]*7
    try:
        ftype = imghdr.what(os.path.join(root, f))
    except IOError:
        print("image type identification (imghdr.what) failed on", os.path.join(root, f))
        msg = '|'.join((os.path.join(root, f), "imghdr.what() failure")) + '\n'
    else:
        if ftype is not None:
            if not update:
                with open(os.path.join(root, f), 'rb') as file:
                    try:
                        tags = exifread.process_file(file, details=details)
                    except AttributeError:
                        msg = '|'.join((os.path.join(root, f), "exifread() failure"))
                        print(msg)
                        error = True
            with open(os.path.join(root, f), 'rb') as file:  # closing and reopening prevents hash inconsistencies
                try:
                    data = file.read()
                except (IOError, OSError, FileNotFoundError):
                    msg = '|'.join((os.path.join(root, f), "read() failure"))
                    print(msg)
                    error = True
                else:
                    try:
                        md5checksum = hashlib.md5(data).hexdigest()
                    except TypeError:
                        msg = '|'.join((os.path.join(root, f), "hashlib.md5() failure"))
                        print(msg)
                        md5checksum = None
                        error = True
            if not error:
                msg = '|'.join((os.path.join(root, f), 'read success'))
                # print(msg)
            ts_mod = os.path.getmtime(os.path.join(root, f))
            dt_mod = datetime.fromtimestamp(ts_mod).strftime('%Y-%m-%d %H:%M:%S')
    return {'root': root, 'fname': f, 'ftype': ftype, 'hash': md5checksum, 'dt_mod': dt_mod, 'msg': msg,
            'tags': tags}


def make_serializable(tags: dict) -> str:
    """
    This function converts objects stored as exifread dictionary to a json serializable dictionary.

    :param tags: dictionary. An object produced by the exifread.process_file function.
    :return: A json serializable dictionary.
    """
    newdict = dict()
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
    j = json.dumps(newdict)
    return j


def write_results(results: list[dict], local: bool, con: Union[sqlite.Connection, psycopg.Connection],
                  path: str, import_date: str, log: TextIO = None, verbose: bool = False,
                  update_path: bool = False, updated: list[dict[str, str]] = None) -> list[dict[str, str]]:
    """
    Writes a list of  dictionary result from read_file function to the database.

    :param results: dictionary. Produced from the read_file function.
    :param local: Boolean. Should file paths be stored relative to the scanned directory?
    :param con: Either a sqlite3 or a psycopg2 connection object.
    :param path: character string. The directory path which was scanned for photos.
    :param import_date: character string. The datetime in UTC string iso format (e.g. YYYY-MM-DDTHH:MM:SSZ). This should
    be the datetime the import started, so it is the same for every record from the same import.
    :param log: text I/O stream. A text file opened in write mode..
    :param verbose: Boolean. Should the function print debugging information?
    :param update_path: Boolean. Should the function only update the photo table with new paths if matched photo hashes
    are found?
    :param updated: An optional list returned by this function. Primarily used to keep track of which duplicate photos
    (photos with the same hash) have been updated.
    :return: A list of dictionaries in the format of {'old_path': character string, 'new_path': character string}.
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
    if not updated:
        updated = []
    print("writing results to database...")
    c = con.cursor()
    results.sort(key=itemgetter('root', 'fname'))
    hash_sql = f'INSERT {ignore} INTO hash (md5hash) VALUES ({ph}) {conflict};'
    photo_sql = '\n'.join((f'INSERT {ignore} INTO photo (path, fname, ftype, md5hash, dt_mod, dt_import) VALUES ',
                           f'({ph},{ph},{ph},{ph},{ph},{ph}) {conflict};'))
    photo_update = f"UPDATE photo SET path = {ph}, fname = {ph}, ftype = {ph} WHERE md5hash = {ph} AND path = {ph};"
    for r in results:
        updated_paths = [u.get('old_path') for u in updated]
        if r['ftype'] is not None:
            ins_path = os.path.join(r['root'], r['fname'])
            if local:
                ins_path = re.sub(r'^([\\/])', '', ins_path.replace(path, ''))
            ins_path = ins_path.replace('\\', '/')  # standardizes path output across multiple os's
            if verbose:
                print(ins_path, r['fname'], r['ftype'], r['hash'], import_date)
            if not update_path:
                c.execute(hash_sql, (r['hash'],))
                c.execute(photo_sql, (ins_path, r['fname'], r['ftype'], r['hash'], r['dt_mod'], import_date))
            else:
                c.execute(f"SELECT path FROM photo WHERE md5hash = {ph};", (r['hash'],))
                rows = c.fetchall()
                # in the case of duplicate hashes, selects only one path to update based on best fuzzy match of paths.
                # not a perfect solution but will work better than nothing.
                if len(rows) > 0:
                    not_updated = [row['path'] for row in rows if row['path'] not in updated_paths]
                    if len(not_updated) > 0:
                        if len(not_updated) > 1:
                            pratio = [fuzz.partial_ratio(ins_path, p) for p in not_updated]
                            max_idx = pratio.index(max(pratio))
                            path = not_updated[max_idx]
                        elif len(not_updated) == 1:
                            path = not_updated[0]
                        if ins_path != path:
                            c.execute(photo_update, (ins_path, r['fname'], r['ftype'], r['hash'], path))
                            updated.append({'old_path': path, 'new_path': ins_path})
            con.commit()
            if r['tags']:
                if verbose:
                    print("inserting tags...")
                c.execute(f'INSERT {ignore} INTO tag (md5hash, meta) VALUES ({ph},{ph}) {conflict};',
                          (r['hash'], make_serializable(r['tags'])))
                con.commit()
        if log and r['msg']:
            log.write(r['msg'] + '\n')
    return updated


def capture_meta(path: str, con: Union[sqlite.Connection, psycopg.Connection], log: TextIO,
                 cores: int, chunk_size: int, local: bool = False, multi: bool = False, thumb: bool = False,
                 maker: bool = False, update: bool = False) -> \
        Tuple[list[dict[str, str, str, str, datetime, str, dict]],
              list[dict[str, str]], dict[Union[float, int], Union[float, int], int]]:
    """
    Scans a path for photos and extracts EXIF metadata as well as other file information.

    :param path: character string. The directory path which was scanned for photos.
    :param con: Either a sqlite3 or a psycopg2 connection object.
    :param log: text I/O stream. A text file opened in write mode.
    :param cores: integer. The number of cpu cores to use when multiprocessing.
    :param chunk_size: integer.  The number of photos to process at one time (read then write).
    :param local: Boolean. Should file paths be stored relative to the scanned directory
    :param multi: Boolean. Should multiprocessing be used?
    :param thumb: Boolean. Should thumbnails blobs be captured from the metadata?
    :param maker: Boolean. Should MakerNote tags be captured from the metadata?
    :param update: Boolean. Should the function only update the photo table with new paths if matched photo hashes are
    found?
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
    isql = '\n'.join((f"INSERT {ignore} INTO import (import_date, base_path, local, type) VALUES ",
                      f"({ph},{ph},{ph}, 'import') {conflict};"))
    c.execute(isql, (import_date, re.sub('/$', '', re.sub(r'\\', '/', path)), local))
    con.commit()

    # insert photo data
    results = []
    updated = []
    exec_time = {'read': 0.0, 'write': 0.0, 'files': 0}
    if log:
        log.write('\nstarting capture_meta function at: ' + str(datetime.now()) + 'with multi=' + str(multi) + '\n')
    inputs = []
    for root, dirs, files in os.walk(path):
        inputs += [(root, f, thumb, maker, update) for f in files]
    chunked = chunks(inputs, chunk_size)  # create smaller lists to feed into the processor
    file_length = len(inputs)
    chunk_count = 0
    if multi:
        print('beginning scan loop with multiprocessing enabled.')
    else:
        print('beginning scan loop with multiprocessing disabled.')
    while True:
        try:
            for chunk in chunked:
                read_start = datetime.now()
                chunk_count += len(chunk)
                print("current chunk @", os.path.sep.join(chunk[0][0:2]))
                if multi:
                    with mp.Pool(processes=min([mp.cpu_count()-cores, 1])) as pool:
                        results = pool.starmap(read_file, chunk)
                else:
                    for root, f, t, m, u in chunk:
                        # print("Processing ", os.path.join(root, f))
                        if os.path.splitext(f)[1] not in ['.sqlite-journal']:
                            res = read_file(root, f, t, m, u)
                            results.append(res)
                # insert results into the database. SQLite concurrency locks do not allow this during multiprocessing.
                # WAL logging can be enabled but currently inserts are very fast so multiprocessing with inserts was not
                # pursued
                read_time = datetime.now() - read_start
                write_start = datetime.now()
                updated = write_results(results=results, local=local, con=con, path=path,
                                        import_date=import_date, log=log, update_path=update, updated=updated)
                write_time = datetime.now() - write_start
                pct_complete = round((chunk_count / file_length) * 100, 1)
                print("database writing finished in:", round(write_time.total_seconds(), 1), "seconds.",
                      pct_complete, "% complete.")
                exec_time['read'] = exec_time['read'] + read_time.total_seconds()
                exec_time['write'] = exec_time['write'] + write_time.total_seconds()
                exec_time['files'] = exec_time['files'] + chunk_count
        except KeyboardInterrupt:
            print('Breaking scan loop and quitting...')
            raise
        break
    print('Read rate:', round(exec_time['files']/exec_time['read'], 2), 'files/sec')
    print('Write rate: ', round(exec_time['files']/exec_time['write'], 2), 'files/sec')
    if log:
        log.write('capture_meta function finished at: ' + str(datetime.now()) + '\n')
        log.write('Total files: ' + str(exec_time['files']) + ', Read time (s): ' + 
                  str(exec_time['read']) + ', Write time (s): ' + str(exec_time['write']) + '\n')
        log.write('Read: ' + str(round(exec_time['files']/exec_time['read'], 2)) + ' files/sec\n')
        log.write('Write: ' + str(round(exec_time['files']/exec_time['write'], 2)) + ' files/sec\n')
    return results, updated, exec_time


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


def update_dt(con: Union[sqlite.Connection, psycopg.Connection], log: TextIO = None,
              tz_string: str = None):
    """
    Update the dt_orig field in the photo table with datetime from the EXIF metadata.

    :param con: Either a sqlite3 or a psycopg2 connection object.
    :param log: text I/O stream. A text file opened in write mode.
    :param tz_string: character string. One of pytz.all_timezones.
    """
    if isinstance(con, psycopg.Connection):
        ph = '%s'
    elif isinstance(con, sqlite.Connection):
        ph = '?'
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")
    dt_start = datetime.now()
    if tz_string:
        tz = pytz.timezone(tz_string)
    else:
        tz = None
    c = con.cursor()
    u = con.cursor()

    if log:
        log.write('\n' + 'starting update_dt function at: ' + str(datetime.now()) + '\n')
    sql = '\n'.join((
        "SELECT md5hash, meta ->> 'EXIF DateTimeOriginal' AS dt ",
        "  FROM tag WHERE meta ->> 'EXIF DateTimeOriginal' IS NOT NULL;"))
    c.execute(sql)
    rows = c.fetchall()
    dt_updates = []
    for row in rows:
        try:
            dt_orig = datetime.strptime(row['dt'], '%Y:%m:%d %H:%M:%S')
        except ValueError:
            dt_orig = None
        if tz:
            dt_final = tz.localize(dt_orig)
        else:
            dt_final = dt_orig
        if dt_final is not None and dt_orig != '0000:00:00 00:00:00':
            dt_updates.append((dt_final, row['md5hash']))
    u.executemany(f'UPDATE photo SET dt_orig = {ph} WHERE md5hash = {ph};', dt_updates)
    con.commit()
    dt_time = (datetime.now() - dt_start).total_seconds()
    print('Date/time conversion finished in:', round(dt_time, 2), 'seconds.')
    if log:
        log.write('update_dt function finished at: ' + str(round(dt_time, 2)) + '\n')


if __name__ == "__main__":
    startTime = datetime.now()
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a folder and import EXIF metadata '
                                     'into a SpatiaLite database.')
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
    parser.add_argument('-c', '--cores', type=int,
                        help='the number of cpu cores to use in multiprocessing.')
    parser.add_argument('-k', '--chunk_size', default='2000', type=int,
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
    parser.add_argument('-g', '--geo', action='store_true',
                        help='store lat/long data in EXIF metadata in a geometry enabled table. Requires that the '
                             'SpatiaLite extension module be loadable.')
    parser.add_argument('-z', '--timezone',
                        help='a timezone available from pytz.all_timezones used to localize EXIF DateTimeOriginal. '
                             'A list of available timezones can also be found at '
                             'https://en.wikipedia.org/wiki/List_of_tz_database_time_zones')

    args = parser.parse_args()

    if args.logpath:
        my_log = open(args.logpath, "w")
        my_log.write('starting script at: ' + str(startTime) + '\n')
    else:
        my_log = None
    # initializes new sqlite database as spatialite database
    if args.dbpath:
        if args.geo:
            init_db(dbpath=args.dbpath, overwrite=args.overwrite)
        conn = get_sqlite_con(dbpath=args.dbpath, geo=args.geo)
    else:
        if args.passwd is None and not args.noask:
            args.passwd = getpass()
        conn = get_pg_con(user=args.user, database=args.db, password=args.passwd, host=args.host, port=args.port)
    if not args.update:
        create_tables(con=conn, wipe=args.wipe, geo=args.geo)
    my_results, updt, etime = capture_meta(path=args.scanpath, con=conn, log=my_log,
                                           cores=args.cores, chunk_size=args.chunk_size, local=args.local,
                                           multi=args.multi, thumb=args.thumb, maker=args.maker_note,
                                           update=args.update)
    if not args.update:
        update_dt(con=conn, log=my_log, tz_string=args.timezone)
    if args.geo and not args.update:
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
