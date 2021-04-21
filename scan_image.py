#!/usr/bin/env python3
import exifread
import os
import hashlib
import imghdr
import re
import argparse
import multiprocessing as mp
import sqlite3 as sqlite
from datetime import datetime
from operator import itemgetter

# can use this list to restrict to only raster images, but currently using imghdr.what() just in case the extension
# is missing/incorrect
raster_list = ['.jpeg', '.jpg', '.jp2', '.tif', '.tiff', '.png', '.gif', '.bmp', '.ppm', '.pgm', '.pbm', '.pnm',
               '.webp', '.hdr', '.dib', '.heif', '.heic', '.bpg', '.iff', '.lbm', '.drw', '.ecw', '.fit', '.fits',
               '.fts', '.flif', '.img', '.jxr', '.hdp', '.wdp', '.liff', '.nrrd', '.pam', '.pcx', '.pgf', '.rgb',
               '.sgi', '.sid', '.ras', '.sun', '.ico', '.tga', '.icb', '.vda', '.vst', '.vicar', '.vic', '.xisf']


def convert_snum_array(arg):
    """covnerts string array into array"""
    b = []
    a = re.sub('[\[\]]', '', arg).split(',')
    for x in range(0, len(a)):
        a[x] = a[x].strip()
        a[x] = a[x].split(r'/')
        for y in range(0, len(a[x])):
            try:
                if y == 0:
                    div = int(a[x][y])
                else:
                    try:
                        div /= int(a[x][y])
                    except ZeroDivisionError:
                        div = 0
            except ValueError:
                div = r'/'.join(a[x])
                break
        b.append(div)
    return b


def convert_gps_array(arg, coordref):
    # a = (arg[0] + ((arg[1] + (arg[2]/60))/60))  # for standard 3 item reference
    # change the above so that it works for any length arg
    rev = [x for x in reversed(arg)]
    b = 0
    for x in rev:
        a = x + b
        b = a/60
    if coordref in ['W', 'S', '1']:
        a *= -1
    return a


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def read_file(root, f, thumb=False):
    error = False
    tags, md5checksum, uid, ftype, msg, kv_list = [None]*6
    try:
        ftype = imghdr.what(os.path.join(root, f))
    except:
        print("image type identification (imghdr.what) failed on", os.path.join(root, f))
        msg = '|'.join((os.path.join(root, f), "imghdr.what() failure")) + '\n'
    else:
        if ftype is not None:
            with open(os.path.join(root, f), 'rb') as file:
                try:
                    tags = exifread.process_file(file)
                except:
                    msg = '|'.join((os.path.join(root, f), "exifread() failure"))
                    print(msg)
                    error = True
            with open(os.path.join(root, f), 'rb') as file:  # closing and reopening prevents hash inconsistencies
                try:
                    data = file.read()
                except:
                    msg = '|'.join((os.path.join(root, f), "read() failure"))
                    print(msg)
                    error = True
                    data = None
                try:
                    md5checksum = hashlib.md5(data).hexdigest()
                except:
                    msg = '|'.join((os.path.join(root, f), "hashlib.md5() failure"))
                    print(msg)
                    md5checksum = None
                    error = True
            if not error:
                msg = '|'.join((os.path.join(root, f), 'read success'))
                # print(msg)
    if tags:
        kv_list = []
        for key, value in tags.items():
            # print(key, value)
            if 'thumb' in str(key.lower()) and not thumb:
                # print("bad key:", key)
                continue  # allows skipping thumbnail data for size reduction
            if isinstance(value, bytes):
                # # following code unusable in multiprocessing (or pathos use) due to attempted pickling of the
                # # sqlite.Binary(value) resulting in an error. The conversion has been passed off to right before
                # # the db insertion (with bin_val) to avoid this.
                # try:
                #     v = sqlite.Binary(value)
                # except:
                #     print("could not convert", key, "to sqlite binary.")
                #     v = None
                v = value
                bin_val = True
            else:
                try:
                    fieldtype = exifread.FIELD_TYPES[value.field_type]
                    if key == 'EXIF MakerNote' or fieldtype[2] == 'Proprietary':
                        v = str(value.values)
                    else:
                        v = value.printable
                except:
                    print("could not convert", key, "to usable value.")
                    v = None
                bin_val = False
            kv_list.append({'name': key, 'value': v, 'b': bin_val})
    return {'root': root, 'fname': f, 'ftype': ftype, 'hash': md5checksum, 'msg': msg, 'tags': kv_list}


def create_tables(dbpath, wipe, geo):
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    con.enable_load_extension(True)
    con.execute("PRAGMA foreign_keys = ON;")
    c = con.cursor()

    # tables
    c.execute('CREATE TABLE IF NOT EXISTS import (import_date DATETIME PRIMARY KEY, base_path TEXT, local BOOLEAN, '
              'type TEXT);')
    c.execute('CREATE TABLE IF NOT EXISTS hash (md5hash TEXT PRIMARY KEY, import_date DATETIME, '
              'FOREIGN KEY (import_date) REFERENCES import(import_date) ON DELETE CASCADE);')
    c.execute('CREATE TABLE IF NOT EXISTS photo (path TEXT PRIMARY KEY, fname TEXT, ftype TEXT, md5hash TEXT, '
              'FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE);')
    c.execute('CREATE TABLE IF NOT EXISTS tag (md5hash TEXT, tag TEXT, value TEXT, PRIMARY KEY (md5hash, tag), '
              'FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE);')
    c.execute('CREATE TABLE IF NOT EXISTS location (md5hash TEXT PRIMARY KEY, fname TEXT, path TEXT, '
              'taken_dt TEXT, X NUMERIC, Y NUMERIC, Z NUMERIC, FOREIGN KEY (md5hash) REFERENCES hash(md5hash) '
              'ON DELETE CASCADE ON UPDATE CASCADE);')

    # geometry
    if geo:
        con.execute("SELECT load_extension('mod_spatialite')")
        rows = c.execute("PRAGMA table_info('location');")
        headers = []
        for row in rows:
            headers.append(row[1])
        if 'geometry' not in headers:
            c.execute("SELECT AddGeometryColumn('location', 'geometry', 4326, 'POINTZ', 'XYZ');")
    if wipe:
        c.execute('DELETE FROM tag;')
        c.execute('DELETE FROM photo;')
        c.execute('DELETE FROM location;')
        c.execute('DELETE FROM hash;')
        c.execute('DELETE FROM import;')

    # indices
    c.execute("CREATE INDEX IF NOT EXISTS photo_md5hash_idx ON photo (md5hash);")
    c.execute("CREATE INDEX IF NOT EXISTS tag_value_idx ON tag (value);")

    con.commit()
    con.close()


def write_results(results, local, dbpath, path, import_date, log):
    print("writing results to database...")
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    con.execute("PRAGMA foreign_keys = ON;")
    c = con.cursor()
    results.sort(key=itemgetter('root', 'fname'))
    hash_sql = r'INSERT OR IGNORE INTO hash (md5hash, import_date) VALUES (?,?);'
    photo_sql = r'INSERT OR IGNORE INTO photo (path, fname, ftype, md5hash) VALUES (?,?,?,?);'
    for r in results:
        if r['ftype'] is not None:
            ins_path = os.path.join(r['root'], r['fname'])
            if local:
                ins_path = re.sub(r'^(\\|/)', '', ins_path.replace(path, ''))
            ins_path = ins_path.replace('\\', '/')  # standardizes path output across multiple os's
            # print(ins_path, r['fname'], r['ftype'], r['hash'], import_date)
            c.execute(hash_sql, (r['hash'], import_date))
            c.execute(photo_sql, (ins_path, r['fname'], r['ftype'], r['hash']))
            con.commit()
            if r['tags']:
                for t in r['tags']:
                    if t['b']:
                        val = sqlite.Binary(t['value'])
                    else:
                        val = t['value']
                    # print(r['hash'], t['name'], val)
                    c.execute('INSERT OR IGNORE INTO tag (md5hash, tag, value) VALUES (?,?,?);',
                              (r['hash'], t['name'], val,))
                con.commit()
        if log and r['msg']:
            log.write(r['msg'] + '\n')
    con.close()


def capture_meta(path, dbpath, log, cores, chunk_size, local=False, multi=False, thumb=False):
    # insert import data
    import_date = datetime.utcnow().isoformat(sep='T', timespec='seconds') + 'Z'
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    c = con.cursor()
    c.execute("INSERT OR IGNORE INTO import (import_date, base_path, local, type) VALUES (?,?,?, 'import')",
              (import_date, re.sub(r'\\', '/', path), local))
    con.commit()
    con.close()

    # insert photo data
    results = []
    if log:
        log.write('\nstarting capture_meta function at: ' + str(datetime.now()) + 'with multi=' + str(multi) +'\n')
    inputs = []
    for root, dirs, files in os.walk(path):
        inputs += [(root, f, thumb) for f in files]
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
                chunk_count += len(chunk)
                print("current chunk @", os.path.sep.join(chunk[0][0:2]))
                if multi:
                    with mp.Pool(processes=min([mp.cpu_count()-cores, 1])) as pool:
                        results = pool.starmap(read_file, chunk)
                else:
                    for root, f, t in chunk:
                        # print("Processing ", os.path.join(root, f))
                        if os.path.splitext(f)[1] not in ['.sqlite-journal']:
                            res = read_file(root, f, t)
                            results.append(res)
                # insert results into the database. SQLite concurrency locks do not allow this during multiprocessing.
                # WAL logging can be enabled but currently inserts are very fast so multiprocessing with inserts was not
                # pursued
                stime = datetime.now()
                write_results(results=results, local=local, dbpath=dbpath, path=path, import_date=import_date, log=log)
                extime = datetime.now() - stime
                pct_complete = round((chunk_count / file_length) * 100, 1)
                print("database writing finished in:", round(extime.total_seconds(), 1), "seconds.",
                      pct_complete, "% complete.")
        except KeyboardInterrupt:
            print('Breaking scan loop and quitting...')
            raise
        break
    if log:
        log.write('capture_meta function finished at: ' + str(datetime.now()) + '\n')
    return results


def convert_gis(dbpath, log):
    con = sqlite.connect(dbpath)
    con.enable_load_extension(True)
    con.execute("SELECT load_extension('mod_spatialite.dll')")
    con.row_factory = sqlite.Row
    c = con.cursor()
    i = con.cursor()

    if log:
        log.write('\n' + 'starting convert_gis function at: ' + str(datetime.now()) + '\n')
    sql = """SELECT a.fname, a.path, a.ftype, a.md5hash, b.value AS 'DateTimeOriginal',
          c.value AS 'GPSLongitude', d.value AS 'GPSLongitudeRef', e.value AS 'GPSLatitude', 
          f.value AS 'GPSLatitudeRef', g.value AS 'GPSAltitude', h.value AS 'GPSAltitudeRef'
          FROM photo AS a
          LEFT JOIN (SELECT md5hash, value FROM tag WHERE tag = 'EXIF DateTimeOriginal') AS b ON a.md5hash = b.md5hash
          LEFT JOIN (SELECT md5hash, value FROM tag WHERE tag = 'GPS GPSLongitude') AS c ON a.md5hash = c.md5hash
          LEFT JOIN (SELECT md5hash, value FROM tag WHERE tag = 'GPS GPSLongitudeRef') AS d ON a.md5hash = d.md5hash
          LEFT JOIN (SELECT md5hash, value FROM tag WHERE tag = 'GPS GPSLatitude') AS e ON a.md5hash = e.md5hash
          LEFT JOIN (SELECT md5hash, value FROM tag WHERE tag = 'GPS GPSLatitudeRef') AS f ON a.md5hash = f.md5hash
          LEFT JOIN (SELECT md5hash, value FROM tag WHERE tag = 'GPS GPSAltitude') AS g ON a.md5hash = g.md5hash
          LEFT JOIN (SELECT md5hash, value FROM tag WHERE tag = 'GPS GPSAltitudeRef') AS h ON a.md5hash = h.md5hash;"""
    rows = c.execute(sql)
    for row in rows:
        if row['GPSLongitude'] is not None:
            x = convert_gps_array(convert_snum_array(row['GPSLongitude']), row['GPSLongitudeRef'])
        else:
            x = None
        if row['GPSLatitude'] is not None:
            y = convert_gps_array(convert_snum_array(row['GPSLatitude']), row['GPSLatitudeRef'])
        else:
            y = None
        if row['GPSAltitude'] is not None:
            z = convert_gps_array(convert_snum_array(row['GPSAltitude']), row['GPSAltitudeRef'])
        else:
            z = 0
        if row['DateTimeOriginal'] is not None:
            dt = row['DateTimeOriginal'].split()
            dt[0] = dt[0].replace(':', '-')
            dtfinal = ' '.join(dt)
        else:
            dtfinal = None
        if x and y:  # will skip insert if either value is exactly 0 or None
            if log:
                log.write('|'.join((row['path'], "GPS coordinate found")) + '\n')
            # print('|'.join((row['path'], "GPS coordinate found")))
            i.execute('INSERT OR IGNORE INTO location (geometry, fname, path, md5hash, taken_dt, X, Y, Z) '
                      'VALUES (MakePointZ(?,?,?,4326),?,?,?,?,?,?,?)',
                      (x, y, z, row['fname'], row['path'], row['md5hash'], dtfinal, x, y, z))
    con.commit()
    con.close()
    if log:
        log.write('convert_gis function finished at: ' + str(datetime.now()) + '\n')


def init_db(dbpath, overwrite):
    if os.path.isfile(dbpath) and overwrite:
        try:
            os.remove(dbpath)
            print('Database deleted')
        except FileNotFoundError:
            print('No database to delete.')
    db_exists = os.path.isfile(dbpath)
    con = sqlite.connect(dbpath)
    con.execute('pragma journal_mode=WAL;')  # turns on 'write ahead logging' for concurrent writing.
    if not db_exists:
        print('Initializing Spatialite...')
        con.enable_load_extension(True)
        con.execute("SELECT load_extension('mod_spatialite')")
        con.execute("SELECT InitSpatialMetaData(1)")
        print('Spatial database initialized.')
    con.close()


if __name__ == "__main__":
    startTime = datetime.now()
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a folder and import EXIF metadata '
                                     'into a SpatiaLite database.')
    parser.add_argument('scanpath', help='path to recursively scan for image files')
    parser.add_argument('-d', '--dbpath',
                        help='the path of the spatialite database to be created. Default: '
                             'scanpath/PhotoMetadata.sqlite')
    parser.add_argument('-l', '--logpath',
                        help='the path of the log file to be generated.')
    parser.add_argument('-c', '--cores', default='2', type=int,
                        help='the number of cpu cores to leave free (if mulitprocessing).')
    parser.add_argument('-k', '--chunk_size', default='2000', type=int,
                        help='the number of files to process simultaneously before writing results to the database.')
    parser.add_argument('-p', '--local', action='store_true',
                        help='store the local path from the scan directory instead of the full path')
    parser.add_argument('-m', '--multi', action='store_true',
                        help='use multiprocessing to spread the load across multitple cores.')
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='overwrite an existing database given with --dbpath')
    parser.add_argument('-w', '--wipe', action='store_true',
                        help='wipe out values in an existing database before insert')
    parser.add_argument('-t', '--thumb', action='store_true',
                        help='store EXIF thumbnail (as BLOB) and related tags in the database.')
    parser.add_argument('-g', '--geo', action='store_true',
                        help='store lat/long data in EXIF metadata in a geometry enabled table. Requires that the '
                             'SpatiaLite extension module be loadable.')

    args = parser.parse_args()

    if args.logpath:
        log = open(args.logpath, "w")
        log.write('starting script at: ' + str(startTime) + '\n')
    else:
        log = None
    # initializes new sqlite database as spatialite database
    if not args.dbpath:
        dbpath = os.path.join(args.scanpath, 'images.sqlite')
    else:
        dbpath = args.dbpath

    if args.geo:
        init_db(dbpath, args.overwrite)
    create_tables(dbpath=dbpath, wipe=args.wipe, geo=args.geo)
    results = capture_meta(path=args.scanpath, dbpath=dbpath, log=log, cores=args.cores, chunk_size=args.chunk_size,
                           local=args.local, multi=args.multi, thumb=args.thumb)
    # print(results)
    convert_gis(dbpath, log)

    if log:
        log.write('\nfinished script at: ' + str(datetime.now()) + '\n')
        log.write('total script execution time: ' + str((datetime.now() - startTime).total_seconds()) + " seconds.")
        log.close()

    # turn off Write Ahead Logging (WAL)
    print(r'disabling WAL / vacuuming database database...')
    con = sqlite.connect(dbpath)
    con.execute('pragma journal_mode=DELETE;')
    con.execute('VACUUM;')
    con.execute('REINDEX;')
    con.close()
    print('Script finished in:', (datetime.now() - startTime).total_seconds(), 'seconds.')
