import sqlite3 as sqlite
import os
import hashlib
import argparse
import re
from datetime import datetime


def hash_files(scan_path):
    hash_paths = []
    for root, dirs, files in os.walk(scan_path):
        for f in files:
            full_path = os.path.join(root, f)
            local_path = re.sub(r'^[\\/]', '', full_path.replace(scan_path, ''))
            with open(full_path, 'rb') as file:  # closing and reopening prevents hash inconsistencies
                try:
                    data = file.read()
                except:
                    msg = ' '.join(("read() failure for:", full_path,))
                    print(msg)
                    data = None
                try:
                    md5checksum = hashlib.md5(data).hexdigest()
                except:
                    msg = ' '.join(("hashlib.md5() failure for:", full_path))
                    print(msg)
                    md5checksum = None
            hash_paths.append((md5checksum, full_path, local_path))
    return hash_paths


def update_db(dbpath, base_path, local, hashed):
    hashed_iter = hashed.copy()
    import_date = datetime.utcnow().isoformat(sep='T', timespec='seconds') + 'Z'
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    c = con.cursor()
    c.execute("INSERT OR IGNORE INTO import (import_date, base_path, local, type) VALUES (?,?,?, 'update')",
              (import_date, re.sub(r'\\', '/', base_path), local))
    con.commit()
    sql = '\n'.join((
        "SELECT a.*, b.import_date, c.base_path",
        "  FROM photo a"
        "  LEFT JOIN hash b ON a.md5hash = b.md5hash"
        "  LEFT JOIN import c ON b.import_date = c.import_date"
        " ORDER BY c.base_path, a.path;"
    ))
    rows = c.execute(sql).fetchall()
    for row in rows:
        hash_list = [x[0] for x in hashed_iter]
        # old_full = os.path.abspath(os.path.join(row['base_path'], row['path']))
        md5hash = row['md5hash']
        # using this approach (instead of a join) to deal with duplicate files with same hash
        if md5hash in hash_list:
            idx = hash_list.index(md5hash)
            new_path = re.sub(r'\\', '/', hashed_iter[idx][1])
            new_local = re.sub(r'\\', '/', hashed_iter[idx][2])
            fname = os.path.basename(hashed_iter[idx][1])
            print('hash found:', md5hash, 'path:', new_path)
            if local:
                store_path = new_local
            else:
                store_path = new_path
            c.execute("UPDATE photo SET path = ?, fname = ? WHERE path = ?;", (store_path, fname, row['path']))
            c.execute("UPDATE hash SET import_date = ? WHERE md5hash = ?;", (import_date, md5hash))
            hashed_iter.pop(idx)  # this stops us from reusing the same path for duplicate files with same hash
    con.commit()
    con.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a folder and reconnect photo paths in the '
                                                 'database based on their hash. Running this script is necessary after '
                                                 'manual renaming or movement of photos in their directory structure.')
    parser.add_argument('scanpath', help='path to recursively scan for image files')
    parser.add_argument('dbpath',
                        help='the path of the spatialite database created via the scan_photo.py module')
    parser.add_argument('-p', '--local', action='store_true',
                        help='store the local path from the scan directory instead of the full path')
    args = parser.parse_args()
    hashed_f = hash_files(scan_path=args.scanpath)
    update_db(hashed=hashed_f, dbpath=args.dbpath, local=args.local, base_path=args.scanpath)
    print('Script finished.')
