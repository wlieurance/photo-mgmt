import sqlite3 as sqlite
import psycopg
import os
import hashlib
import argparse
import re
import uuid
from typing import Union
from datetime import datetime
from getpass import getpass
from photo_mgmt.create_db import get_pg_con, get_sqlite_con


def hash_files(scan_path: str) -> list[tuple]:
    """
    Scans a directory and returns the md5 hash for each files found

    :param scan_path: A directory to scan.
    :return: A list of tuples in the form of (md5 checksum, full path, local path)
    """
    hash_paths = []
    for root, dirs, files in os.walk(scan_path):
        for f in files:
            full_path = os.path.join(root, f)
            print(full_path)
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


def update_db(con: Union[sqlite.Connection, psycopg.Connection], base_path: str, local: bool,
              hashed: list[tuple]) -> int:
    """
    Updates a photo management database with the results of the hash_files function, allowing photos which have been
    moved or renamed to be reconnected to records in the database.

    :param con: A database connection object.
    :param base_path: The base path that was scanned in the hash_files function.
    :param local: Whether to store local paths in relation to base_path or store full paths.
    :param hashed: The list of hashes produced from the hash_files function.
    :return: A count of the records updated in the database.
    """
    if isinstance(con, psycopg.Connection):
        ph = '%s'
        ignore = ''
        conflict = 'ON CONFLICT DO NOTHING'
    elif isinstance(con, sqlite.Connection):
        ph = '?'
        ignore = 'OR IGNORE'
        conflict = ''
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")
    hashed_iter = hashed.copy()
    import_date = datetime.utcnow().isoformat(sep='T', timespec='seconds') + 'Z'
    c = con.cursor()
    c.execute(
        f"INSERT {ignore} INTO import (import_date, base_path, local, type) VALUES ({ph},{ph},{ph}, 'update') "
        f"{conflict};",
        (import_date, re.sub(r'\\', '/', base_path), local))
    sql = '\n'.join((
        "SELECT a.*, b.base_path",
        "  FROM photo a"
        "  LEFT JOIN import b ON a.dt_import = b.import_date"
        " ORDER BY b.base_path, a.path;"
    ))
    rows = c.execute(sql).fetchall()
    count = 0
    for row in rows:
        hash_list = [x[0] for x in hashed_iter]
        # old_full = os.path.abspath(os.path.join(row['base_path'], row['path']))
        md5hash = row['md5hash']
        if isinstance(md5hash, uuid.UUID):
            md5hash = md5hash.hex
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
            c.execute(f"UPDATE photo SET path = {ph}, fname = {ph}, dt_import = {ph} WHERE path = {ph};",
                      (store_path, fname, import_date, row['path']))
            updated = c.rowcount
            hashed_iter.pop(idx)  # this stops us from reusing the same path for duplicate files with same hash
            count += updated
    if count == 0:
        c.execute(f"DELETE FROM import WHERE import_date = {ph};", (import_date,))
    con.commit()
    con.close()
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will scan a folder and reconnect photo paths in the '
                                                 'database based on their hash. Running this script is necessary after '
                                                 'manual renaming or movement of photos in their directory structure.')
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
    parser.add_argument('-p', '--local', action='store_true',
                        help='store the local path from the scan directory instead of the full path')
    args = parser.parse_args()

    if args.dbpath:
        conn = get_sqlite_con(dbpath=args.dbpath, geo=False)
    else:
        if args.passwd is None and not args.noask:
            args.passwd = getpass()
        conn = get_pg_con(user=args.user, database=args.db, password=args.passwd, host=args.host, port=args.port)

    hashed_f = hash_files(scan_path=args.scanpath)
    rn = update_db(hashed=hashed_f, con=conn, local=args.local, base_path=args.scanpath)
    conn.close()
    conn = None
    print(rn, "rows affected.")
    print('Script finished.')
