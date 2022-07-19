#!/usr/bin/env python3
import argparse
import os
import sqlite3 as sqlite
import psycopg
import psycopg.rows
from typing import Union
from getpass import getpass


def init_db(dbpath: str, overwrite: bool):
    """
    Creates a new spatialite database.

    :param dbpath: character string. The file path where the new database is to be created.
    :param overwrite: Boolean. Overwrite the database if dbpath already exists.
    """
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


def get_pg_con(user: str, database: str, password: str = None, host: str = 'localhost',
               port: int = 5432) -> psycopg.Connection:
    """
    Connects to a PostgreSQL instance and returns the connection for further use.

    :param user: character string. The database username.
    :param database: character string. The database name.
    :param password: character string. The password for the user.
    :param host: character string. The hostname, DNS name or IP address of the PG instance.
    :param port: integer. The port to connect through.
    :return: A psycopg2 connection object.
    """
    con = psycopg.connect(user=user, password=password, host=host, port=port, dbname=database,
                          row_factory=psycopg.rows.dict_row)
    return con


def get_sqlite_con(dbpath: str, geo: bool = False) -> sqlite.Connection:
    """
    Connects to a SQLite database and returns the connection for further use.

    :param dbpath: character string. The file path to an existing SQLite database.
    :param geo: Boolean. Is the SQLite database also a Spatialite database.
    :return: A sqlite3 connection object.
    """
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    con.execute("PRAGMA foreign_keys = ON;")
    if geo:
        con.enable_load_extension(True)
        con.execute("SELECT load_extension('mod_spatialite')")
    return con


def create_tables(con: Union[sqlite.Connection, psycopg.Connection], wipe: bool, geo: bool, con_type: str,
                  verbose: bool = False):
    """
    Creates the tables if they do not exist in the database.

    :param con: Either a sqlite3 or a psycopg2 connection object.
    :param wipe: Boolean. Should the database be wiped clean of existing data?
    :param geo: Boolean. Should the database contain geometry data harvested from EXIF metadata?
    :param con_type: character string. Either 'sqlite' or 'postgres'. This tells the function which connection type
    it is using.
    :param verbose: Boolean. Should the function print out each sql statement before executing it (for debugging)?
    """
    c = con.cursor()

    # tables
    if con_type == 'sqlite':
        sql_list = [
            '\n'.join((
                'CREATE TABLE IF NOT EXISTS import (',
                '   import_date DATETIME PRIMARY KEY, base_path TEXT, local BOOLEAN, type TEXT);')),
            'CREATE TABLE IF NOT EXISTS hash (md5hash TEXT PRIMARY KEY);',
            '\n'.join((
                'CREATE TABLE IF NOT EXISTS photo (',
                '   path TEXT PRIMARY KEY, fname TEXT, ftype TEXT, md5hash TEXT,',
                '   dt_orig DATETIME, dt_mod DATETIME, dt_import DATETIME,',
                '   FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE,',
                '   FOREIGN KEY (dt_import) REFERENCES import(import_date) ON DELETE CASCADE ON UPDATE CASCADE);')),
            '\n'.join((
                'CREATE TABLE IF NOT EXISTS tag (',
                '   md5hash TEXT, meta JSON, PRIMARY KEY (md5hash),',
                '   FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE);'))
        ]
        for sql in sql_list:
            if verbose:
                print(sql)
            con.execute(sql)

        # geometry
        if geo:
            geo_sql = '\n'.join((
                'CREATE TABLE IF NOT EXISTS location (',
                '   md5hash TEXT PRIMARY KEY, fname TEXT, path TEXT, lat NUMERIC, long NUMERIC, elev_m NUMERIC,',
                '   FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE);'))
            if verbose:
                print(geo_sql)
            con.execute(geo_sql)
            con.execute("SELECT load_extension('mod_spatialite')")
            rows = c.execute("PRAGMA table_info('location');")
            headers = []
            for row in rows:
                headers.append(row[1])
            if 'geometry' not in headers:
                c.execute("SELECT AddGeometryColumn('location', 'geometry', 4326, 'POINTZ', 'XYZ');")
    elif con_type == 'postgres':
        # tables
        sql_list = [
            '\n'.join((
                'CREATE TABLE IF NOT EXISTS import (',
                '   import_date TIMESTAMP WITH TIME ZONE PRIMARY KEY,',
                '   base_path TEXT, local BOOLEAN, type VARCHAR);')),
            'CREATE TABLE IF NOT EXISTS hash (md5hash UUID PRIMARY KEY);',
            '\n'.join((
                'CREATE TABLE IF NOT EXISTS photo (',
                '   path VARCHAR PRIMARY KEY, fname VARCHAR, ftype VARCHAR, md5hash UUID,',
                '   dt_orig TIMESTAMP WITH TIME ZONE, dt_mod TIMESTAMP, dt_import TIMESTAMP WITH TIME ZONE,',
                '   FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE,',
                '   FOREIGN KEY (dt_import) REFERENCES import(import_date) ON DELETE CASCADE ON UPDATE CASCADE);')),
            '\n'.join((
                'CREATE TABLE IF NOT EXISTS tag (',
                '   md5hash UUID, meta JSONB, PRIMARY KEY (md5hash),',
                '   FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE);'))
        ]
        for sql in sql_list:
            if verbose:
                print(sql)
            c.execute(sql)
        # geometry
        if geo:
            geo_list = [
                'CREATE EXTENSION IF NOT EXISTS postgis;',
                '\n'.join((
                    'CREATE TABLE IF NOT EXISTS location (md5hash UUID PRIMARY KEY, fname VARCHAR, path VARCHAR,',
                    '   lat DOUBLE PRECISION, long DOUBLE PRECISION, elev_m DOUBLE PRECISION, geom GEOMETRY,',
                    '   FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE);')),
                "CREATE INDEX IF NOT EXISTS location_geom_gix ON location USING gist(geom);"
            ]
            for gsql in geo_list:
                if verbose:
                    print(gsql)
                c.execute(gsql)
    else:
        raise ValueError("con_type must be either 'postgres' or 'sqlite'.")
    if wipe:
        c.execute('DELETE FROM tag;')
        c.execute('DELETE FROM photo;')
        c.execute('DELETE FROM location;')
        c.execute('DELETE FROM hash;')
        c.execute('DELETE FROM import;')

    # indices
    c.execute("CREATE INDEX IF NOT EXISTS photo_md5hash_idx ON photo (md5hash);")
    # c.execute("CREATE INDEX IF NOT EXISTS tag_value_idx ON tag (value);")
    con.commit()


if __name__ == "__main__":
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
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='overwrite an existing database given with --dbpath')
    parser.add_argument('-w', '--wipe', action='store_true',
                        help='Blank an existing database.')
    parser.add_argument('-g', '--geo', action='store_true',
                        help='store lat/long data in EXIF metadata in a geometry enabled table. Requires that the '
                             'SpatiaLite extension module be loadable.')
    parser.add_argument('-z', '--timezone',
                        help='a timezone available from pytz.all_timezones used to localize EXIF DateTimeOriginal. '
                             'A list of available timezones can also be found at '
                             'https://en.wikipedia.org/wiki/List_of_tz_database_time_zones')

    args = parser.parse_args()

    if args.dbpath:
        ctype = 'sqlite'
        if args.geo:
            init_db(dbpath=args.dbpath, overwrite=args.overwrite)
        conn = get_sqlite_con(dbpath=args.dbpath, geo=args.geo)
    else:
        ctype = 'postgres'
        if args.passwd is None and not args.noask:
            args.passwd = getpass()
        conn = get_pg_con(user=args.user, database=args.db, password=args.passwd, host=args.host, port=args.port)
    if not args.update:
        create_tables(con=conn, wipe=args.wipe, geo=args.geo, con_type=ctype)
    print("Script finished.")