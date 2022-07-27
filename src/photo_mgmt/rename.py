#!/usr/bin/env python3
import argparse
import sqlite3 as sqlite
import psycopg
import os
import re
import csv
import sys
import random
import uuid
from getpass import getpass
from datetime import datetime
from pathlib import Path
from typing import Union
from photo_mgmt.create_db import get_pg_con, get_sqlite_con


def rename_files(con: Union[sqlite.Connection, psycopg.Connection], new_base: str = None, out_format: str = None,
                 whitespace: str = None, level: int = None, match: str = None, expand: str = None,
                 date_frmt: str = '%Y%m%d_%H%M%S', split_dirs: str = None) -> list[dict]:
    """Renames files previously stored in a database using scan_image() and a variety of criteria, and then updates the
    new file names in the database.

    :param con: database connection.
    :param new_base:
    :param out_format: character string. The string representing the new filename with string formatting tags to be
    replaced by actual values. (e.g. {regex}_{timestamp}.jpg).
    :param whitespace: character string. Replace whitespace in the filename with new character(s).
    :param level: integer. The level to flatten the directory structure. (e.g. `0` will remove all  sub-folders, `1`
    will keep first level of sub-folders, etc.
    :param match: character string. A string supplying regex matching and capture groups which is used to extract text
    from the old file path for use in the new name (See re.match).
    :param expand: character string. A string containing the capture groups from the `match` parameter to form a
    new string using python `re` library notation (e.g. \1_example_text_\2.jpg). This parameter is used in the
    `out_format` parameter {regex} tag (See Match.expand in the `re` library).
    :param date_frmt: character string. The format for the {timestamp} tag in the `out_format` parameter using the
    `datetime` library `strftime` format (e.g. %Y%m%d_%H%M%S).
    :param split_dirs: character string. One of [year, month]. Subdivides base output directory derived from the `level`
    parameter further into month or year sub-directories.
    :return: A list of dictionaries containing the newly constructed file path to use in renaming as well as other other
    useful file data.
    """
    new = []
    c = con.cursor()
    sql = '\n'.join((
        "SELECT a.*, b.base_path, b.local",
        "  FROM photo a",
        "  LEFT JOIN import b ON a.dt_import = b.import_date",
        " ORDER BY b.import_date, a.path;"
    ))
    print("Getting records from database...")
    rows = c.execute(sql).fetchall()
    i = 0
    for row in rows:
        # print('row: ', i, ', path: ', row['path'], sep='')
        # get vars
        vd = dict()
        if isinstance(row['md5hash'], uuid.UUID):
            vd['md5hash'] = row['md5hash'].hex
        else:
            vd['md5hash'] = row['md5hash']
        if isinstance(row['dt_orig'], str):
            vd['dt'] = datetime.strptime(row['dt_orig'], '%Y-%m-%d %H:%M:%S')
        else:
            vd['dt'] = row['dt_orig']
        if new_base is not None:
            vd['new_base'] = re.sub('/$', '', re.sub(r'\\', '/', new_base))
        else:
            vd['new_base'] = row['base_path']
        vd['year'] = vd['dt'].year
        vd['month'] = vd['dt'].month
        vd['old_base'] = row['base_path']
        vd['old_path'] = row['path']
        vd['old_local'] = bool(row['local'])
        if vd['old_local']:
            vd['old_dir'] = os.path.dirname(row['path'])
        else:
            vd['old_dir'] = re.sub('^/', '', os.path.dirname(row['path']).replace(row['base_path'], ''))
        vd['old_name'] = row['fname']
        vd['old_dt_orig'] = row['dt_orig']
        vd['timestamp'] = vd['dt'].strftime(date_frmt)
        vd['isoyear'] = vd['dt'].isocalendar()[0]
        vd['isoweek'] = vd['dt'].isocalendar()[1]
        vd['isoday'] = vd['dt'].isocalendar()[2]
        vd['regex'] = None

        # process filename
        if match is not None:
            matches = re.search(match, vd.get('old_path'))
            if matches is not None:
                if expand is not None:
                    vd['regex'] = matches.expand(expand)
                else:
                    vd['regex'] = '_'.join(matches.groups())
        if out_format is not None:
            old_ext = os.path.splitext(vd.get('old_name'))[1]
            new_ext = os.path.splitext(out_format)[1]
            if new_ext:
                out_ext = ''
            else:
                out_ext = old_ext
            vd['new_name'] = ''.join((out_format.format(**vd), out_ext))
        else:
            vd['new_name'] = vd.get('old_name')

        # restructure path
        if level is not None:
            path_split = re.split(r'[/\\]', vd['old_dir'])
            vd['new_dir'] = '/'.join(path_split[0:level])
        else:
            vd['new_dir'] = vd['old_dir']
        if split_dirs is not None:
            if split_dirs == 'year':
                vd['new_dir'] = '/'.join((vd['new_dir'], vd['dt'].strftime('%Y')))
            elif split_dirs == 'month':
                vd['new_dir'] = '/'.join((vd['new_dir'], vd['dt'].strftime('%Y/%b')))
            vd['new_dir'] = re.sub('^/', '', vd['new_dir'])  # removes leading / in case of blank new_dir

        # process full path
        vd['new_path'] = '/'.join((vd['new_dir'], vd['new_name']))
        vd['new_path'] = os.path.normpath(re.sub('^/', '', vd['new_path']))  # rm leading / in case of blank new_path
        vd['new_full_path'] = os.path.normpath('/'.join((vd['new_base'], vd['new_dir'], vd['new_name'])))
        if whitespace is not None:
            vd['new_path'] = re.sub(r'\s+', whitespace, vd['new_path'])
            vd['new_full_path'] = re.sub(r'\s+', whitespace, vd['new_full_path'])
        new.append(vd)
        i += 1
    return new


def write_test(out_path: str, out_dict: list[dict]):
    """
    Writes the results of `rename_files` to a comma delimited file.

    :param out_path: character string. The path to an output file to store the results.
    :param out_dict: A list of dictionaries.
    """
    with open(out_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, dialect='excel', fieldnames=list(out_dict[0].keys()))
        writer.writeheader()
        for name in out_dict:
            writer.writerow(name)


def write_new(con: Union[sqlite.Connection, psycopg.Connection], out_dict: list[dict], local: bool = False):
    """
    Renames files and writes renaming results into the database.

    :param con: A database connection object.
    :param out_dict: A list of dictionaries produced from `rename_files`.
    :param local: Boolean. Whether to store a local path instead of a full path.
    """
    updated = 0
    if isinstance(con, psycopg.Connection):
        ph = '%s'
        nh = '%({})s'
        ignore = ''
        conflict = 'ON CONFLICT DO NOTHING'
        text = 'VARCHAR'
        uid = 'UUID'
    elif isinstance(con, sqlite.Connection):
        ph = '?'
        nh = ':{}'
        ignore = 'OR IGNORE'
        conflict = ''
        text = 'TEXT'
        uid = 'TEXT'
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")
    c = con.cursor()
    c.execute("DROP TABLE IF EXISTS rename;")
    c.execute(f"CREATE TEMP TABLE rename (md5hash {uid}, new_path {text}, new_base {text}, old_path {text}, "
              f"old_base {text}, old_fname {text}, old_local BOOLEAN);")
    isql = '\n'.join((
        "INSERT INTO rename (md5hash, new_path, new_base, old_path, old_base, old_fname, old_local) VALUES ",
        f"    ({nh}, {nh}, {nh}, {nh}, {nh}, {nh}, {nh});")).format('md5hash', 'new_path', 'new_base', 'old_path',
                                                                    'old_base', 'old_name', 'old_local')
    c.executemany(isql, out_dict)
    con.commit()

    # deal with duplicate paths by adding row numbers to the duplicates according to the original filesystem order.
    dup_base_sql = os.linesep.join((
        "WITH dups AS (",
        "SELECT new_path, count(md5hash) n",
        "  FROM rename",
        " GROUP BY new_path",
        "HAVING count(md5hash) > 1",
        "",
        "), row_ordered AS (",
        "SELECT a.*,",
        "       row_number() over(partition by a.new_path order by a.old_path) rn",
        "  FROM rename a",
        " INNER JOIN dups b ON a.new_path = b.new_path",
        ")",
        "",
    ))
    # get the max row number for 0 padding purposes so files sort correctly in the filesystem
    max_rn_sql = os.linesep.join((dup_base_sql, "SELECT max(rn) n FROM row_ordered;"))
    max_n = c.execute(max_rn_sql).fetchone()['n']
    digits = len(str(max_n))

    rn_sql = os.linesep.join((dup_base_sql, "SELECT * FROM row_ordered;"))
    rows = c.execute(rn_sql).fetchall()
    u = con.cursor()
    for row in rows:
        new_path = row['new_path']
        rn = str(row['rn']).rjust(digits, '0')
        altered_path = ''.join((os.path.splitext(new_path)[0], '-', rn, os.path.splitext(new_path)[1]))
        u.execute(f'UPDATE rename SET new_path = {ph} WHERE md5hash = {ph} AND new_path = {ph};',
                  (altered_path, row['md5hash'], new_path))
    con.commit()

    # post-duplicate check, do move/rename and database update
    print('Beginning renaming process...')
    new_bases = set([x.get('new_base') for x in out_dict])
    for base_path in new_bases:
        import_date = datetime.utcnow().isoformat(sep='T', timespec='seconds') + 'Z'
        c.execute(f"INSERT {ignore} INTO import (import_date, base_path, local, type) VALUES ({ph},{ph},{ph}, "
                  f"'rename') {conflict};",
                  (import_date, base_path, local))
        con.commit()
        rows = c.execute(f"SELECT * FROM rename WHERE new_base = {ph};", (base_path,)).fetchall()
        i = 0  # for progressbar
        n = len(rows)  # for progressbar
        for row in rows:
            if row['old_local']:
                old_fullpath = os.path.abspath(os.path.join(row['old_base'], row['old_path']))
            else:
                old_fullpath = os.path.abspath(row['old_path'])
            new_fullpath = os.path.abspath(os.path.join(base_path, row['new_path']))
            # print('Renaming', old_fullpath, 'to', new_fullpath)
            try:
                Path(os.path.dirname(new_fullpath)).mkdir(parents=True, exist_ok=True)
                Path(old_fullpath).rename(os.path.abspath(new_fullpath))
            except OSError:
                print('Cannot rename', old_fullpath, 'to', new_fullpath)
            else:
                if local:
                    final_path = row['new_path']
                else:
                    final_path = '/'.join((row['new_base'], row['new_path']))
                u.execute(f'UPDATE photo SET path = {ph}, fname = {ph}, dt_import = {ph} '
                          f'WHERE md5hash = {ph} AND path = {ph};',
                          (final_path, os.path.basename(row['new_path']), import_date,
                           row['md5hash'], row['old_path']))
                updated += u.rowcount
                con.commit()
            # progressbar
            # https://stackoverflow.com/questions/3002085/python-to-print-out-status-bar-and-percentage
            j = (i + 1) / n
            sys.stdout.write('\r')
            sys.stdout.write("[%-20s] %d%%" % ('=' * int(20 * j), 100 * j))
            sys.stdout.flush()
            i += 1
        if updated == 0:
            c.execute(f"DELETE FROM import WHERE import_date = {ph};")
        print(os.linesep)
        print(updated, "records updated in", base_path)


def remove_empty_dir(path: str):
    """
    Removes empty directories.

    :param path: character string. The directory path to attempt to remove.
    """
    try:
        os.rmdir(path)
    except OSError:
        # print('Could not remove', path)
        pass


def remove_file(path: str):
    """
    Attempts to delete a file.

    :param path: character string. The file path of the file to remove.
    """
    try:
        os.remove(path)
    except OSError:
        print('Could not remove', path)
        pass


def delete_empty(path: str):
    """
    Scans a path for empty directories or those with only a thumbnail database, then removes that directory.

    :param path: character string. The path to scan for empty directories.
    """
    # get rid of lone thumbs.db
    for root, dirs, files in os.walk(path, topdown=False):
        # print(root)
        if len(files) == 1:
            if files[0].lower() == 'thumbs.db':
                filepath = os.path.join(root, files[0])
                print('Deleting:', filepath)
                remove_file(filepath)
    # remove empty dirs
    for root, dirs, files in os.walk(path, topdown=False):
        for d in dirs:
            dirpath = os.path.realpath(os.path.join(root, d))
            # print(dirpath)
            remove_empty_dir(dirpath)


def confirm_write(out_dict: list[dict]) -> bool:
    """
    Acts as a sanity check for interactive execution to print out renamed examples and ask for confirmation.

    :param out_dict: A list of dictionaries. The return of `rename_files`.
    :return: Boolean. True is confirmation to continue.
    """
    print("### RENAMED EXAMPLES ###\n")
    print_list = random.sample(out_dict, 5)
    for e in print_list:
        print('old dbpath:', e['old_path'].replace('/', os.path.sep))
        print('new dbpath:', e['new_path'].replace('/', os.path.sep))
        if e['old_local']:
            print('old file path:', os.path.join(e['old_base'], e['old_path']).replace('/', os.path.sep))
        else:
            print('old file path:', e['old_path'].replace('/', os.path.sep))
        print('new file path:', e['new_full_path'].replace('/', os.path.sep))
        print(os.linesep)
    ask = input("Are you sure you want to rename/move files with these changes? (y/n): ")
    if ask:
        if ask[0].lower() == 'y':
            go = True
        else:
            go = False
    else:
        go = False
    return go


def test_check(con: Union[sqlite.Connection, psycopg.Connection], dict_list: list[dict],
               test: str = None, rm_empty: bool = False):
    """
    Initializes the writing functions, i.e. checking for test flags, getting confirmation, and writing results.

    :param con: A database connection object.
    :param dict_list: The renaming dictionary produced by the rename_files function.
    :param test: Either None (don't dry run), a path to an output file (write to delimited file), or 'stdout'
    (print the dry run).
    :param rm_empty: Remove empty directories from the paths where files were renamed from.
    """
    if test is None:
        go = confirm_write(out_dict=dict_list)
        if go:
            write_new(con=con, out_dict=dict_list)
            if rm_empty:
                check_paths = set([x.get('old_base') for x in dict_list])
                for old_base in check_paths:
                    delete_empty(old_base)
        else:
            print('Skipping rename/move and database update.')
    else:
        if test != 'stdout':
            print('Writing test output to', args.test)
            write_test(out_path=test, out_dict=dict_list)
        else:
            for d in dict_list:
                print(os.path.join(d['old_base'], d['old_path']), '-->', os.path.join(d['new_base'], d['new_path']))
                for k, v in d.items():
                    if k not in ['old_base', 'old_path']:
                        print('\t', k, ": ", v, sep="")
                print("")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will rename photos according to specific criteria '
                                                 'and update them in the database.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--dbpath',
                       help='the path of the spatialite database to use.')
    group.add_argument('--db', help='the PostgreSQL database to which to connect.')
    args_pg = parser.add_argument_group('PostgreSQL')
    args_pg.add_argument('--host', default='localhost')
    args_pg.add_argument('--user', default='postgres')
    args_pg.add_argument('--port', default=5432, type=int)
    args_pg.add_argument('--passwd', help="Password for user.")
    args_pg.add_argument('--noask', action='store_true',
                         help="User will not be prompted for password if none given.")
    parser.add_argument('-b', '--base', help='The new base directory where the renamed files will be stored.')
    parser.add_argument('-l', '--level', help='The level to flatten the directory structure. 0 will remove all '
                                              'sub-folders, 1 will keep first level of sub-folders, etc.',
                        type=int)
    parser.add_argument('-r', '--match', help='Use regex string matching and capture groups to extract text from '
                                              'the old path for the new name '
                                              '[https://docs.python.org/3/library/re.html, re.match()].')
    parser.add_argument('-R', '--expand', help=r'The replacement string which utilizes the capture groups from '
                                               r'the --match argument to form a new string using \1, \2, etc to refer '
                                               r'to capture groups '
                                               r'[https://docs.python.org/3/library/re.html, Match.expand()].')
    parser.add_argument('-d', '--date_format', help='The format for the {timestamp} tag (https://strftime.org/).',
                        default='%Y%m%d_%H%M%S')
    parser.add_argument('-s', '--rename_string', help='The format for the new filename. Users can use the following '
                                                      'tags in the string: {regex} {timestamp}, {year}, {month}, '
                                                      '{isoyear}, {isoweek}, {isoday}, {old_name}, {md5hash}')
    parser.add_argument('-t', '--test', nargs='?', const='stdout',
                        help='Print the new filenames instead of writing the changes to the database. A file path can '
                             'optionally be provided to store the results. Choosing this option will not '
                             'make any changes to the database or the photo file structure.')
    parser.add_argument('-w', '--whitespace',
                        help='Replace whitespace in the new name with the provided string.')
    parser.add_argument('-T', '--time_subdirs', choices=(['year', 'month']),
                        help='Subdivide base directory obtained from --level by either year or month')
    parser.add_argument('-e', '--delete_empty', help='Delete empty folders after the renaming process.',
                        action='store_true')
    parser.add_argument('-p', '--local', action='store_true',
                        help='store the local path from the scan directory instead of the full path')

    args = parser.parse_args()

    if args.dbpath:
        conn = get_sqlite_con(dbpath=args.dbpath, geo=False)
    else:
        if args.passwd is None and not args.noask:
            args.passwd = getpass()
        conn = get_pg_con(user=args.user, database=args.db, password=args.passwd, host=args.host, port=args.port)

    new_names = rename_files(con=conn, level=args.level, match=args.match, new_base=args.base,
                             whitespace=args.whitespace, expand=args.expand, date_frmt=args.date_format,
                             split_dirs=args.time_subdirs, out_format=args.rename_string)
    test_check(con=conn, dict_list=new_names, test=args.test, rm_empty=args.delete_empty)
    conn.close()
    conn = None

    print('Script finished.')
