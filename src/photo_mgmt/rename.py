#!/usr/bin/env python3
import argparse
import sqlite3 as sqlite
import os
import re
import csv
import sys
import random
from datetime import datetime
from pathlib import Path


def rename_files(dbpath, out_format=None, whitespace=None, level=None, match=None, expand=None,
                 date_frmt='%Y%m%d_%H%M%S', split_dirs=None):
    """Renames files previously stored in a database using scan_image() and a variety of criteria, and then updates the
    new file names in the database.

    :param dbpath: character string. The file path to the sqlite database.
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
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    c = con.cursor()
    sql = '\n'.join((
        "SELECT a.*,",
        "       replace(substr(b.value, 1, instr(b.value, ' ')-1), ':', '-') ||",
        "       ' ' || substr(b.value, instr(b.value, ' ')+1) dt_orig,",
        "       d.base_path",
        "  FROM photo a",
        "  LEFT JOIN tag b ON a.md5hash = b.md5hash",
        "  LEFT JOIN hash c ON a.md5hash = c.md5hash",
        "  LEFT JOIN import d ON c.import_date = d.import_date",
        " WHERE b.tag = 'EXIF DateTimeOriginal'",
        " ORDER BY a.path;"
    ))
    print("Getting records from database...")
    rows = c.execute(sql).fetchall()
    i = 0
    for row in rows:
        # print('row: ', i, ', path: ', row['path'], sep='')
        # get vars
        vd = dict()
        vd['md5hash'] = row['md5hash']
        vd['dt'] = datetime.strptime(row['dt_orig'], '%Y-%m-%d %H:%M:%S')
        vd['year'] = vd['dt'].year
        vd['month'] = vd['dt'].month
        vd['old_base'] = row['base_path']
        vd['old_path'] = row['path']
        vd['old_dir'] = os.path.dirname(row['path'])
        vd['old_name'] = row['fname']
        vd['old_dt_orig'] = row['dt_orig']
        vd['timestamp'] = vd['dt'].strftime(date_frmt)
        vd['isoyear'] = vd['dt'].isocalendar()[0]
        vd['isoweek'] = vd['dt'].isocalendar()[1]
        vd['isoday'] = vd['dt'].isocalendar()[2]
        vd['regex'] = None

        # process filename
        if match is not None:
            matches = re.match(match, vd['old_path'])
            if matches is not None:
                if expand is not None:
                    vd['regex'] = matches.expand(expand)
                else:
                    vd['regex'] = '_'.join(matches.groups())
        if out_format is not None:
            old_ext = os.path.splitext(vd['old_name'])[1]
            vd['new_name'] = ''.join((out_format.format(**vd), old_ext.lower()))
        else:
            vd['new_name'] = vd['old_name']

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
        vd['new_path'] = re.sub('^/', '', vd['new_path'])  # removes leading / in case of blank new_path
        if whitespace is not None:
            vd['new_path'] = re.sub(r'\s+', whitespace, vd['new_path'])
        new.append(vd)
        i += 1
    con.close()
    return new


def write_test(out_path, out_dict):
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


def write_new(dbpath, out_dict, base=''):
    """
    Renames files and writes renaming results into the database.

    :param dbpath: character string. The path to the photo database.
    :param out_dict: A list of dictionaries produced from `rename_files`.
    :param base: character string. The base path for the photo paths in the case that relative paths are stored.
    """
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    c = con.cursor()
    c.execute("DROP TABLE IF EXISTS rename;")
    c.execute("CREATE TEMP TABLE rename (md5hash TEXT, new_path TEXT, old_base TEXT, old_path TEXT, old_fname TEXT);")
    isql = ("INSERT INTO rename (md5hash, old_base, new_path, old_path, old_fname) VALUES "
            "(:md5hash, :old_base, :new_path, :old_path, :old_name);")
    c.executemany(isql, out_dict)
    con.commit()

    # deal with duplicate paths by adding row numbers to the duplicates according to the original filesystem order.
    dup_base_sql = os.linesep.join((
        "WITH dups AS (",
        "SELECT new_path, count(md5hash) n",
        "  FROM rename",
        " GROUP BY new_path",
        "HAVING n > 1",
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
        u.execute('UPDATE rename SET new_path = ? WHERE md5hash = ? AND new_path = ?;',
                  (altered_path, row['md5hash'], new_path))
    con.commit()

    # post-duplicate check, do move/rename and database update
    print('Beginning renaming process...')
    import_date = datetime.utcnow().isoformat(sep='T', timespec='seconds') + 'Z'
    if base == '':
        base_path = None
        local = False
    else:
        base_path = re.sub(r'\\', '/', base)
        local = True
    c.execute("INSERT OR IGNORE INTO import (import_date, base_path, local, type) VALUES (?,?,?, 'rename')",
              (import_date, base_path, local))
    con.commit()
    rows = c.execute("SELECT * FROM rename;").fetchall()
    i = 0  # for progressbar
    n = len(rows)  # for progressbar
    for row in rows:
        old_fullpath = os.path.abspath(os.path.join(row['old_base'], row['old_path']))
        new_fullpath = os.path.abspath(os.path.join(base, row['new_path']))
        # print('Renaming', old_fullpath, 'to', new_fullpath)
        try:
            Path(os.path.dirname(new_fullpath)).mkdir(parents=True, exist_ok=True)
            Path(old_fullpath).rename(os.path.abspath(new_fullpath))
        except:
            print('Cannot rename', old_fullpath, 'to', new_fullpath)
        else:
            u.execute('UPDATE photo SET path = ?, fname = ? WHERE md5hash = ? AND path = ?;',
                      (row['new_path'], os.path.basename(row['new_path']), row['md5hash'], row['old_path']))
            u.execute('UPDATE hash SET import_date = ? WHERE md5hash = ?;',
                      (import_date, row['md5hash']))
            con.commit()

        # progressbar
        # https://stackoverflow.com/questions/3002085/python-to-print-out-status-bar-and-percentage
        j = (i + 1) / n
        sys.stdout.write('\r')
        sys.stdout.write("[%-20s] %d%%" % ('=' * int(20 * j), 100 * j))
        sys.stdout.flush()
        i += 1
    print(os.linesep)
    con.close()


def remove_empty_dir(path):
    """
    Removes empty directories.

    :param path: character string. The directory path to attempt to remove.
    """
    try:
        os.rmdir(path)
    except OSError:
        # print('Could not remove', path)
        pass


def remove_file(path):
    """
    Attempts to delete a file.

    :param path: character string. The file path of the file to remove.
    """
    try:
        os.remove(path)
    except OSError:
        print('Could not remove', path)
        pass


def delete_empty(path):
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


def confirm_write(out_dict, new_base=None):
    """
    Acts as a sanity check for interactive execution to print out renamed examples and ask for confirmation.

    :param out_dict: A list of dictionaries. The return of `rename_files`.
    :param new_base: a character string. The base directory for the photos stored in the database if relative paths
    stored.
    :return: Boolean. True is confirmation to continue.
    """
    print("### RENAMED EXAMPLES ###\n")
    print_list = random.sample(out_dict, 5)
    for e in print_list:
        print('old dbpath:', e['old_path'].replace('/', os.path.sep))
        print('new dbpath:', e['new_path'].replace('/', os.path.sep))
        if e['old_base']:
            print('old file path:', os.path.join(e['old_base'], e['old_path']).replace('/', os.path.sep))
        if new_base:
            print('new file path:', os.path.join(new_base, e['new_path']).replace('/', os.path.sep))
        else:
            print('new file path:', e['new_path'].replace('/', os.path.sep))
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will rename photos according to specific criteria '
                                                 'and update them in the database.')
    parser.add_argument('dbpath', help='The path of the spatialite database to be created with the scan_photos module.')
    parser.add_argument('-b', '--base', help='The base path for the photos, in the case that only relative file paths '
                                             'are to be stored in the db.')
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
    parser.add_argument('-t', '--test', help='A file path in which to export the new file paths/names. Choosing this '
                                             'option will not make any changes to the database or the photo file '
                                             'structure.')
    parser.add_argument('-w', '--whitespace',
                        help='Replace whitespace in the new name with the provided string.')
    parser.add_argument('-T', '--time_subdirs', choices=(['year', 'month']),
                        help='Subdivide base directory obtained from --level by either year or month')
    parser.add_argument('-e', '--delete_empty', help='Delete empty folders after the renaming process.',
                        action='store_true')

    args = parser.parse_args()

    new_names = rename_files(dbpath=args.dbpath, level=args.level, match=args.match,
                             whitespace=args.whitespace, expand=args.expand, date_frmt=args.date_format,
                             split_dirs=args.time_subdirs, out_format=args.rename_string)

    if args.test is None:
        go = confirm_write(out_dict=new_names, new_base=args.base)
        if go:
            write_new(dbpath=args.dbpath, out_dict=new_names, base=args.base)
            if args.delete_empty and args.base is not None:
                delete_empty(args.base)
        else:
            print('Skipping rename/move and database update.')
    else:
        print('Writing test output to', args.test)
        write_test(out_path=args.test, out_dict=new_names)

    print('Script finished.')
