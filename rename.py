#!/usr/bin/env python3
import argparse
import sqlite3 as sqlite
import os
import re
import csv
import sys
from time import sleep
from datetime import datetime
from pathlib import Path


def rename_files(path, out_format=None, whitespace=None, level=None, match=None, expand=None, date_frmt='%Y%m%d_%H%M%S',
                 split_dirs=None):
    new = []
    conn = sqlite.connect(path)
    conn.row_factory = sqlite.Row
    c = conn.cursor()
    sql = ("SELECT a.*,"
           "       replace(substr(b.value, 1, instr(b.value, ' ')-1), ':', '-') ||"
           "       ' ' || substr(b.value, instr(b.value, ' ')+1) dt_orig"
           "  FROM photo a"
           "  LEFT JOIN tag b ON a.md5hash = b.md5hash"
           " WHERE b.tag = 'EXIF DateTimeOriginal'"
           " ORDER BY a.path;")
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

        # process full path
        vd['new_path'] = '/'.join((vd['new_dir'], vd['new_name']))
        if whitespace is not None:
            vd['new_path'] = re.sub(r'\s+', whitespace, vd['new_path'])
        new.append(vd)
        i += 1

    conn.close()
    return new


def write_test(out_path, out_dict):
    with open(out_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, dialect='excel', fieldnames=list(out_dict[0].keys()))
        writer.writeheader()
        for name in out_dict:
            writer.writerow(name)


def write_new(path, out_dict, base=''):
    conn = sqlite.connect(path)
    conn.row_factory = sqlite.Row
    c = conn.cursor()
    c.execute("DROP TABLE IF EXISTS rename;")
    c.execute("CREATE TABLE rename (md5hash TEXT, new_path TEXT, old_path TEXT, old_name TEXT);")
    isql = ("INSERT INTO rename (md5hash, new_path, old_path, old_name) VALUES "
            "(:md5hash, :new_path, :old_path, :old_name);")
    c.executemany(isql, out_dict)
    conn.commit()

    # deal with duplicate paths
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
    max_rn_sql = os.linesep.join((dup_base_sql, "SELECT max(rn) n FROM row_ordered;"))
    rn_sql = os.linesep.join((dup_base_sql, "SELECT * FROM row_ordered;"))
    max_n = c.execute(max_rn_sql).fetchone()['n']
    digits = len(str(max_n))
    rows = c.execute(rn_sql).fetchall()
    u = conn.cursor()
    for row in rows:
        new_path = row['new_path']
        rn = str(row['rn']).rjust(digits, '0')
        altered_path = ''.join((os.path.splitext(new_path)[0], '-', rn, os.path.splitext(new_path)[1]))
        u.execute('UPDATE rename SET new_path = ? WHERE md5hash = ? AND new_path = ?;',
                  (altered_path, row['md5hash'], new_path))
    conn.commit()

    # rename and move
    print('Beginning renaming process...')
    rows = c.execute("SELECT * FROM rename;").fetchall()
    i = 0  # for progressbar
    n = len(rows)  # for progressbar
    for row in rows:
        old_fullpath = os.path.abspath(os.path.join(base, row['old_path']))
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
            conn.commit()

        # progressbar
        # https://stackoverflow.com/questions/3002085/python-to-print-out-status-bar-and-percentage
        j = (i + 1) / n
        sys.stdout.write('\r')
        sys.stdout.write("[%-20s] %d%%" % ('=' * int(20 * j), 100 * j))
        sys.stdout.flush()
        i += 1
    conn.close()


def remove_empty_dir(path):
    try:
        os.rmdir(path)
    except OSError:
        # print('Could not remove', path)
        pass


def remove_file(path):
    try:
        os.remove(path)
    except OSError:
        print('Could not remove', path)
        pass


def delete_empty(path):
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will rename photos according to specific criteria while '
                                                 'and update them in the database.')
    parser.add_argument('dbpath', help='The path of the spatialite database to be created.')
    parser.add_argument('-b', '--base', help='The base path for the photos, in the case that only relative file paths '
                                             'are stored in the db.')
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
                                                      '{isoyear}, {isoweek}, {isoday}, {old_name}, {hash}',
                        default='{old_name}')
    parser.add_argument('-t', '--test', help='A file path in which to export the new file paths/names. Choosing this '
                                             'option will not make any changes to the database or the photo file '
                                             'structure.')
    parser.add_argument('-w', '--whitespace',
                        help='Replace whitespace in the new name with the provided string.')
    parser.add_argument('-T', '--time_subdirs', choices=(['year', 'month']),
                        help='Subdivide base directory obtained from --level by either year or month')
    parser.add_argument('-e', '--delete_empty', help='Delete empty folders after the renaming process.',
                        action='store_true')

    args = parser.parse_args([r'G:\GIS\Photos\trailcam\CA\PhotoMetadata.sqlite', '-b', r'G:\GIS\Photos\trailcam\CA',
                              '-l', '1', '-r', r'([^/\\]+)', '-R', r'\1', '-s', '{regex}_{timestamp}', '-t',
                              r'C:\Users\wlieurance\Documents\temp\rename.csv', '-w', '', '-T', 'year', '-e'])
    args = parser.parse_args()

    new_names = rename_files(path=args.dbpath, level=args.level, match=args.match,
                             whitespace=args.whitespace, expand=args.expand, date_frmt=args.date_format,
                             split_dirs=args.time_subdirs, out_format=args.rename_string)
    if args.test is not None:
        print('Writing test output to', args.test)
        write_test(out_path=args.test, out_dict=new_names)
    else:
        write_new(dbpath=args.dbpath, out_dict=new_names, base=args.base)

    if args.delete_empty and args.base is not None:
        delete_empty(args.base)

    print('Script finished.')
