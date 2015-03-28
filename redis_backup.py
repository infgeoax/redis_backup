"""
Redis RDB backup script.
Written in Python 2.7
"""
# -*- coding: utf-8 -*-
__author__ = 'Luke.The.Coder'

from time import sleep
from datetime import datetime, timedelta

import argparse
import redis
import sys
import os
import shutil
import hashlib


def file_md5(filename, blocksize=2**20):
    f = open(filename)
    md5 = hashlib.md5()
    while True:
        data = f.read(blocksize)
        if not data:
            break
        md5.update(data)
    f.close()
    return md5.digest()


def checksum_compare(src, dst):
    """
    """
    assert(os.path.isfile(src) and os.path.isfile(dst))
    return file_md5(src) == file_md5(dst)


def bgsave_and_wait(r, timeout=timedelta(seconds=60)):
    assert (isinstance(r, redis.StrictRedis))

    bgsave_begin = datetime.now()

    t0 = r.lastsave()
    if r.bgsave():
        while True:
            if r.lastsave() != t0:
                break
            if datetime.now() - bgsave_begin > timeout:
                return 'timeout'
            sleep(1)
        return 'ok'
    else:
        return 'failed'


def rdb_path(r):
    """
    Get&return redis config `dbfilename`
    """
    assert (isinstance(r, redis.StrictRedis))
    d = r.config_get('dir')
    dbfilename = r.config_get('dbfilename')
    return '%s/%s' % (d['dir'], dbfilename['dbfilename'])


def copy_rdb(rdb, backup_dir, backup_filename, port):
    """
    Copies and renames the rdb file to backup dir, compare checksums when finished.
    The final backup name is:
    now().strftime(backup_filename) + "(port_%d)" % port
    
    Returns True when the copy was success and passed the checksum check.
    """
    backup_filename = '%s/%s(port_%d).rdb' % (backup_dir, datetime.now().strftime(backup_filename), port)
    
    if not os.path.exists(backup_dir):
        os.makedirs(backup_dir) 
    elif not os.path.isdir(backup_dir):
        sys.stderr.write('backupdir: %s is not a directory.\n' % backup_dir)
        return False
    elif os.path.exists(backup_filename):
        sys.stderr.write('backupfile: %s already exists.\n' % backup_filename)
        return False

    shutil.copy2(rdb, backup_filename)
    
    if not checksum_compare(rdb, backup_filename):
        sys.stderr.write('failed to copy dbfile %s, checksum compare failed.' % rdb)
        return False
    print 'backup', backup_filename, 'created.', os.path.getsize(backup_filename), 'bytes, checksum ok!'
    return True


def clean_backup_dir(backup_dir, max_backups):
    """
    Removes oldest backups if the total number of backups exceeds max_backups
    """
    files = [f for f in os.listdir(backup_dir) if f.endswith('.rdb')]
    n_files = len(files)
    if n_files > max_backups:
        print 'number of backups(%d) exceeds limit(%d), deleting old backups.' % (n_files, max_backups)

        files_time = []
        for filename in files:
            fp = '%s/%s' % (backup_dir, filename)
            files_time.append((fp, os.path.getmtime(fp)))
        files_time.sort(key=lambda x: x[1])

        for fp in files_time[:n_files - max_backups]:
            print 'delete', fp[0]
            os.remove(fp[0])

        files = os.listdir(backup_dir)
        assert(len(files) == max_backups)


if __name__ == '__main__':
    # Ensures that there is only one instance of this script is running.
    # code from http://stackoverflow.com/questions/380870/python-single-instance-of-program
    # from tendo import singleton
    # me = singleton.SingleInstance()

    # Setup command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-backup_dir', type=str, dest='backup_dir', help='backup directory', default='./backups')
    parser.add_argument('-backup_filename', type=str, dest='backup_filename', help='',
                        default='redis_dump_%Y-%m-%d_%H%M%S')
    parser.add_argument('-redis_port', type=int, dest='redis_port', help='redis port', default=6379)
    parser.add_argument('-max_backups', type=int, dest='max_backups', help='maximum number of backups to keep',
                        default=10)
    parser.add_argument('-bgsave_timeout', type=int, dest='bgsave_timeout', help='bgsave timeout in seconds',
                        default=60)

    # Parse command line arguments
    args = parser.parse_args()

    args.backup_dir = os.path.abspath(args.backup_dir)

    st = datetime.now()

    print 'backup begin @', st
    print 'backup dir:    \t', args.backup_dir
    print 'backup file:   \t', args.backup_filename
    print 'max backups:   \t', args.max_backups
    print 'redis port:    \t', args.redis_port
    print 'bgsave timeout:\t', args.bgsave_timeout, 'seconds'

    # Connect to local redis server
    r = redis.StrictRedis(port=args.redis_port)
    print 'connected to redis server localhost:%d' % args.redis_port

    # Get where redis saves the RDB file
    rdb = rdb_path(r)
    print 'redis rdb file path:', rdb

    # Start bgsave and wait for it to finish
    print 'redis bgsave...',
    sys.stdout.flush()
    ret = bgsave_and_wait(r, timeout=timedelta(seconds=args.bgsave_timeout))
    print ret

    if ret == 'ok':
        if copy_rdb(rdb, args.backup_dir, args.backup_filename, args.redis_port):
            clean_backup_dir(args.backup_dir, args.max_backups)
            print 'backup successful! time cost:', datetime.now() - st
            sys.exit(0)
    sys.stderr.write('%s %s\n' % ('backup failed!', datetime.now() - st))
    sys.exit(1)
