#!/usr/bin/env python3
'''
pyhttpfs.py - use pyfuse3 to bind a simple http server to the filesystem
'''

import errno
import faulthandler
import httpx
import json
import logging
import os
import pyfuse3
import stat as stat_m
import sys
import trio

from argparse import ArgumentParser
from collections import defaultdict
from tempfile import TemporaryFile 

from .types import File, Timestamp

faulthandler.enable()
log = logging.getLogger(__name__)

class HttpFs(pyfuse3.Operations):

    enable_writeback_cache = True

    def __init__(self, source):
        super(HttpFs, self).__init__()

        self._url = source

        mode = stat_m.S_IREAD | stat_m.S_IFDIR | stat_m.S_IRGRP | stat_m.S_IROTH
        self.root = File(path='/', inode=pyfuse3.ROOT_INODE, mode=mode, size=4096,
                         timestamp=Timestamp())
        self._inode_to_file_map = {  pyfuse3.ROOT_INODE: self.root }
        self._inode_to_tmpfile_map = {}
        self._fd_inode_map = dict()
        self._fd_open_count = dict()

        # Okay blocking for init
        trio.run(self._load_children,self.root)

    async def async_get_json(self, url):
        async with httpx.AsyncClient() as client:
            return (await client.get(url, follow_redirects=True)).json()

    async def lookup(self, inode_p, name, ctx=None):
        name = os.fsdecode(name)
        log.debug('lookup for %s in %d', name, inode_p)

        inode = None
        try:
            if name == '.':
                inode = inode_p
            elif name == '..':
                inode = self._inode_to_file_map[inode_p]['parent'].l_inode()
            else:
                for c in self._inode_to_file_map[inode_p]['children']:
                    if name == c.basename():
                        inode = c.l_inode()
                        break

        except KeyError as e:
            raise(pyfuse3.FUSEError(errno.ENOENT))

        if inode == None:
            raise(pyfuse3.FUSEError(errno.ENOENT))

        return await self.getattr(inode, ctx)

    async def _load_children(self, pobj):
        log.info(f"Loading children for {pobj.full_path()}")
        url = self._url + pobj.full_path()

        for f in await self.async_get_json(url):
            c = File.from_json(f)
            c['parent'] = pobj
            pobj.push_child(File.from_json(f))
            self._inode_to_file_map[c.l_inode()] = c

    async def getattr(self, inode, ctx=None):
        return self._getattr(self._inode_to_file_map[inode])

    def _getattr(self, fobj):
        stat = fobj.stat()
        entry = pyfuse3.EntryAttributes()
        for attr in ('st_ino', 'st_mode', 'st_nlink', 'st_uid', 'st_gid',
                     'st_rdev', 'st_size', 'st_atime_ns', 'st_mtime_ns',
                     'st_ctime_ns'):
            setattr(entry, attr, getattr(stat, attr))
        entry.generation = 0
        entry.entry_timeout = 0
        entry.attr_timeout = 0
        entry.st_blksize = 512
        entry.st_blocks = ((entry.st_size+entry.st_blksize-1) // entry.st_blksize)
        return entry

    async def readlink(self, inode, ctx):
        log.error('readlink not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def opendir(self, inode, ctx):
        return inode

    async def readdir(self, inode, off, token):
        fobj = self._inode_to_file_map[inode]
        if not fobj['walked']:
            await self._load_children(fobj)

        path = fobj.basename()
        log.debug('reading %s', path)
        entries = []
        for c in fobj['children']:
            if c.basename() == '.' or c.basename() == '..':
                continue
            attr = self._getattr(c)
            entries.append((attr.st_ino, c.basename(), attr))

        log.debug('read %d entries, starting at %d', len(entries), off)

        for (ino, name, attr) in sorted(entries):
            if ino <= off:
                continue
            if not pyfuse3.readdir_reply(
                token, os.fsencode(name), attr, ino):
                break

    async def unlink(self, inode_p, name, ctx):
        log.error('unlink not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def rmdir(self, inode_p, name, ctx):
        log.error('rmdir not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def symlink(self, inode_p, name, target, ctx):
        log.error('symlink not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def rename(self, inode_p_old, name_old, inode_p_new, name_new,
                     flags, ctx):
        log.error('rename not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def link(self, inode, new_inode_p, new_name, ctx):
        log.error('link not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def setattr(self, inode, attr, fields, fh, ctx):
        log.error('setattr not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def mknod(self, inode_p, name, mode, rdev, ctx):
        log.error('mknod not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def mkdir(self, inode_p, name, mode, ctx):
        log.error('mkdir not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def statfs(self, ctx):
        log.error('statfs not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def open(self, inode, flags, ctx):
        if inode in self._inode_to_tmpfile_map.keys():
            fd = self._inode_to_tmpfile_map[inode]
            self._fd_open_count[fd.fileno()] += 1
            return pyfuse3.FileInfo(fh=fd.fileno())
        assert flags & os.O_CREAT == 0

        uri = self._url + self._inode_to_file_map[inode].full_path()
        fd = TemporaryFile('w+b')

        async with httpx.AsyncClient() as client:
            async with client.stream('GET', uri) as response:
                async for chunk in response.aiter_bytes():
                    fd.write(chunk)

        fd.seek(0)
        self._inode_to_tmpfile_map[inode] = fd
        self._fd_inode_map[fd.fileno()] = inode
        self._fd_open_count[fd.fileno()] = 1
        return pyfuse3.FileInfo(fh=fd.fileno())

    async def create(self, inode_p, name, mode, flags, ctx):
        log.error('create not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def read(self, fd, offset, length):
        os.lseek(fd, offset, os.SEEK_SET)
        return os.read(fd, length)

    async def write(self, fd, offset, buf):
        log.error('write not supported')
        raise pyfuse3.FUSEError(errno.EINVAL)

    async def release(self, fd):
        if self._fd_open_count[fd] > 1:
            self._fd_open_count[fd] -= 1
            return

        del self._fd_open_count[fd]

        inode = self._fd_inode_map[fd]
        self._inode_to_tmpfile_map[inode].close()
        del self._inode_to_tmpfile_map[inode]
        del self._fd_inode_map[fd]

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


def parse_args(args):
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('source', type=str,
                        help='URL where PyHTTPfs server is running')
    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')

    return parser.parse_args(args)

def main():
    options = parse_args(sys.argv[1:])
    init_logging(options.debug)
    httpfs = HttpFs(options.source)

    log.debug('Mounting...')
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=pyhttpfs,ro,noexec')
    if options.debug_fuse:
        fuse_options.add('debug')

    pyfuse3.init(httpfs, options.mountpoint, fuse_options)

    try:
        log.debug('Entering main loop..')
        trio.run(pyfuse3.main)
    except:
        pyfuse3.close(unmount=True)
        raise

    log.debug('Unmounting..')
    pyfuse3.close()

if __name__ == '__main__':
    main()

