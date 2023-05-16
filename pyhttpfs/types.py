
import os
import stat

from datetime import datetime

class Timestamp(dict):
    def __init__(self, mtim_sec=0,
                       mtim_usec=0,
                       atim_sec=0,
                       atim_usec=0,
                       ctim_sec=0,
                       ctim_usec=0):
        
        self.mtime = datetime.fromtimestamp(mtim_sec + (mtim_usec / 1000000000))
        self.atime = datetime.fromtimestamp(atim_sec + (atim_usec / 1000000000))
        self.ctime = datetime.fromtimestamp(ctim_sec + (ctim_usec / 1000000000))

        dict.__init__(self, mtime = self.mtime.isoformat(),
                            atime = self.atime.isoformat(),
                            ctime = self.ctime.isoformat())

    @classmethod
    def from_json(cls, json_data):
        return cls(mtim_sec=datetime.fromisoformat(json_data['mtime']).timestamp(),
                   atim_sec=datetime.fromisoformat(json_data['atime']).timestamp(),
                   ctim_sec=datetime.fromisoformat(json_data['ctime']).timestamp())

    def __str__(self):
        return str(self.mtime)

    def __repr__(self):
        return "%s %s %s" % (str(self.mtime), str(self.atime), str(self.ctime))

    def mtime_ns(self):
        return Timestamp._to_ns(self.mtime)

    def atime_ns(self):
        return Timestamp._to_ns(self.atime)

    def ctime_ns(self):
        return Timestamp._to_ns(self.ctime)

    @classmethod
    def _to_ns(cls, ts):
        if not type(ts) == datetime:
            raise TypeError()
        return ts.timestamp() * 1000000000

class File(dict):
    def __init__(self, path,
                       dev=1, # non-zero for pseudo-local-inode calc
                       inode=0,
                       mode=0,
                       nlink=0,
                       uid=0,
                       gid=0,
                       size=0,
                       timestamp=None,
                       children=[],
                       parent=None,
                       walked=False):

        dict.__init__(self, 
                      st_path   = path,
                      st_dev    = dev,
                      st_inode  = inode,
                      st_mode   = mode,
                      st_nlink  = nlink,
                      st_uid    = uid,
                      st_gid    = gid,
                      st_size   = size,
                      timestamp = timestamp,
                      children  = children,
                      parent    = parent,
                      walked    = walked)

    @classmethod
    def from_json(cls, json_data):
        return cls(path = json_data['st_path'],
                   dev = json_data['st_dev'],
                   inode = json_data['st_inode'],
                   mode = json_data['st_mode'],
                   nlink = json_data['st_nlink'],
                   uid = json_data['st_uid'],
                   gid = json_data['st_gid'],
                   size = json_data['st_size'],
                   timestamp = Timestamp.from_json(json_data['timestamp']),
                   children = json_data['children'],
                   parent = json_data['parent'],
                   walked = json_data['walked'])

    def stat(self):
        ent = type('', (), {})()
        setattr(ent, 'st_ino', self.l_inode())
        setattr(ent, 'st_mode', self['st_mode'])
        setattr(ent, 'st_nlink', self['st_nlink'])
        setattr(ent, 'st_uid', self['st_uid'])
        setattr(ent, 'st_gid', self['st_gid'])
        setattr(ent, 'st_rdev', self['st_dev'])
        setattr(ent, 'st_size', self['st_size'])
        setattr(ent, 'st_atime_ns', self['timestamp'].atime_ns())
        setattr(ent, 'st_mtime_ns', self['timestamp'].mtime_ns())
        setattr(ent, 'st_ctime_ns', self['timestamp'].ctime_ns())
        return ent

    def basename(self):
        return os.path.basename(self['st_path'])

    def l_inode(self):
        return self['st_dev'] * self['st_inode']

    def full_path(self):
        if not self['parent']:
            return '/'

        parent = self['parent'].full_path()
        if parent != '/':
            parent += '/'
        return parent + self['st_path']

    def push_child(self, c):
        self['walked'] = True
        self['children'].append(c)

    def mode(self):
        return stat.filemode(self['st_mode'])

    def is_dir(self):
        return stat.S_ISDIR(self['st_mode'])

    def is_block(self):
        return stat.S_ISBLK(self['st_mode'])

    def is_char(self):
        return stat.S_ISCHR(self['st_mode'])

    def is_door(self):
        return stat.S_ISDOOR(self['st_mode'])

    def is_fifo(self):
        return stat.S_ISFIFO(self['st_mode'])

    def is_link(self):
        return stat.S_ISLNK(self['st_mode'])

    def is_port(self):
        return stat.S_ISPORT(self['st_mode'])

    def is_regular(self):
        return stat.S_ISREG(self['st_mode'])

    def is_sock(self):
        return stat.S_ISSOCK(self['st_mode'])

    def is_whiteout(self):
        return stat.S_ISWHT(self['st_mode'])
