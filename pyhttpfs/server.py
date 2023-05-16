
import http.server
import io
import json
import os
import socketserver
import stat
import sys

from argparse import ArgumentParser
from datetime import datetime
from tabulate import tabulate

from .types import File, Timestamp

class FsServer(http.server.SimpleHTTPRequestHandler):

    def list_directory(self, path):
        try:
            dirents = os.scandir(path)
        except OSError as e:
            self.send_error(404, explain=e);
            return

        self.send_response(200)

        tab = []
        for dirent in dirents:
            st = dirent.stat()
            ts = datetime.fromtimestamp(st.st_mtime)
            t = Timestamp(mtim_sec=st.st_mtime,
                          atim_sec=st.st_atime,
                          ctim_sec=st.st_ctime)
            f = File(dirent.name, dev=st.st_dev, inode=st.st_ino,
                mode=st.st_mode, nlink=st.st_nlink, uid=st.st_uid,
                gid=st.st_gid, size=st.st_size, timestamp=t)
            tab.append(f)

        output = json.dumps(tab, indent=4).encode()

        headers = f'''HTTP/1.0 200 OK
Server: PyHttpFS/1.0 Python/3.10.6
Content-type: application/json; charset=utf-8
Content-Length: {len(output)}'''
        response = headers.encode() + b'\r\n\r\n' + output

        return io.BytesIO(response)

desc = '''Binds port (default 8000), and serves DIR (default .) to pyhttpfs clients'''
epi = '''NOT FOR PRODUCTION USE: Does not implement any authentication, and
data is exposed without crypto.'''

def parse_args(args):
    parser = ArgumentParser(prog='HTTPfs Server',
                            description=desc,
                            epilog=epi)

    parser.add_argument('-d', '--dir', type=str, default='.',
                        help='Directory to serve')
    parser.add_argument('port', type=int, default=8000,
                        help='Port to bind for server')
    return parser.parse_args(args)

def main():
    options = parse_args(sys.argv[1:])
    os.chdir(options.dir)
    with socketserver.TCPServer(('', options.port), FsServer) as httpd:
        try:
            print("serving at port", options.port)
            httpd.serve_forever()
        except KeyboardInterrupt as e:
            httpd.shutdown()
    
if __name__ == '__main__':
    main()
