#!/usr/bin/python
 
__doc__ = """Tiny HTTP Proxy.
 
This module implements GET, HEAD, POST, PUT and DELETE methods
on BaseHTTPServer, and behaves as an HTTP proxy.  The CONNECT
method is also implemented experimentally, but has not been
tested yet.
 
Any help will be greatly appreciated.       SUZUKI Hisao
 
2009/11/23 - Modified by Mitko Haralanov
             * Added very simple FTP file retrieval
             * Added custom logging methods
             * Added code to make this a standalone application

2011/01/19 - Modified by Te-Yuan Huang
             * Specific for netflix

Downloaded from : http://www.voidtrance.net/2010/01/simple-python-http-proxy/

"""
 
__version__ = "0.3.2"
 
import BaseHTTPServer, select, socket, SocketServer, urlparse, traceback
import logging
import logging.handlers
import getopt
import sys
import os
import signal
import threading
from types import FrameType, CodeType
from time import sleep
import ftplib
import MySQLdb
from MySQLdb.cursors import SSCursor

 
DEFAULT_LOG_FILENAME = "proxy.log"
CACHE_PATH = "/home/huangty/Research/netflix/setup/proxy/cache/"
HOST_IP = "172.24.74.100"
SERVE_FROM_CACHE = True
OFFLINE_VIEW = True
SPLIT_REQUEST = False

class DatabaseHandler:
    def __init__(self, *args, **kw):
        self.conn = MySQLdb.connect(host='localhost', db='netflix_movie_token', user='huangty', passwd='ofwork')
        self.cursor = self.conn.cursor(cursorclass=SSCursor)

    def get_file_list(self):
        sql_query = 'SELECT DISTINCT filename from token_table ORDER BY timestamp DESC'
        self.cursor.execute(sql_query)
        return self.cursor

    def get_available_tokens(self, filename):
        sql_query = 'SELECT base_url, hostname, token from token_table WHERE filename=\''+filename+'\' ORDER BY timestamp DESC'
        self.cursor.execute(sql_query)
        urls = {}
        servers = {}
        try:
            rows = self.cursor.fetchall()
            if not rows:
                return
            for base_url, hostname, token in rows:
                urls[base_url] = token
                servers[base_url] = hostname
        except:
            traceback.print_exc()
        return (urls, servers)
    
    def add_etag(self, filename, etag, server):
        sql_query = 'UPDATE token_table SET etag=\'%s\' WHERE filename=\'%s\' and hostname=\'%s\'' % (etag, filename, server)
        self.cursor.execute(sql_query)
        return self.cursor
    
    def get_etag(self, filename):
        sql_query = 'SELECT etag from token_table WHERE filename=\''+filename+'\' ORDER BY timestamp DESC Limit 1'
        self.cursor.execute(sql_query)
        etag = ''
        try:
            rows = self.cursor.fetchall()
            if not rows:
                return
            for tag in rows:
                etag = tag
        except:
            traceback.print_exc()
        return etag
        
    def close(self):
        self.cursor.close()
        self.conn.close()


class ProxyHandler (BaseHTTPServer.BaseHTTPRequestHandler):
    __base = BaseHTTPServer.BaseHTTPRequestHandler
    __base_handle = __base.handle
 
    server_version = "NetflixProxy/" + __version__
    rbufsize = 0                        # self.rfile Be unbuffered
 
    def handle(self):
        (ip, port) =  self.client_address
        self.server.logger.log (logging.INFO, "Request from '%s'", ip)
        if hasattr(self, 'allowed_clients') and ip not in self.allowed_clients:
            self.raw_requestline = self.rfile.readline()
            if self.parse_request(): self.send_error(403)
        else:
            self.__base_handle()
 
    def _connect_to(self, netloc, soc):
        i = netloc.find(':')
        if i >= 0:
            host_port = netloc[:i], int(netloc[i+1:])
        else:
            host_port = netloc, 80
        self.server.logger.log (logging.INFO, "connect to %s:%d", host_port[0], host_port[1])
        try: soc.connect(host_port)
        except socket.error, arg:
            try: msg = arg[1]
            except: msg = arg
            self.send_error(404, msg)
            return 0
        return 1
 
    def do_CONNECT(self):
        print "doing do_CONNECT"
        soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if self._connect_to(self.path, soc):
                self.log_request(200)
                self.wfile.write(self.protocol_version +
                                 " 200 Connection established\r\n")
                self.wfile.write("Proxy-agent: %s\r\n" % self.version_string())
                self.wfile.write("\r\n")
                self._read_write(soc, 300)
        finally:
            soc.close()
            self.connection.close()
 
    def send_http_request(self, server, command, url):
        import httplib
        conn = httplib.HTTPConnection(server)
        conn.putrequest(command, url)
        conn.putheader("Host", HOST_IP)
        conn.putheader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_5_8) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.77 Safari/535.7")
        conn.putheader("Accept", "*/*")
        conn.putheader("Accept-Encoding", "gzip,deflate,sdch")
        conn.putheader("Accept-Language", "en-US,en;q=0.8")
        conn.putheader("Accept-Charset", "ISO-8859-1,utf-8;q=0.7,*;q=0.3")
        conn.putheader("Connection", "close")
        conn.endheaders()
        return conn.getresponse()
    
    def forge_http_header(self, filename, server, request_bytes, etag):
        headerq = []
        headerq.append("Server: Apache")
        headerq.append("ETag: %s" % etag)
        headerq.append("Last-Modified: Sat, 07 May 2011 07:52:22 GMT")
        headerq.append("Accept-Ranges: bytes")
        headerq.append("Content-Length: %s" % request_bytes)
        headerq.append("Content-Type: text/plain")
        import time
        headerq.append(self.date_time_string(time.time()))
        headerq.append("Connection: close")
        headerq.append("Cache-Control: no-store")
        
        return "\r\n".join(headerq)+"\r\n"


    def is_netflix_data_request(self, path, server, url):
        is_netflix = False
        print "path=%s, server=%s, url=%s" % (path, server, url)
        if  ("/range/" in path) and (("edgesuite.net" in server) or ("llnwd.net" in server) or ("lcdn.nflximg.com" in server) ):            
            is_netflix = True
            is_header_forgable = True
            request_range_start = ''
            request_range_end = ''
            filename = path.split("/range/")[0].split("/")
            file_range = path.split("/range/")[1].split("-")
            if( len(file_range) >= 2 ):
                request_range_start = file_range[0]
                request_range_end = file_range[1]
            if(request_range_end == ''):
                is_header_forgable = False
                        
            if(is_header_forgable == True):
                etag = db.get_etag(filename[len(filename)-1])
                if(etag == ''):
                    is_header_forgable = False
            if(OFFLINE_VIEW == True and is_header_forgable == True):
                print "Forging the header... \n"                
                request_bytes = int(request_range_end) - int(request_range_start) + 1
                http_header_fake = self.forge_http_header(filename[len(filename)-1], server, request_bytes, etag)
                #print "\n=============================\n"
                #print "\n Forged HTTP Header\n"
                #print "\n=============================\n"
                #print http_header_fake
                #print "\n=============================\n"
                return (is_netflix, filename[len(filename)-1], request_range_start, request_range_end, http_header_fake)
            else:
                print "Get Header from server, url: %s" % url
                http_header = self.send_http_request(server, "HEAD", url)
                file_size = float(http_header.getheader('content-length'))
                etag = http_header.getheader('Etag')
                if( file_size == 0): #not a data traffic
                    is_netflix = False
                    return(is_netflix, [], 0, 0, [])
                
                #if( "referer" in self.headers and ("movies.netflix.com" in self.headers['referer'])):
                #print "filename:%s, etag:%s, hostname:%s" % (filename[len(filename)-1], etag, server)
                if(etag != ""):
                    db.add_etag(filename[len(filename)-1], etag, server)
                if(request_range_end == ''):
                    request_range_end = file_size
                return (is_netflix, filename[len(filename)-1], request_range_start, request_range_end, http_header.msg)
        else:
            return(is_netflix, [], 0, 0, [])

    def normal_proxy_relay(self, http_command, url, request_version, headers, soc):
        print "Serve From Normal Proxy";
        soc.send("%s %s %s\r\n" % (http_command, url, request_version))
        headers['Connection'] = 'close'
        del headers['Proxy-Connection']            
        for key_val in headers.items():
            soc.send("%s: %s\r\n" % key_val)
            #print "%s: %s\r\n" % key_val
        soc.send("\r\n")
        #print "User Header: \n %s" % headers
        self._read_write(soc)


    def do_GET(self):
        (scm, netloc, path, params, query, fragment) = urlparse.urlparse(
            self.path, 'http')
        if scm not in ('http', 'ftp') or fragment or not netloc:
            self.send_error(400, "bad url %s" % self.path)
            return
        soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            if scm == 'http':                
                url = urlparse.urlunparse(('', '', path, params, query,''))
                print ("COMMAND: %s path=%s netloc=%s url=%s \r \n" % (self.command, path, netloc, url))
                (is_netflix_data_traffic, filename, request_range_start, request_range_end, http_header) = self.is_netflix_data_request(path, netloc, url)
                #print ("Netflix Traffic?: %s, Request File: %s, Range=%s-%s, HEADER=%s"
                #                %(is_netflix_data_traffic, filename, request_range_start, request_range_end, http_header))
                
                if( is_netflix_data_traffic and SERVE_FROM_CACHE == True):
                    if(os.path.exists(CACHE_PATH+filename) and ( int(request_range_end) <= os.path.getsize(CACHE_PATH+filename) ) ):
                        print "SERVE Header from Head request abd Serve data from the cache for file %s" % filename
                        start = int(request_range_start)
                        end = int(request_range_end)                            
                        header = "HTTP/1.1 200 OK\r\nHTTP/1.1 200 OK\r\n%s\r\n" % http_header
                        #print header
                        self.connection.send(header)
                        f = open(CACHE_PATH+filename, "r")
                        f.seek(start)
                        video = f.read(end-start+1)
                        f.close()
                        self.connection.send(video)
                    else:
                        print "No Cache Exist, Serve from Proxy"
                        if self._connect_to(netloc, soc):
                            self.log_request()
                            self.normal_proxy_relay(self.command, url, self.request_version, self.headers, soc)
                elif( is_netflix_data_traffic and SPLIT_REQUEST == True):
                    #print "Request from %s, Token available at %s"
                    urls,servers = db.get_available_tokens(filename)
                    start = int(request_range_start)
                    end = int(request_range_end)
                    num_split = len(urls)
                    #print "start: %s, end: %s, split into %s" % (start, end, num_split)
                    url_start = start
                    i = 1
                    video = ""
                    for base_url, token in urls.items():                            
                        if ( i == num_split ):
                            url_end = end
                        else:
                            url_end = url_start + int((end-start+1) / num_split)                            
                        i += 1
                        split_url = base_url + "range/%s-%s?" % (url_start, url_end) + token
                        url_start = url_end + 1
                        print split_url
                        http_response = self.send_http_request(servers[base_url], "GET", split_url)
                        video += http_response.read()
                    #print video
                    header = "HTTP/1.1 200 OK\r\nHTTP/1.1 200 OK\r\n%s\r\n" % http_header.msg
                    self.connection.send(header)
                    self.connection.send(video)
                    #self.normal_proxy_relay(self.command, url, self.request_version, self.headers, soc)
                else:
                    if self._connect_to(netloc, soc):
                        self.log_request()
                        self.normal_proxy_relay(self.command, url, self.request_version, self.headers, soc)
            elif scm == 'ftp':
                # fish out user and password information
                i = netloc.find ('@')
                if i >= 0:
                    login_info, netloc = netloc[:i], netloc[i+1:]
                    try: user, passwd = login_info.split (':', 1)
                    except ValueError: user, passwd = "anonymous", None
                else: user, passwd ="anonymous", None
                self.log_request ()
                try:
                    ftp = ftplib.FTP (netloc)
                    ftp.login (user, passwd)
                    if self.command == "GET":
                        ftp.retrbinary ("RETR %s"%path, self.connection.send)
                    ftp.quit ()
                except Exception, e:
                    self.server.logger.log (logging.WARNING, "FTP Exception: %s",
                                            e)
        finally:
            soc.close()
            self.connection.close()
 
    def _read_write(self, soc, max_idling=20, local=False):
        iw = [self.connection, soc]
        local_data = ""
        ow = []
        count = 0
        while 1:
            count += 1
            (ins, _, exs) = select.select(iw, ow, iw, 1)
            if exs: break
            if ins:
                for i in ins:
                    if i is soc: out = self.connection
                    else: out = soc
                    data = i.recv(8192)
                    if data:
                        if local: local_data += data
                        else: out.send(data)
                        count = 0
            if count == max_idling: break
        if local: return local_data
        return None

    do_HEAD = do_GET
    do_POST = do_GET
    do_PUT  = do_GET
    do_DELETE=do_GET
 
    def log_message (self, format, *args):
        self.server.logger.log (logging.INFO, "%s %s", self.address_string (),
                                format % args)
 
    def log_error (self, format, *args):
        self.server.logger.log (logging.ERROR, "%s %s", self.address_string (),
                                format % args)
 
class ThreadingHTTPServer (SocketServer.ThreadingMixIn,
                           BaseHTTPServer.HTTPServer):
    def __init__ (self, server_address, RequestHandlerClass, logger=None):
        BaseHTTPServer.HTTPServer.__init__ (self, server_address,
                                            RequestHandlerClass)
        self.logger = logger
 
def logSetup (filename, log_size, daemon):
    logger = logging.getLogger ("TinyHTTPProxy")
    logger.setLevel (logging.INFO)
    if not filename:
        if not daemon:
            # display to the screen
            handler = logging.StreamHandler ()
        else:
            handler = logging.handlers.RotatingFileHandler (DEFAULT_LOG_FILENAME,
                                                            maxBytes=(log_size*(1<<20)),
                                                            backupCount=5)
    else:
        handler = logging.handlers.RotatingFileHandler (filename,
                                                        maxBytes=(log_size*(1<<20)),
                                                        backupCount=5)
    fmt = logging.Formatter ("[%(asctime)-12s.%(msecs)03d] "
                             "%(levelname)-8s {%(name)s %(threadName)s}"
                             " %(message)s",
                             "%Y-%m-%d %H:%M:%S")
    handler.setFormatter (fmt)
 
    logger.addHandler (handler)
    return logger
 
def usage (msg=None):
    if msg: print msg
    print sys.argv[0], "[-p port] [-l logfile] [-dh] [allowed_client_name ...]]"
    print
    print "   -p       - Port to bind to"
    print "   -l       - Path to logfile. If not specified, STDOUT is used"
    print "   -d       - Run in the background"
    print
 
def handler (signo, frame):
    while frame and isinstance (frame, FrameType):
        if frame.f_code and isinstance (frame.f_code, CodeType):
            if "run_event" in frame.f_code.co_varnames:
                frame.f_locals["run_event"].set ()
                return
        frame = frame.f_back
 
def daemonize (logger):
    class DevNull (object):
        def __init__ (self): self.fd = os.open ("/dev/null", os.O_WRONLY)
        def write (self, *args, **kwargs): return 0
        def read (self, *args, **kwargs): return 0
        def fileno (self): return self.fd
        def close (self): os.close (self.fd)
    class ErrorLog:
        def __init__ (self, obj): self.obj = obj
        def write (self, string): self.obj.log (logging.ERROR, string)
        def read (self, *args, **kwargs): return 0
        def close (self): pass
 
    if os.fork () != 0:
        ## allow the child pid to instanciate the server
        ## class
        sleep (1)
        sys.exit (0)
    os.setsid ()
    fd = os.open ('/dev/null', os.O_RDONLY)
    if fd != 0:
        os.dup2 (fd, 0)
        os.close (fd)
    null = DevNull ()
    log = ErrorLog (logger)
    sys.stdout = null
    sys.stderr = log
    sys.stdin = null
    fd = os.open ('/dev/null', os.O_WRONLY)
    #if fd != 1: os.dup2 (fd, 1)
    os.dup2 (sys.stdout.fileno (), 1)
    if fd != 2: os.dup2 (fd, 2)
    if fd not in (1, 2): os.close (fd)
 
def main ():
    logfile = None
    daemon  = False
    max_log_size = 20
    port = 8000
    allowed = []
    run_event = threading.Event ()
    local_hostname = socket.gethostname ()
 
    try: opts, args = getopt.getopt (sys.argv[1:], "l:dhp:", [])
    except getopt.GetoptError, e:
        usage (str (e))
        return 1
 
    for opt, value in opts:
        if opt == "-p": port = int (value)
        if opt == "-l": logfile = value
        if opt == "-d": daemon = not daemon
        if opt == "-h":
            usage ()
            return 0
 
    # setup the log file
    logger = logSetup (logfile, max_log_size, daemon)
 
    if daemon:
        daemonize (logger)
    signal.signal (signal.SIGINT, handler)
 
    if args:
        allowed = []
        for name in args:
            client = socket.gethostbyname(name)
            allowed.append(client)
            logger.log (logging.INFO, "Accept: %s (%s)" % (client, name))
        ProxyHandler.allowed_clients = allowed
    else:
        logger.log (logging.INFO, "Any clients will be served...")
 
    #server_address = (socket.gethostbyname (local_hostname), port)
    server_address = ( "%s" % HOST_IP , port)
    ProxyHandler.protocol = "HTTP/1.0"
    httpd = ThreadingHTTPServer (server_address, ProxyHandler, logger)
    sa = httpd.socket.getsockname ()
    print "Servering HTTP on", sa[0], "port", sa[1]
    req_count = 0
    while not run_event.isSet ():
        try:
            httpd.handle_request ()
            req_count += 1
            if req_count == 1000:
                logger.log (logging.INFO, "Number of active threads: %s",
                            threading.activeCount ())
                req_count = 0
        except select.error, e:
            if e[0] == 4 and run_event.isSet (): pass
            else:
                logger.log (logging.CRITICAL, "Errno: %d - %s", e[0], e[1])
    logger.log (logging.INFO, "Server shutdown")
    return 0
 
if __name__ == '__main__':
    db = DatabaseHandler()
    #urls = db.get_available_tokens("1050719205.ismv")
    #print urls
    sys.exit (main ())
