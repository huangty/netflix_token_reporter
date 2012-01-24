#!/usr/bin/python
#
#   Netflix Traffic Blocker
#   @auther: Te-Yuan Huang
#   @date: 2012 Jan 12
##################################################

import os, time, signal, popen2, sha, traceback, StringIO, sys
import urllib2, urllib
import MySQLdb
from MySQLdb.cursors import SSCursor

#----------------------------------------------
# Hard coded block
#----------------------------------------------
pidfile = "/home/huangty/Research/netflix/setup/proxy/cache/cachegrabber.pid"
cache_path = "/home/huangty/Research/netflix/setup/proxy/cache/"
#----------------------------------------------

class DatabaseHandler:
    def __init__(self, *args, **kw):
        self.conn = MySQLdb.connect(host='localhost', db='netflix_movie_token', user='huangty', passwd='ofwork')
        self.cursor = self.conn.cursor(cursorclass=SSCursor)

    def get_file_list(self):
        sql_query = 'SELECT DISTINCT filename from token_table ORDER BY timestamp DESC'
        self.cursor.execute(sql_query)
        return self.cursor
	
    def get_file_request(self, filename):
        sql_query = 'SELECT base_url, token, hostname from token_table WHERE filename=\''+filename+'\' ORDER BY timestamp DESC Limit 1'
        self.cursor.execute(sql_query)
        return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()

        

class CacheGrabber:
    def __init__(self, *args, **kw):
        self.peroid = 10*60 #check the cache every 10 minutes
        self.fileq = []
        self.db = DatabaseHandler()

    def cleanUP(self):
        print "clean up .... "
        self.db.close()
        try:
            os.unlink(pidfile) #delete the file
        except:
            pass

    def sigHandler(self, signum, frame):
        if signum in [signal.SIGQUIT, signal.SIGTERM, signal.SIGHUP]:
            self.quitFlag = True

    def getFileList(self):
        cursor = self.db.get_file_list()
        self.fileq = []
        try:
            rows = cursor.fetchall()
            if not rows:
                return
            for row in rows:
                filename = row[0]
                self.fileq.append(filename)
                #if( not os.path.exists(cache_path+filename) ):
                #    self.fileq.append(filename)
        except:
            traceback.print_exc()
        self.getFiles()
    
    def getFiles(self):
        for filename in self.fileq:
            print "Checking file: %s" % filename
            cursor = self.db.get_file_request(filename)
            rows = cursor.fetchall()
            if not rows:
                return
            for base_url, token, host in rows:
                request = base_url+"range/0-?"+ token
                fil = base_url.split(host)
                if( len(fil) != 2):
                    return
                else:
                    head_request = fil[1] + "range/0-?"+ token
                    break            
            if (os.path.exists(cache_path+filename)):
                import httplib
                conn = httplib.HTTPConnection(host)
                conn.request("HEAD", head_request)
                res = conn.getresponse()
                correct_file_size = float(res.getheader('content-length'))
                current_file_size = os.path.getsize(cache_path+filename)
                print ("correct size: %s, current size: %s \n" % (correct_file_size, current_file_size))
                if(correct_file_size == current_file_size):
                    continue
            cmd = "wget -U \"Mozilla/5.0\" \"%s\" -O %s"  % (request, cache_path+filename)
            print("Requesting: %s\n" % (cmd))
            shOut, shIn = popen2.popen4("/bin/sh")
            shIn.write(cmd)
            shIn.close()
            out = shOut.read()
            shOut.close()
            print ("Script Output %s" % out)
            #filename_full = cache_path + filename
            #fout = open(filename_full, "wb")
            #headers= {'User-agent': 'Mozilla/5.0'}
            #req = urllib2.Request(request, None, headers)
            #fout.write(urllib2.urlopen(req).read())
            #fout.close()
        print "Finish checking, waiting for the next check...."

    def run(self):
        self.quitFlag = False
        if os.path.isfile(pidfile):
            print "Another instance of TrafficShaper is running under pid %s" % pidfile
            print "quitting .... "
            sys.exit(1)
        file(pidfile, "w").write(str(os.getpid()))
        signal.signal(signal.SIGQUIT, self.sigHandler)
        signal.signal(signal.SIGHUP, self.sigHandler)
        signal.signal(signal.SIGTERM, self.sigHandler)
        
        try:
            while not self.quitFlag:
                print "checking again .... "
                self.getFileList()
                #wait a bit
                i = 0
                while i < self.peroid:
                    if self.quitFlag:
                        break
                    time.sleep(1)
                    i = i + 1
        except KeyboardInterrupt:
            print "Shaping terminated by keyboard interrupt"
        except:
            traceback.print_exc()
            print "cache grabber terminated"
        else:
            print "cache grabber terminated by kill signal"
        self.cleanUP()


if __name__ == '__main__':
    shaper = CacheGrabber()
    shaper.run()
    
    #conn = MySQLdb.connect(host='localhost', db='netflix_movie_token', user='huangty', passwd='ofwork')
    #cursor = conn.cursor(cursorclass=SSCursor)

    #sql_query = 'SELECT DISTINCT ip from host_table'
    #cursor.execute(sql_query)
    #db = DatabaseHandler()
    #cursor = db.get_ip()

    #try:
    #    while True:
    #        rows = cursor.fetchall()
    #        if not rows:
    #            break
    #        for row in rows:
    #            ip = row[0]
    #            print ip
    #            
    #finally:
    #    db.close()
