#
#   Netflix Traffic Blocker
#   @auther: Te-Yuan Huang
#   @date: 2012 Jan 12
##################################################
import os, time, signal, popen2, sha, traceback, StringIO, sys
import MySQLdb
from MySQLdb.cursors import SSCursor

#----------------------------------------------
# Hard coded block
#----------------------------------------------
IF="eth5"
TOKEN_HOST="192.168.27.54"
pidfile = "/var/run/trafficshaper.pid"
TC = "/sbin/tc"
U32 = TC + " filter add dev " + IF + " protocol ip parent 1:0 prio 1 u32"
TROTTLE_RATE = "1kbit"
#----------------------------------------------

class DatabaseHandler:
    def __init__(self, *args, **kw):
        self.conn = MySQLdb.connect(host='localhost', db='netflix_movie_token', user='huangty', passwd='ofwork')
        self.cursor = self.conn.cursor(cursorclass=SSCursor)

    def get_lastest_ip(self):
        sql_query = 'SELECT hostname from token_table ORDER BY timestamp DESC LIMIT 5'
        self.cursor.execute(sql_query)
        hosts = self.cursor.fetchall()
        if not hosts:
            return
        sql_query = 'SELECT DISTINCT ip from host_table WHERE '
        initial = 0;
        for host in hosts:
            if ( initial == 0 ):
                sql_query = sql_query + " hostname='" + host[0] + "'"
            else:
                sql_query = sql_query + " OR hostname='" + host[0] + "'"
            initial = 1
	
        print sql_query
        self.cursor.execute(sql_query)
        return self.cursor

	
    def get_allip(self):
        sql_query = 'SELECT DISTINCT ip from host_table'
        self.cursor.execute(sql_query)
        return self.cursor

    def close(self):
        self.cursor.close()
        self.conn.close()

        

class TrafficShaper:
    def __init__(self, *args, **kw):
        self.peroid = 10*60 #check the config every 10 minutes
        self.cmdq = []
        self.cmdBuf = ''
        self.cmdBufHash = ''
        self.cmdBufHashOld = ''
        self.db = DatabaseHandler()
        if os.getuid() != 0:
            raise Exception("You must be root to run this program")

    def cleanUP(self):
        print "clean up .... "
        self.stopTC()
        self.db.close()
        try:
            os.unlink(pidfile) #delete the file
        except:
            pass

    def sigHandler(self, signum, frame):
        if signum in [signal.SIGQUIT, signal.SIGTERM, signal.SIGHUP]:
            self.quitFlag = True

    def setupTC(self):
        self.cmdq.append(TC + " qdisc add dev " + IF + " root handle 1: htb default 30")
        self.cmdq.append(TC + " class add dev " + IF + " parent 1: classid 1:1 htb rate " + TROTTLE_RATE)
        cursor = self.db.get_lastest_ip()

        try:
            rows = cursor.fetchall()
            if not rows:
                return
            for row in rows:
                blockip = row[0]
                self.cmdq.append(U32 + " match ip dst " + TOKEN_HOST + "/32 match ip src " + blockip + "/32 flowid 1:1")
        except:
            traceback.print_exc()
        self.runCommands()
    
    def runCommands(self):
        self.cmdBuf = "\n".join(self.cmdq)+"\n"
        self.cmdBufHash = sha.new(self.cmdBuf).hexdigest()
        self.cmdq = []
        
        if self.cmdBufHash == self.cmdBufHashOld :
            print("runCommands: no change to tc command set, not executing")
            return
        else:
            self.stopTC()
            time.sleep(1)
            print("runCommands: executing the following command: \n %s " % self.cmdBuf)

        shOut, shIn = popen2.popen4("/bin/sh")
        shIn.write(self.cmdBuf)
        shIn.close()
        out = shOut.read()
        shOut.close()

        self.cmdBufHashOld = self.cmdBufHash
        print("Script Output:\n %s\n" % out);

    def stopTC(self):
        shOut, shIn = popen2.popen4("/bin/sh")
        cmd = "\n "+ TC + " qdisc del dev " + IF + " root" 
        shIn.write(cmd)
        shIn.close()
        out = shOut.read()
        shOut.close()
        print("stopping TC by %s \n, output: %s" % (cmd, out))
        
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
                self.setupTC()
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
            print "traffic shaper terminated"
        else:
            print "traffic shaper terminated by kill signal"
        self.cleanUP()


if __name__ == '__main__':

    shaper = TrafficShaper()
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
