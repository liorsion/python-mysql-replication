__author__ = 'lior'

import unittest
import random
from pymysqlreplication import BinLogStreamReader

from pymysqlreplication.row_event import *
from pymysqlreplication.constants.BINLOG import *
from time import sleep
from threading import Thread
import pymysql
import Queue

last_event = None


class BinLogStreamReaderTestCase(unittest.TestCase):
    def setUp(self):
        global last_event
        last_event = None
        pass

    def tearDown(self):
        pass

    def testChnageNonWorkingMysqlToWorking(self):
        mysql_settings = {'host': "localhost", 'port': 9999, 'user': "root", 'passwd': ""}
        serverId = random.randint(100, 100000)

        _stream = BinLogStreamReader(connection_settings = mysql_settings, server_id=serverId,
                                     resume_stream=True,
                                     only_events = None, blocking=True,
                                     last_log_persistancer=None)

        q = Queue.Queue(maxsize=0)
        def listen_and_alert_function():
            do_thread = True
            while do_thread:
                try:
                    for binlogevent in _stream:
                        do_thread = False
                        break
                except pymysql.OperationalError:
                    q.put(1)
                    sleep(1)
                #print "in thread loop"
            #print "exited thread loop"


        working_thread = Thread(target =  listen_and_alert_function, args=())
        working_thread.start()

        e = q.get()
        mysql_settings['port'] = 3306
        _stream.update_connection_settings(mysql_settings)
        working_thread.join()

        # this test, well, will get stuck forever if it fails
        assert(True)



if __name__ == '__main__':
    unittest.main()
