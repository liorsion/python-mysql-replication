import struct
import copy
import pymysql
import socket
import errno
from pymysql.constants.COMMAND import *
from pymysql.err import OperationalError
from pymysql.util import byte2int, int2byte
from .packet import BinLogPacketWrapper
import logging
from .constants.BINLOG import TABLE_MAP_EVENT, ROTATE_EVENT
from pymysql.err import InternalError

class BinLogStreamReader(object):
    '''Connect to replication stream and read event'''

    def __init__(self, connection_settings = {}, resume_stream = False, blocking = False, only_events = None, server_id = 255,
                 last_log_persistancer = None):
        """
        resume_stream: Start for latest event of binlog or from older available event
        blocking: Read on stream is blocking
        only_events: Array of allowed events
        """
        self.__connection_settings = connection_settings
        self.__connection_settings['charset'] = 'utf8'

        self.__connected_stream = False
        self.__connected_ctl = False
        self.__resume_stream = resume_stream
        self.__blocking = blocking
        self.__only_events = only_events
        self.__server_id = server_id
        self.__log_pos = last_log_persistancer.load() if last_log_persistancer else None
        self.__last_log_persistancer = last_log_persistancer
        self.__log_file = None
        self.__is_running = False

        #Store table meta information
        self.table_map = {}

    def update_connection_settings(self, connection_settings):
        self.__connection_settings = connection_settings
        self.__connection_settings['charset'] = 'utf8'

        self.__resume_stream = True
        self.__last_log_persistancer = None
        self.close()

    def update_log_persistancer(self, last_log_persistancer):
        self.__last_log_persistancer = last_log_persistancer

    def close(self):
        if self.__connected_stream:
            try:
                self._stream_connection.close()
                self.__connected_stream = False
            except socket.error, e:
                if isinstance(e.args, tuple):
                    if e[0] == errno.EPIPE:
                        pass
                    else:
                        raise
                else:
                    raise
            except IOError, e:
                if e.errno == errno.EPIPE:
                    pass
                else:
                    raise

        if self.__connected_ctl:
            try:
                self._ctl_connection.close()
                self.__connected_ctl = False
            except IOError, e:
                if e.errno == errno.EPIPE:
                    pass
                else:
                    raise

    def __connect_to_ctl(self):
        self._ctl_connection_settings = copy.copy(self.__connection_settings)
        self._ctl_connection_settings['db'] = 'information_schema'
        self._ctl_connection_settings['cursorclass'] = pymysql.cursors.DictCursor
        self._ctl_connection = pymysql.connect(**self._ctl_connection_settings)
        self.__connected_ctl = True

    def __connect_to_stream(self):
        self._stream_connection = pymysql.connect(**self.__connection_settings)
        cur = self._stream_connection.cursor()
        cur.execute("SHOW MASTER STATUS")
        (log_file, log_pos) = cur.fetchone()[:2]
        cur.close()

        if self.__log_file is None:
            self.__log_file = log_file
        # binlog_pos (4) -- position in the binlog-file to start the stream with
        # flags (2) BINLOG_DUMP_NON_BLOCK (0 or 1)
        # server_id (4) -- server id of this slave
        # binlog-filename (string.EOF) -- filename of the binlog on the master
        command = COM_BINLOG_DUMP
        prelude = struct.pack('<i', len(self.__log_file) + 11) \
                  + int2byte(command)
        if self.__log_pos is None:
            if self.__resume_stream:
                prelude += struct.pack('<I', log_pos)
            else:
                prelude += struct.pack('<I', 4)
        else:
            prelude += struct.pack('<I', self.__log_pos)
        if self.__blocking:
            prelude += struct.pack('<h', 0)
        else:
            prelude += struct.pack('<h', 1)
        prelude += struct.pack('<I', self.__server_id)
        self._stream_connection.wfile.write(prelude + self.__log_file.encode())
        self._stream_connection.wfile.flush()
        self.__connected_stream = True

    def stop(self):
        self.__is_running = False
        #print "stop closing {0}".format(id(self._stream_connection))
        # forcing socket to close. Idealy, self._stream_connection.close() would have worked, but if the socked is blocked on reading, it doesn't.
        self._stream_connection.socket.shutdown(socket.SHUT_RDWR)
        #if self.__connected_ctl:
        #    self._ctl_connection.kill(self._stream_connection.thread_id())

    def fetchone(self):
        self.__is_running = True
        while self.__is_running:
            if not self.__connected_stream:
                self.__connect_to_stream()
            if not self.__connected_ctl:
                self.__connect_to_ctl()
            pkt = None
            try:
                pkt = self._stream_connection.read_packet()
            except pymysql.OperationalError as error:
                code, message = error.args
                if code == 2013: #2013: Connection Lost
                    self.close()
                    continue
            except NotImplementedError:
                logging.exception("Error iterating log!")
                continue
            except InternalError as (code, message):
                if code == 1236:
                    logging.exception("Interal error - (%d) in mysql (%s)" % (code, message))
                    if self.__last_log_persistancer:
                        self.__last_log_persistancer.save(0)
                else:
                    logging.exception("Internal error (%d) in mysql (%s)" % code, message)
                raise
            if not pkt.is_ok_packet():
                return None
            try:
                binlog_event = BinLogPacketWrapper(pkt, self.table_map, self._ctl_connection, self.__last_log_persistancer)
            except OperationalError as (code, message):
                if code == 2013:
                    self.close()
                else:
                    raise
            except Exception, e:
                logging.exception("Error iterating log!")
                continue


            if binlog_event.event_type == TABLE_MAP_EVENT:
                self.table_map[binlog_event.event.table_id] = binlog_event.event.get_table()
            if self.__filter_event(binlog_event.event):
                continue
            if binlog_event.event_type == ROTATE_EVENT:
                self.__log_pos = binlog_event.event.position
                self.__log_file = binlog_event.event.next_binlog
            else:
                self.__log_pos = binlog_event.log_pos
            return binlog_event.event

        logging.debug("Ending fetchone")

    def __filter_event(self, event):
        if self.__only_events is not None:
            for allowed_event in self.__only_events:
                if isinstance(event, allowed_event):
                    return False
            logging.info("Event type (%d) - %s not handled", event.event_type,  event.__class__.__name__)
            return True
        return False

    def __iter__(self):
        return iter(self.fetchone, None)
