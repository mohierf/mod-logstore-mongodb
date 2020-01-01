#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (C) 2009-2010:
#    Gabes Jean, naparuba@gmail.com
#    Gerhard Lausser, Gerhard.Lausser@consol.de
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken.  If not, see <http://www.gnu.org/licenses/>.


#
# This file is used to test host- and service-downtimes.
#


from __future__ import print_function

import os
import socket
import sys
import subprocess
import time
import random
import tempfile

import datetime
# from datetime import datetime
from freezegun import freeze_time

import pytest
# sys.path.append('../shinken/modules')

"""
This function is declared in the ShinkenModulesTest class in the Shinken repository, test directory
Unfortunately, there is no real chance to propose a modification that will be merged in the 
Shinken project. I rewrite the code and update here :-) 
"""
from shinken_modules import ShinkenModulesTest

from shinken_test import time_hacker

from shinken.modulesctx import modulesctx
from shinken.objects.module import Module
from shinken.objects.service import Service


from mock_livestatus import mock_livestatus_handle_request
from livestatus.log_line import Logline


LiveStatusLogStoreMongoDB = modulesctx.get_module('logstore-mongodb').LiveStatusLogStoreMongoDB


sys.setcheckinterval(10000)


@mock_livestatus_handle_request
class TestConfig(ShinkenModulesTest):

    # how much seconds give to mongod be fully started
    # == listening on its input socket/port.
    mongod_start_timeout = 60

    def update_broker(self, dodeepcopy=False):
        """Overloads the Shinken update_broker method because it does not handle
        the broks list as a list but as a dict !"""
        for brok in self.sched.brokers['Default-Broker']['broks']:
            if dodeepcopy:
                brok = copy.deepcopy(brok)
            brok.prepare()
            # print("Managing a brok, type: %s" % brok.type)
            self.livestatus_broker.manage_brok(brok)
        self.sched.brokers['Default-Broker']['broks'] = []

    @classmethod
    def _read_mongolog_and_raise(cls, log, proc, reason):
        try:
            with open(log) as fh:
                mongolog = fh.read()
        except Exception as err:
            mongolog = "Couldn't read log from mongo log file: %s" % err
        raise RuntimeError(
            "%s: rc=%s stdout/err=%s ; monglog=%s" % (
            reason, proc.returncode, proc.stdout.read(), mongolog))

    @classmethod
    def setUpClass(cls):
        # Real time for all the tests - cannot remove this silly time_hacker, so disable it!
        time_hacker.set_real_time()

        # temp path for mongod files :
        # as you can see it's relative path, that'll be relative to where the test is launched,
        # which should be in the Shinken test directory.
        mongo_path = cls._mongo_tmp_path = tempfile.mkdtemp(dir="./tmp/", prefix="mongo")
        mongo_db = os.path.join(mongo_path, 'db')
        mongo_log = os.path.join(mongo_path, 'log.txt')
        os.system('/bin/rm -rf %r' % mongo_path)
        os.makedirs(mongo_db)

        print("%s - Starting embedded mongo daemon..." % time.strftime("%H:%M:%S"))
        sock = socket.socket()
        sock.bind(('127.0.0.1', 0))
        port = sock.getsockname()[1]
        sock.close()
        cls.mongo_db_uri = "mongodb://127.0.0.1:%s" % port

        mp = cls._mongo_proc = subprocess.Popen(['/usr/bin/mongod', '--dbpath', mongo_db, '--port', str(port),
                                                 '--logpath', mongo_log, '--smallfiles'],
                                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=False)
        print("%s - Giving it some seconds to correctly start..." % time.strftime("%H:%M:%S"))

        # mongo takes some time to startup as it creates freshly new database files
        # so we need a relatively big timeout:
        timeout = time.time() + cls.mongod_start_timeout
        while time.time() < timeout:
            time.sleep(1)
            mp.poll()
            if mp.returncode is not None:
                cls._read_mongolog_and_raise(mongo_log, mp,
                                             "Launched mongod but it directly died")

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            errno = sock.connect_ex(('127.0.0.1', port))
            if not errno:
                sock.close()
                break
        else:
            mp.kill()
            cls._read_mongolog_and_raise(mongo_log, mp,
                                         "could not connect to port %s : mongod failed to correctly start?" % port)

        print("%s - started" % time.strftime("%H:%M:%S"))

    @classmethod
    def tearDownClass(cls):
        mp = cls._mongo_proc
        mp.terminate()
        print("%s - waiting mongod server to exit..." % time.strftime("%H:%M:%S"))
        # time_hacker.set_real_time()
        for _ in range(30):
            if mp.poll() is not None:
                break
            time.sleep(1.0)
        else:
            print("%s - didn't exited after 30 seconds! killing it..." % time.strftime("%H:%M:%S"))
            mp.kill()
        mp.wait()
        print("%s - exited" % time.strftime("%H:%M:%S"))
        os.system('/bin/rm -rf %r' % cls._mongo_tmp_path)

    def tearDown(self):
        self.livestatus_broker.db.commit()
        self.livestatus_broker.db.close()

        if os.path.exists(self.livelogs):
            os.remove(self.livelogs)
        if os.path.exists(self.livelogs + "-journal"):
            os.remove(self.livelogs + "-journal")
        if os.path.exists("tmp/archives"):
            for db in os.listdir("tmp/archives"):
                print("cleanup", db)
                os.remove(os.path.join("tmp/archives", db))
        if os.path.exists('var/nagios.log'):
            os.remove('var/nagios.log')
        if os.path.exists('var/retention.dat'):
            os.remove('var/retention.dat')
        if os.path.exists('var/status.dat'):
            os.remove('var/status.dat')
        self.livestatus_broker = None


@mock_livestatus_handle_request
class TestConfigSmall(TestConfig):
    def setUp(self):
        setup_state_time = time.time()
        self.setup_with_file('etc/shinken_1r_1h_1s.cfg')
        self.testid = str(os.getpid() + random.randint(1, 1000))

        self.cfg_database = 'test' + self.testid
        self.cfg_collection = 'ls-logs'

        dbmodconf = Module({
            'module_name': 'LogStore',
            'module_type': 'logstore_mongodb',
            'mongodb_uri': self.mongo_db_uri,
            'database': self.cfg_database,
            'collection': self.cfg_collection
        })

        self.init_livestatus(dbmodconf=dbmodconf)

        print("Requesting initial status broks...")
        self.sched.conf.skip_initial_broks = False
        self.sched.brokers['Default-Broker'] = {'broks': [], 'has_full_broks': False}
        self.sched.fill_initial_broks('Default-Broker')
        print("My initial broks: %d broks" % (len(self.sched.brokers['Default-Broker'])))

        self.update_broker()
        print("Initial setup duration:", time.time() - setup_state_time)

        self.nagios_path = None
        self.livestatus_path = None
        self.nagios_config = None
        # add use_aggressive_host_checking so we can mix exit codes 1 and 2
        # but still get DOWN state
        host = self.sched.hosts.find_by_name("test_host_0")
        host.__class__.use_aggressive_host_checking = 1

    def _make_down_up(self, the_host_name, the_date):
        """Make the host go DOWN 15 minutes after the date and then UP 15 minutes later!"""
        host = self.sched.hosts.find_by_name(the_host_name)
        assert host is not None, "Host %s is not known!" % the_host_name

        # Freeze the time !
        with freeze_time(the_date) as frozen_datetime:

            # Time warp 15 minutes in the future
            print("Now is: %s / %s" % (time.time(), time.strftime("%H:%M:%S")))
            frozen_datetime.tick(delta=datetime.timedelta(seconds=900))
            print("Now is: %s / %s" % (time.time(), time.strftime("%H:%M:%S")))

            host.state = 'DOWN'
            host.state_type = 'SOFT'
            host.attempt = 1
            host.output = "i am down"
            host.raise_alert_log_entry()
            self.update_broker()

            # Time warp 15 minutes in the future
            frozen_datetime.tick(delta=datetime.timedelta(seconds=900))
            print("Now is: %s / %s" % (time.time(), time.strftime("%H:%M:%S")))

            host.state = 'UP'
            host.state_type = 'HARD'
            host.attempt = 1
            host.output = "i am up"
            host.raise_alert_log_entry()
            self.update_broker()

    def _request(self, request, expected_response_length):
        print("\n-----\nRequest: %s\n-----\n" % request)
        tic = time.time()
        response, keepalive = self.livestatus_broker.livestatus.handle_request(request)
        tac = time.time()
        pyresponse = eval(response)
        print("Result: \n - # records matching the filter: %d\n - duration: %.2f"
              % (len(pyresponse), tac - tic))
        print("Response:")
        for item in pyresponse:
            print("- %s" % item)
        self.assertTrue(len(pyresponse) == expected_response_length)

        return pyresponse

    def test_one_log(self):
        now = time.time()
        print("Now is: %s / %s" % (now, time.strftime("%H:%M:%S")))

        # Make one DOWN/UP for the host
        self._make_down_up("test_host_0", datetime.datetime.utcfromtimestamp(now))

        print("----------")
        print("Request database logs")
        database = self.cfg_database
        collection = self.cfg_collection
        numlogs = self.livestatus_broker.db.conn[database][collection].count_documents({})
        print("- total logs count: %d" % numlogs)
        self.assertTrue(numlogs == 2)
        logs = self.livestatus_broker.db.conn[database][collection].find()
        print("- log 0: %s" % logs[0])
        self.assertTrue(logs[0]['state_type'] == 'SOFT')
        print("- log 1: %s" % logs[1])
        self.assertTrue(logs[1]['state_type'] == 'HARD')

        print("----------")
        print("Request logs for the host: test_host_9")
        request = """GET log
        Filter: host_name = test_host_9
        Columns: time type options state host_name
        OutputFormat: json"""
        tic = time.time()
        response, keepalive = self.livestatus_broker.livestatus.handle_request(request)
        tac = time.time()
        pyresponse = eval(response)
        print("Result: \n - # records matching the filter: %d\n - duration: %.2f" % (len(pyresponse), tac - tic))
        print("Response:")
        for item in pyresponse:
            print("- %s" % item)
        # No matching log!
        self.assertTrue(len(pyresponse) == 0)

        print("Request logs for the host: test_host_0")
        request = """GET log
        Filter: host_name = test_host_0
        Columns: time type options state host_name
        OutputFormat: json"""
        tic = time.time()
        response, keepalive = self.livestatus_broker.livestatus.handle_request(request)
        tac = time.time()
        pyresponse = eval(response)
        print("Result: \n - # records matching the filter: %d\n - duration: %.2f" % (len(pyresponse), tac - tic))
        print("Response:")
        for item in pyresponse:
            print("- %s" % item)
        # 2 matching logs!
        self.assertTrue(len(pyresponse) == 2)

        print("----------")
        print("Request logs in the current hour (from %s to %s)" % (int(now), int(now + 3600)))
        print("(from %s to %s)" % (time.asctime(time.localtime(int(now))),
                                   time.asctime(time.localtime(int(now + 3600)))))
        request = """GET log
        Filter: time >= """ + str(int(now)) + """
        Filter: time <= """ + str(int(now + 3600)) + """
        Columns: time type options state host_name
        OutputFormat: json"""
        tic = time.time()
        response, keepalive = self.livestatus_broker.livestatus.handle_request(request)
        tac = time.time()
        pyresponse = eval(response)
        print("Result: \n - # records matching the filter: %d\n - duration: %.2f"
              % (len(pyresponse), tac - tic))
        print("Response:")
        for item in pyresponse:
            print("- %s" % item)
        self.assertTrue(len(pyresponse) == 2)

    def test_several_log(self):
        now = time.time()
        print("Now is: %s / %s" % (now, time.strftime("%H:%M:%S")))
        print("Current hour (from %s to %s)" % (time.asctime(time.localtime(int(now))),
                                                time.asctime(time.localtime(int(now + 3600)))))

        # Make one DOWN/UP for the host
        self._make_down_up("test_host_0", datetime.datetime.utcfromtimestamp(now))
        self._make_down_up("test_router_0", datetime.datetime.utcfromtimestamp(now))

        print("----------")
        print("Requesting logs in the current hour...")
        request = """GET log
        Filter: time >= """ + str(int(now)) + """
        Filter: time <= """ + str(int(now + 3600)) + """
        Columns: time type options state host_name
        OutputFormat: json"""
        self._request(request, 4)

        print("----------")
        print("Requesting host alerts for test_host_0 and test_router_0...")
        request = """GET log
        Filter: time >= """ + str(int(now)) + """
        Filter: time <= """ + str(int(now + 3600)) + """
        Filter: type = HOST ALERT
        And: 3
        Filter: host_name = test_host_0
        Filter: host_name = test_router_0
        Or: 2
        And: 2
        Columns: time type options state host_name
        OutputFormat: json"""
        self._request(request, 4)

        print("----------")
        print("Requesting host alerts for test_host_0...")
        request = """GET log
        Filter: time >= """ + str(int(now)) + """
        Filter: time <= """ + str(int(now + 3600)) + """
        Filter: type = HOST ALERT
        And: 3
        Filter: host_name = test_host_0
        Or: 1
        And: 2
        Columns: time type options state host_name
        OutputFormat: json"""
        self._request(request, 2)

    def test_max_logs_age(self):
        # 1 - default
        db_module_conf = Module({
            'module_name': 'LogStore',
            'module_type': 'logstore_mongodb',
            'mongodb_uri': self.mongo_db_uri,
            'database': self.cfg_database,
            'collection': self.cfg_collection,
            'max_logs_age': '7'
        })

        livestatus_broker = LiveStatusLogStoreMongoDB(db_module_conf)
        self.assertEqual(7, livestatus_broker.max_logs_age)

        # 2 - days
        db_module_conf = Module({
            'module_name': 'LogStore',
            'module_type': 'logstore_mongodb',
            'mongodb_uri': self.mongo_db_uri,
            'database': self.cfg_database,
            'collection': self.cfg_collection,
            'max_logs_age': '7d'
        })

        livestatus_broker = LiveStatusLogStoreMongoDB(db_module_conf)
        self.assertEqual(7, livestatus_broker.max_logs_age)

        # 3 - weeks
        db_module_conf = Module({
            'module_name': 'LogStore',
            'module_type': 'logstore_mongodb',
            'mongodb_uri': self.mongo_db_uri,
            'database': self.cfg_database,
            'collection': self.cfg_collection,
            'max_logs_age': '1w'
        })

        livestatus_broker = LiveStatusLogStoreMongoDB(db_module_conf)
        self.assertEqual(7, livestatus_broker.max_logs_age)

        # 4 - months
        db_module_conf = Module({
            'module_name': 'LogStore',
            'module_type': 'logstore_mongodb',
            'mongodb_uri': self.mongo_db_uri,
            'database': self.cfg_database,
            'collection': self.cfg_collection,
            'max_logs_age': '3m'
        })

        livestatus_broker = LiveStatusLogStoreMongoDB(db_module_conf)
        self.assertEqual(3*31, livestatus_broker.max_logs_age)

        # 5 - years
        db_module_conf = Module({
            'module_name': 'LogStore',
            'module_type': 'logstore_mongodb',
            'mongodb_uri': self.mongo_db_uri,
            'database': self.cfg_database,
            'collection': self.cfg_collection,
            'max_logs_age': '7y'
        })

        livestatus_broker = LiveStatusLogStoreMongoDB(db_module_conf)
        self.assertEqual(7*365, livestatus_broker.max_logs_age)

    def test_replica_set(self):
        db_module_conf = Module({
            'module_name': 'LogStore',
            'module_type': 'logstore_mongodb',
            'mongodb_uri': self.mongo_db_uri,
            'database': self.cfg_database,
            'collection': self.cfg_collection,
            'replica_set': '1'
        })

        livestatus_broker = LiveStatusLogStoreMongoDB(db_module_conf)
        self.show_logs()

    def test_backlog(self):
        now = time.time()
        print("Now is: %s / %s" % (now, time.strftime("%H:%M:%S")))

        host = self.sched.hosts.find_by_name("test_host_0")
        assert host is not None, "Host test_host_0 is not known!"

        # Freeze the time !
        with freeze_time(datetime.datetime.utcfromtimestamp(now)) as frozen_datetime:
            # Time warp 1 hour in the past
            frozen_datetime.tick(delta=datetime.timedelta(hours=-1))
            # time_hacker.time_warp(-3600)

            host.state = 'DOWN'
            host.state_type = 'SOFT'
            host.attempt = 1
            host.output = "i am down"
            host.raise_alert_log_entry()
            # Time warp 1 hour
            frozen_datetime.tick(delta=datetime.timedelta(hours=1))

            # Here we have one broks for the module - simulate a DB failure to append to the back log!
            for brok in self.sched.brokers['Default-Broker']['broks']:
                brok.prepare()

                # Build a log line from the brok and append to the backlog!
                line = brok.data['log']
                log_line = Logline(line=line)
                values = log_line.as_dict()
                self.livestatus_broker.db.backlog.append(values)

            self.sched.brokers['Default-Broker']['broks'] = []

            host.state = 'UP'
            host.state_type = 'HARD'
            host.attempt = 1
            host.output = "i am up"
            host.raise_alert_log_entry()
            # Time warp 1 hour
            frozen_datetime.tick(delta=datetime.timedelta(hours=1))

            # Send broks to the module - only one brok is sent!
            self.update_broker()

        print("----------")
        print("Request database logs")
        database = self.cfg_database
        collection = self.cfg_collection
        numlogs = self.livestatus_broker.db.conn[database][collection].count_documents({})
        print("- total logs count: %d" % numlogs)
        self.assertTrue(numlogs == 2)
        logs = self.livestatus_broker.db.conn[database][collection].find()
        # First log is the 2nd event...
        print("Log 0: %s" % logs[0])
        self.assertTrue(logs[0]['state_type'] == 'HARD')
        # Second log is the 1st event...
        print("Log 1: %s" % logs[1])
        self.assertTrue(logs[1]['state_type'] == 'SOFT')
        # ... this because of the backlog cache!

@mock_livestatus_handle_request
class TestConfigBig(TestConfig):
    def setUp(self):
        setup_state_time = time.time()
        print("%s - starting setup..." % time.strftime("%H:%M:%S"))

        # self.setup_with_file('etc/shinken_1r_1h_1s.cfg')
        self.setup_with_file('etc/shinken_5r_100h_2000s.cfg')

        self.testid = str(os.getpid() + random.randint(1, 1000))
        print("%s - Initial setup duration: %.2f seconds" % (time.strftime("%H:%M:%S"),
                                                             time.time() - setup_state_time))

        self.cfg_database = 'test' + self.testid
        self.cfg_collection = 'ls-logs'

        dbmodconf = Module({
            'module_name': 'LogStore',
            'module_type': 'logstore_mongodb',
            'mongodb_uri': self.mongo_db_uri,
            'database': self.cfg_database,
            'collection': self.cfg_collection,
            'max_logs_age': '7d'
        })

        self.init_livestatus(dbmodconf=dbmodconf)
        print("%s - Initialized livestatus: %.2f seconds" % (time.strftime("%H:%M:%S"),
                                                             time.time() - setup_state_time))

        print("Requesting initial status broks...")
        self.sched.conf.skip_initial_broks = False
        self.sched.brokers['Default-Broker'] = {'broks': [], 'has_full_broks': False}
        self.sched.fill_initial_broks('Default-Broker')
        self.update_broker()
        print("%s - Initial setup duration: %.2f seconds" % (time.strftime("%H:%M:%S"),
                                                             time.time() - setup_state_time))

        # add use_aggressive_host_checking so we can mix exit codes 1 and 2
        # but still get DOWN state
        host = self.sched.hosts.find_by_name("test_host_000")
        # host = self.sched.hosts.find_by_name("test_host_0")
        host.__class__.use_aggressive_host_checking = 1

    # @pytest.mark.skip("Temp...")
    def test_a_long_history(self):
        # copied from test_livestatus_cache
        test_host_005 = self.sched.hosts.find_by_name("test_host_005")
        test_host_099 = self.sched.hosts.find_by_name("test_host_099")
        find = self.sched.services.find_srv_by_name_and_hostname
        test_ok_00 = find("test_host_005", "test_ok_00")
        test_ok_01 = find("test_host_005", "test_ok_01")
        test_ok_04 = find("test_host_005", "test_ok_04")
        test_ok_16 = find("test_host_005", "test_ok_16")
        test_ok_99 = find("test_host_099", "test_ok_01")

        print("----------")
        days = 4
        # todo: all this stuff does not look very time zone aware... naive dates :(
        etime = time.time()
        print("now it is", time.ctime(etime))
        etime_midnight = (etime - (etime % 86400)) + time.altzone
        print("midnight was", time.ctime(etime_midnight))
        query_start = etime_midnight - (days - 1) * 86400
        query_end = etime_midnight
        print("query_start", time.ctime(query_start))
        print("query_end ", time.ctime(query_end))
        print("----------")

        # Freeze the time !
        # initial_datetime = datetime.datetime(year=2018, month=6, day=1,
        #                                      hour=18, minute=30, second=0)
        print("%s - generating..." % time.strftime("%H:%M:%S"))
        initial_datetime = datetime.datetime.now()
        with freeze_time(initial_datetime) as frozen_datetime:
            # # Time warp 1 second
            # frozen_datetime.tick(delta=datetime.timedelta(seconds=1))
            #
            loops = int(86400 / 192)
            # Time warp N days back
            # time_hacker.time_warp(-1 * days * 86400)
            frozen_datetime.tick(delta=datetime.timedelta(days=-(days)))
            print("%s - time warp back to %s" % (time.strftime("%H:%M:%S"), time.strftime("%Y-%m-%d %H:%M:%S")))

            # run silently
            old_stdout = sys.stdout
            sys.stdout = open(os.devnull, "w")

            should_be = 0
            sys.stderr.write("%s - query_start: %s\n" % (time.strftime("%H:%M:%S"), time.ctime(query_start)))
            sys.stderr.write("%s - query_end: %s\n" % (time.strftime("%H:%M:%S"), time.ctime(query_end)))

            for day in xrange(days):
                # frozen_datetime.tick(delta=datetime.timedelta(days=1))
                # frozen_datetime.move_to(
                #     datetime.datetime(year=2, month=8, day=13, hour=14, minute=5, second=0))

                sys.stderr.write("%s - day %d started, it is %s and i run %d loops\n" % (
                    time.strftime("%H:%M:%S"), day, time.ctime(time.time()), loops))

                self.scheduler_loop(2, [
                    [test_ok_00, 0, "OK"],
                    [test_ok_01, 0, "OK"],
                    [test_ok_04, 0, "OK"],
                    [test_ok_16, 0, "OK"],
                    [test_ok_99, 0, "OK"],
                ])
                sys.stderr.write("%s - hosts are up\n" % (time.strftime("%H:%M:%S")))
                self.update_broker()

                # Some hosts change state
                # +1h, go down
                frozen_datetime.tick(delta=datetime.timedelta(minutes=60))
                self.scheduler_loop(3, [
                    [test_host_005, 2, "DOWN"],
                ])
                self.scheduler_loop(3, [
                    [test_host_099, 2, "DOWN"],
                ])
                sys.stderr.write("%s - hosts go down\n" % (time.strftime("%H:%M:%S")))

                # +1h, return back
                frozen_datetime.tick(delta=datetime.timedelta(minutes=60))
                self.scheduler_loop(3, [
                    [test_host_005, 0, "UP"],
                ])
                self.scheduler_loop(3, [
                    [test_host_099, 0, "UP"],
                ])
                sys.stderr.write("%s - hosts recover\n" % (time.strftime("%H:%M:%S")))
                self.update_broker()

                # Some services change state
                # +2h, go bad
                frozen_datetime.tick(delta=datetime.timedelta(minutes=120))
                self.scheduler_loop(3, [
                    [test_ok_00, 1, "WARN"],
                    [test_ok_01, 2, "CRIT"],
                ])
                # +1h, recover
                frozen_datetime.tick(delta=datetime.timedelta(minutes=60))
                self.scheduler_loop(1, [
                    [test_ok_00, 0, "OK"],
                    [test_ok_01, 0, "OK"],
                ])
                sys.stderr.write("%s - services changed and recovered\n" % (time.strftime("%H:%M:%S")))
                self.update_broker()

                # +1h, go bad
                frozen_datetime.tick(delta=datetime.timedelta(minutes=60))
                self.scheduler_loop(3, [
                    [test_ok_00, 1, "WARN"],
                    [test_ok_01, 2, "CRIT"],
                    [test_ok_04, 3, "UNKN"],
                    [test_ok_16, 1, "WARN"],
                    [test_ok_99, 2, "CRIT"],
                ])
                if query_start <= int(time.time()) <= query_end:
                    should_be += 3
                    sys.stderr.write("%s - now the result should be %s\n"
                                     % (time.strftime("%H:%M:%S"), should_be))

                # +1h, recover
                frozen_datetime.tick(delta=datetime.timedelta(minutes=60))
                self.scheduler_loop(2, [
                    [test_ok_00, 0, "OK"],
                    [test_ok_01, 0, "OK"],
                    [test_ok_04, 0, "OK"],
                    [test_ok_16, 0, "OK"],
                    [test_ok_99, 0, "OK"],
                ])
                if query_start <= int(time.time()) <= query_end:
                    should_be += 1
                    sys.stderr.write("%s - now the result should be %s\n"
                                     % (time.strftime("%H:%M:%S"), should_be))

                sys.stderr.write("%s - services changed and recovered\n" % (time.strftime("%H:%M:%S")))
                self.update_broker()

                sys.stderr.write("%s - day %d ended, it is %s\n" % (
                    time.strftime("%H:%M:%S"), day, time.ctime(time.time())))

                # Make the day have 24 hours -)
                frozen_datetime.tick(delta=datetime.timedelta(hours=17))

                self.livestatus_broker.db.commit()

            sys.stdout.close()
            sys.stdout = old_stdout
        print("%s - generated" % time.strftime("%H:%M:%S"))

        self.livestatus_broker.db.commit_and_rotate_log_db(forced=True)

        database = self.cfg_database
        collection = self.cfg_collection
        numlogs = self.livestatus_broker.db.conn[database][collection].count_documents({})
        print("%s - logs count: %d" % (time.strftime("%H:%M:%S"), numlogs))

        # now we have a lot of events
        # find type = HOST ALERT or SERVICE ALERT for test_host_099, service test_ok_01
        columns = (
            "class time type state host_name service_description plugin_output message options "
            "contact_name command_name state_type current_host_groups current_service_groups"
        )
        request = """GET log
Columns: """ + columns + """
Filter: time >= """ + str(int(query_start)) + """
Filter: time <= """ + str(int(query_end)) + """
Filter: type = SERVICE ALERT
And: 1
Filter: type = HOST ALERT
And: 1
Filter: type = SERVICE FLAPPING ALERT
Filter: type = HOST FLAPPING ALERT
Filter: type = SERVICE DOWNTIME ALERT
Filter: type = HOST DOWNTIME ALERT
Filter: type ~ starting...
Filter: type ~ shutting down...
Or: 8
Filter: host_name = test_host_099
Filter: service_description = test_ok_01
And: 5
OutputFormat: json"""
        print("\n-----\nRequest: %s" % request)
        # Mongo filter is
        # '$and' : [
        # { 'service_description' : 'test_ok_01' },
        # { 'host_name' : 'test_host_099' },
        # { '$or' : [
        # { 'type' : { '$regex' : 'shutting down...' } },
        # { 'type' : { '$regex' : 'starting...' } },
        # { 'type' : 'HOST DOWNTIME ALERT' },
        # { 'type' : 'SERVICE DOWNTIME ALERT' },
        # { 'type' : 'HOST FLAPPING ALERT' },
        # { 'type' : 'SERVICE FLAPPING ALERT' },
        # { 'type' : 'HOST ALERT' },
        # { 'type' : 'SERVICE ALERT' }
        # ] },
        # { 'time' : { '$lte' : 1575331200 } },
        # { 'time' : { '$gte' : 1575072000 } }]

        tic = time.time()
        response, keepalive = self.livestatus_broker.livestatus.handle_request(request)
        tac = time.time()
        pyresponse = eval(response)
        print("Result: \n - # records with test_host_099/test_ok_01: %d\n - duration: %.2f"
              % (len(pyresponse), tac - tic))
        for item in pyresponse:
            print("- %s" % item)
        self.assertTrue(len(pyresponse) == should_be)

        # and now test Negate:
        request = """GET log
Filter: time >= """ + str(int(query_start)) + """
Filter: time <= """ + str(int(query_end)) + """
Filter: type = SERVICE ALERT
And: 1
Filter: type = HOST ALERT
And: 1
Filter: type = SERVICE FLAPPING ALERT
Filter: type = HOST FLAPPING ALERT
Filter: type = SERVICE DOWNTIME ALERT
Filter: type = HOST DOWNTIME ALERT
Filter: type ~ starting...
Filter: type ~ shutting down...
Or: 8
Filter: host_name = test_host_099
Filter: service_description = test_ok_01
And: 2
Negate:
And: 2
OutputFormat: json"""
        print("\n-----\nRequest: %s" % request)
        # Mongo filter is
        # '$and' : [
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # { '$and' : [
        #   { 'time' : { '$exists' : True } },
        #   { '$and' : [
        #     { 'service_description' : 'test_ok_01' },
        #     { 'host_name' : 'test_host_099' }
        #   ] }
        # ] },
        # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        # { '$or' : [
        # { 'type' : { '$regex' : 'shutting down...' } },
        # { 'type' : { '$regex' : 'starting...' } },
        # { 'type' : 'HOST DOWNTIME ALERT' },
        # { 'type' : 'SERVICE DOWNTIME ALERT' },
        # { 'type' : 'HOST FLAPPING ALERT' },
        # { 'type' : 'SERVICE FLAPPING ALERT' },
        # { 'type' : 'HOST ALERT' },
        # { 'type' : 'SERVICE ALERT' }
        # ] },
        # { 'time' : { '$lte' : 1575331200 } },
        # { 'time' : { '$gte' : 1575072000 } }]

        tic = time.time()
        response, keepalive = self.livestatus_broker.livestatus.handle_request(request)
        tac = time.time()
        notpyresponse = eval(response)
        print("Result: \n - # records without test_host_099/test_ok_01: %d\n - duration: %.2f"
              % (len(notpyresponse), tac - tic))

        request = """GET log
Filter: time >= """ + str(int(query_start)) + """
Filter: time <= """ + str(int(query_end)) + """
Filter: type = SERVICE ALERT
And: 1
Filter: type = HOST ALERT
And: 1
Filter: type = SERVICE FLAPPING ALERT
Filter: type = HOST FLAPPING ALERT
Filter: type = SERVICE DOWNTIME ALERT
Filter: type = HOST DOWNTIME ALERT
Filter: type ~ starting...
Filter: type ~ shutting down...
Or: 8
OutputFormat: json"""
        print("\n-----\nRequest all events: %s" % request)
        # Mongo filter is
        # '$and' : [
        # { '$or' : [
        # { 'type' : { '$regex' : 'shutting down...' } },
        # { 'type' : { '$regex' : 'starting...' } },
        # { 'type' : 'HOST DOWNTIME ALERT' },
        # { 'type' : 'SERVICE DOWNTIME ALERT' },
        # { 'type' : 'HOST FLAPPING ALERT' },
        # { 'type' : 'SERVICE FLAPPING ALERT' },
        # { 'type' : 'HOST ALERT' },
        # { 'type' : 'SERVICE ALERT' }
        # ] },
        # { 'time' : { '$lte' : 1575331200 } },
        # { 'time' : { '$gte' : 1575072000 } }]

        tic = time.time()
        response, keepalive = self.livestatus_broker.livestatus.handle_request(request)
        tac = time.time()
        allpyresponse = eval(response)
        print("Result: \n - # records: %d\n - duration: %.2f"
              % (len(allpyresponse), tac - tic))
        # FIXME: assertion should be true but the Negate is not functional!
        # self.assertTrue(len(allpyresponse) == len(notpyresponse) + len(pyresponse))
        print("\n-----\nFIXME: assertion should be true but the Negate is not functional !\n-----\n")

        # Now a pure class check query
        request = """GET log
Filter: time >= """ + str(int(query_start)) + """
Filter: time <= """ + str(int(query_end)) + """
Filter: class = 1
OutputFormat: json"""
        tic = time.time()
        response, keepalive = self.livestatus_broker.livestatus.handle_request(request)
        tac = time.time()
        allpyresponse = eval(response)
        print("Result: \n - # records: %d\n - duration: %.2f"
              % (len(allpyresponse), tac - tic))
        # FIXME: assertion should be true but the Negate is not functional!
        # self.assertTrue(len(allpyresponse) == len(notpyresponse) + len(pyresponse))
        print("\n-----\nFIXME: assertion should be true but the Negate is not functional !\n-----\n")

        # numlogs = self.livestatus_broker.db.conn[database][collection].find().count()
        numlogs = self.livestatus_broker.db.conn[database][collection].count_documents({})
        times = [x['time'] for x in self.livestatus_broker.db.conn[database][collection].find()]
        print("Whole database: %d - %s - %s" % (numlogs, min(times), max(times)))
        self.assertTrue(times != [])
        numlogs = self.livestatus_broker.db.conn[database][collection].count_documents({
            '$and': [
                {'time': {'$gt': min(times)}},
                {'time': {'$lte': max(times)}}
            ]})
        now = max(times)
        print("Filter database: %d - %s - %s" % (numlogs, min(times), max(times)))
        daycount = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        for day in xrange(25):
            one_day_earlier = now - 3600*24
            numlogs = self.livestatus_broker.db.conn[database][collection].count_documents({
                '$and': [
                    {'time': {'$gt': one_day_earlier}},
                    {'time': {'$lte': now}}
                ]})
            daycount[day] = numlogs
            print("day -%02d %d..%d - %d" % (day, one_day_earlier, now, numlogs))
            now = one_day_earlier

        # now delete too old entries from the database (> 14days)
        # that's the job of commit_and_rotate_log_db()
        self.livestatus_broker.db.commit_and_rotate_log_db(forced=True)

        now = max(times)
        print("Filter database (after log rotation): %d - %s - %s" % (numlogs, min(times), max(times)))
        for day in xrange(25):
            one_day_earlier = now - 3600*24
            numlogs = self.livestatus_broker.db.conn[database][collection].count_documents({
                '$and': [
                    {'time': {'$gt': one_day_earlier}},
                    {'time': {'$lte': now}}
                ]})
            print("day -%02d %d..%d - %d" % (day, one_day_earlier, now, numlogs))
            now = one_day_earlier

        numlogs = self.livestatus_broker.db.conn[database][collection].count_documents({})
        # simply an estimation. the cleanup-routine in the mongodb logstore
        # cuts off the old data at midnight, but here in the test we have
        # only accuracy of a day.
        print("After: %d - %s - %s" % (numlogs, sum(daycount[:7]), sum(daycount[:8])))

        self.assertTrue(numlogs >= sum(daycount[:7]))
        self.assertTrue(numlogs <= sum(daycount[:8]))

        time_hacker.set_my_time()
