#!/usr/bin/python
# -*- coding: utf-8 -*-

# Copyright (C) 2009-2012:
#    Gabes Jean, naparuba@gmail.com
#    Gerhard Lausser, Gerhard.Lausser@consol.de
#    Gregory Starck, g.starck@gmail.com
#    Hartmut Goebel, h.goebel@goebel-consult.de
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

# import von modules/livestatus_logstore

"""
This class is for attaching a mongodb database to a livestatus broker module.
It is one possibility for an exchangeable storage for log broks
"""

import time
import datetime
import re
from pprint import pprint

import pymongo
from pymongo import MongoClient
from pymongo.errors import AutoReconnect

from shinken.modulesctx import modulesctx
from shinken.basemodule import BaseModule
from shinken.log import logger
from shinken.util import to_bool


# Import a class from the livestatus module, should be already loaded!
livestatus_broker = modulesctx.get_module('livestatus')
LiveStatusStack = livestatus_broker.LiveStatusStack
LOGCLASS_INVALID = livestatus_broker.LOGCLASS_INVALID
Logline = livestatus_broker.Logline

#############################################################################

DEFAULT_LOGS_AGE = 7

CONNECTED = 1
DISCONNECTED = 2
SWITCHING = 3

#############################################################################

properties = {
    'daemons': ['livestatus'],
    'type': 'logstore_mongodb',
    'external': False,
    'phases': ['running'],
}


# called by the plugin manager
def get_instance(plugin):
    logger.info("[LogstoreMongoDB] Get an LogStore MongoDB module for plugin %s", plugin.get_name())
    instance = LiveStatusLogStoreMongoDB(plugin)
    return instance


# def row_factory(cursor, row):
#     """Handler for the sqlite fetch method."""
#     return Logline(cursor.description, row)


class LiveStatusLogStoreError(Exception):
    pass


class LiveStatusLogStoreMongoDB(BaseModule):
    def __init__(self, modconf):
        BaseModule.__init__(self, modconf)
        self.plugins = []
        # mongodb://host1,host2,host3/?safe=true;w=2;wtimeoutMS=2000
        self.mongodb_uri = getattr(modconf, 'mongodb_uri', None)
        self.replica_set = getattr(modconf, 'replica_set', None)
        if self.replica_set:
            logger.warning('[LogStoreMongoDB] the parameter replica_set will be ignored. Use a mongodb uri instead.')

        self.database = getattr(modconf, 'database', 'shinken')
        logger.info("[LogStore MongoDB] database name: %s", self.database)
        self.collection = getattr(modconf, 'collection', 'ls-logs')
        logger.info("[LogStore MongoDB] collection name: %s", self.collection)

        self.mongodb_fsync = to_bool(getattr(modconf, 'mongodb_fsync', "True"))

        self.max_logs_age = getattr(modconf, 'max_logs_age', DEFAULT_LOGS_AGE)
        maxmatch = re.match(r'^(\d+)([dwmy]*)$', str(self.max_logs_age))
        if maxmatch is None:
            logger.warning('[LogStoreMongoDB] Wrong format for max_logs_age. '
                           'Must be <number>[d|w|m|y] or <number> and not %s', max_logs_age)
            return
        if not maxmatch.group(2):
            self.max_logs_age = int(maxmatch.group(1))
        elif maxmatch.group(2) == 'd':
            self.max_logs_age = int(maxmatch.group(1))
        elif maxmatch.group(2) == 'w':
            self.max_logs_age = int(maxmatch.group(1)) * 7
        elif maxmatch.group(2) == 'm':
            self.max_logs_age = int(maxmatch.group(1)) * 31
        elif maxmatch.group(2) == 'y':
            self.max_logs_age = int(maxmatch.group(1)) * 365
        logger.info("[LogStore MongoDB] maximum log age: %d days", self.max_logs_age)

        self.use_aggressive_sql = (getattr(modconf, 'use_aggressive_sql', '1') == '1')
        logger.info("[LogStore MongoDB] agressive SQL: %s", self.use_aggressive_sql)
        self.is_connected = DISCONNECTED
        self.backlog = []

        self.app = None
        self.conn = None
        self.db = None
        self.next_log_db_rotate = time.time()
        self.mongo_filter_stack = None
        self.mongo_time_filter_stack = None

        # Now sleep one second, so that won't get lineno collisions with the last second
        time.sleep(1)
        Logline.lineno = 0

    def load(self, app):
        self.app = app

    def init(self):
        pass

    def do_loop_turn(self):
        return True

    def open(self):
        # This stack is used to create a full-blown select-statement
        self.mongo_filter_stack = LiveStatusMongoStack()
        # This stack is used to create a minimal select-statement which
        # selects only by time >= and time <=
        self.mongo_time_filter_stack = LiveStatusMongoStack()
        try:
            self.conn = MongoClient(self.mongodb_uri)
            self.db = self.conn[self.database]
            # Former indexes that are not the best ever :/
            # self.db[self.collection].ensure_index(
            #     [
            #         ('host_name', pymongo.ASCENDING),
            #         ('time', pymongo.DESCENDING)
            #     ], name='logs_idx')
            # self.db[self.collection].ensure_index(
            #     [
            #         ('time', pymongo.ASCENDING),
            #         ('lineno', pymongo.ASCENDING)
            #     ], name='time_1_lineno_1')
            self.db[self.collection].create_index(
                [
                    ('host_name', pymongo.ASCENDING)
                ], name='hostname')
            self.db[self.collection].create_index(
                [
                    ('time', pymongo.DESCENDING)
                ], name='time')
            self.db[self.collection].create_index(
                [
                    ('host_name', pymongo.ASCENDING),
                    ('time', pymongo.DESCENDING)
                ], name='hostname_time')

            self.is_connected = CONNECTED
            self.next_log_db_rotate = time.time()
        except AutoReconnect as err:
            # now what, ha?
            logger.error("[LogStoreMongoDB] LiveStatusLogStoreMongoDB.AutoReconnect %s", err)
            # The mongodb is hopefully available until this module is restarted
            raise LiveStatusLogStoreError(err)
        except Exception as err:
            logger.error("[LogStoreMongoDB] Could not open the database: %s", err)
            raise LiveStatusLogStoreError(err)

    def close(self):
        pass

    def commit(self):
        pass

    def commit_and_rotate_log_db(self, forced=False):
        """For a MongoDB there is no rotate, but we will delete old contents."""
        now = time.time()

        if not forced and self.next_log_db_rotate > now:
            return

        today = datetime.date.today()
        today0000 = datetime.datetime(today.year, today.month, today.day, 0, 0, 0)
        today0005 = datetime.datetime(today.year, today.month, today.day, 0, 5, 0)
        oldest = today0000 - datetime.timedelta(days=self.max_logs_age)
        self.db[self.collection].delete_many({u'time': {'$lt': time.mktime(oldest.timetuple())}})

        if now < time.mktime(today0005.timetuple()):
            next_rotation = today0005
        else:
            next_rotation = today0005 + datetime.timedelta(days=1)

        # See you tomorrow
        self.next_log_db_rotate = time.mktime(next_rotation.timetuple())
        logger.info("[LogStoreMongoDB] Next log rotation at %s ",
                    time.asctime(time.localtime(self.next_log_db_rotate)))

    def manage_log_brok(self, b):
        data = b.data
        line = data['log']
        if re.match(r"^\[[0-9]*\] [A-Z][a-z]*.:", line):
            # Match log which NOT have to be stored
            # print "Unexpected in manage_log_brok", line
            return

        log_line = Logline(line=line)
        values = log_line.as_dict()
        if log_line.logclass == LOGCLASS_INVALID:
            logger.debug("[LogStoreMongoDB] This line is invalid: %s", line)
            return
        logger.debug("[LogStoreMongoDB] a new line: %s", values)

        try:
            self.db[self.collection].insert_one(values)
            self.is_connected = CONNECTED
            if self.backlog:
                # If we have a backlog from an outage, we flush these lines
                # First we make a copy, so we can delete elements from
                # the original self.backlog
                backlog_lines = [bl for bl in self.backlog]
                for backlog_line in backlog_lines:
                    try:
                        self.db[self.collection].insert_one(backlog_line)
                        self.backlog.remove(backlog_line)
                    except AutoReconnect:
                        self.is_connected = SWITCHING
                    except Exception as exp:
                        logger.error("[LogStoreMongoDB] Got an exception inserting the backlog: %s", exp)
        except AutoReconnect:
            if self.is_connected != SWITCHING:
                self.is_connected = SWITCHING
                time.sleep(5)
                # Under normal circumstances after these 5 seconds
                # we should have a new primary node
            else:
                # Not yet? Wait, but try harder.
                time.sleep(0.1)
            # At this point we must save the logline for a later attempt
            # After 5 seconds we either have a successful write
            # or another exception which means, we are disconnected
            self.backlog.append(values)
        except Exception as exp:
            self.is_connected = DISCONNECTED
            logger.error("[LogStoreMongoDB] Database error occurred: %s", exp)
        # FIXME need access to this #self.livestatus.count_event('log_message')

    def add_filter(self, operator, attribute, reference):
        if attribute == 'time':
            self.mongo_time_filter_stack.put_stack(
                self.make_mongo_filter(operator, attribute, reference))
        self.mongo_filter_stack.put_stack(self.make_mongo_filter(operator, attribute, reference))

    def add_filter_and(self, andnum):
        self.mongo_filter_stack.and_elements(andnum)

    def add_filter_or(self, ornum):
        self.mongo_filter_stack.or_elements(ornum)

    def add_filter_not(self):
        self.mongo_filter_stack.not_elements()

    def get_live_data_log(self):
        """Like get_live_data, but for log objects"""

        if not self.is_connected == CONNECTED:
            logger.warning("[LogStoreMongoDB] sorry, not connected")
            return []

        # finalize the filter stacks
        self.mongo_time_filter_stack.and_elements(self.mongo_time_filter_stack.qsize())
        self.mongo_filter_stack.and_elements(self.mongo_filter_stack.qsize())
        if self.use_aggressive_sql:
            # Be aggressive, get preselected data from sqlite and do less
            # filtering in python. But: only a subset of Filter:-attributes
            # can be mapped to columns in the logs-table, for the others
            # we must use "always-true"-clauses. This can result in
            # funny and potentially ineffective sql-statements
            mongo_filter_func = self.mongo_filter_stack.get_stack()
        else:
            # Be conservative, get everything from the database between
            # two dates and apply the Filter:-clauses in python
            mongo_filter_func = self.mongo_time_filter_stack.get_stack()

        mongo_filter = mongo_filter_func()
        logger.debug("[Logstore MongoDB] Mongo filter is %s", str(mongo_filter))
        # We can apply the filter_stack here as well. we have columns and filter_columns.
        # the only additional step is to enrich log lines with host/service-attributes
        # A time range can be useful for a faster preselection of lines

        # pylint: disable=eval-used
        filter_element = eval('{ ' + mongo_filter + ' }')
        logger.debug("[LogstoreMongoDB] Mongo filter is %s", str(filter_element))

        print("[LogstoreMongoDB] Mongo filter is:")
        pprint(filter_element)

        columns = [
            'logobject', 'attempt', 'logclass', 'command_name', 'comment', 'contact_name',
            'host_name', 'message', 'plugin_output', 'service_description',
            'state', 'state_type', 'time', 'type'
        ]

        # Remove the sorting
        # db_result = [
        #     Logline([(c,) for c in columns], [x[col] for col in columns])
        #     for x in self.db[self.collection].find(filter_element).sort(
        #         [(u'time', pymongo.DESCENDING)])
        # ]
        db_count = self.db[self.collection].count_documents(filter_element)
        print("[LogstoreMongoDB] Mongo filtered collection count is: %d" % db_count)

        db_result = [
            Logline([(c,) for c in columns], [x[col] for col in columns])
            for x in self.db[self.collection].find(filter_element)
        ]
        print("[LogstoreMongoDB] Mongo db_result: %d" % len(db_result))
        for x in db_result:
            print("-: %s / %s" % (type(x), x))
        return db_result

    def make_mongo_filter(self, operator, attribute, reference):
        # The filters are text fragments which are put together to form a
        # sql where-condition finally.
        # Add parameter Class (Host, Service), lookup datatype (default string), convert reference
        # which attributes are suitable for a sql statement

        good_attributes = [
            'time', 'attempt', 'logclass', 'command_name', 'comment', 'contact_name', 'message',
            'host_name', 'plugin_output', 'service_description', 'state', 'state_type', 'type']
        # good_operators = ['=', '!=']
        # string fields for the query
        string_attributes = [
            'command_name', 'comment', 'contact_name', 'host_name', 'message', 'plugin_output',
            'service_description', 'state_type', 'type']
        if attribute in string_attributes:
            reference = "'%s'" % reference

        # We should change the "class" query into the internal "logclass" attribute
        if attribute == 'class':
            attribute = 'logclass'

        def eq_filter():
            if not reference:
                return '\'%s\' : \'\'' % (attribute)
            return '\'%s\' : %s' % (attribute, reference)

        def match_filter():
            return '\'%s\' : { \'$regex\' : %s }' % (attribute, reference)

        def eq_nocase_filter():
            if not reference:
                return '\'%s\' : \'\'' % (attribute)
            return '\'%s\' : { \'$regex\' : %s, \'$options\' : \'i\' }' % (attribute, '^' + reference + '$')

        def match_nocase_filter():
            return '\'%s\' : { \'$regex\' : %s, \'$options\' : \'i\' }' \
                   % (attribute, reference)

        def lt_filter():
            return '\'%s\' : { \'$lt\' : %s }' % (attribute, reference)

        def gt_filter():
            return '\'%s\' : { \'$gt\' : %s }' % (attribute, reference)

        def le_filter():
            return '\'%s\' : { \'$lte\' : %s }' % (attribute, reference)

        def ge_filter():
            return '\'%s\' : { \'$gte\' : %s }' % (attribute, reference)

        def ne_filter():
            if not reference:
                return '\'%s\' : { \'$ne\' : '' }' % (attribute)
            return '\'%s\' : { \'$ne\' : %s }' % (attribute, reference)

        def not_match_filter():
            # From http://myadventuresincoding.wordpress.com/
            # 2011/05/19/mongodb-negative-regex-query-in-mongo-shell/
            return '\'%s\' : { \'$regex\' : %s }' % (attribute, '^((?!' + reference + ').)')

        def ne_nocase_filter():
            if not reference:
                return '\'%s\' : \'\'' % (attribute)

            return '\'%s\' : { \'$regex\' : %s, \'$options\' : \'i\' }' % (attribute, '^((?!' + reference + ').)')

        def not_match_nocase_filter():
            return '\'%s\' : { \'$regex\' : %s, \'$options\' : \'i\' }' % (attribute, '^((?!' + reference + ').)')

        def no_filter():
            return '\'time\' : { \'$exists\' : True }'

        if attribute not in good_attributes:
            return no_filter

        if operator == '=':
            return eq_filter
        elif operator == '~':
            return match_filter
        elif operator == '=~':
            return eq_nocase_filter
        elif operator == '~~':
            return match_nocase_filter
        elif operator == '<':
            return lt_filter
        elif operator == '>':
            return gt_filter
        elif operator == '<=':
            return le_filter
        elif operator == '>=':
            return ge_filter
        elif operator == '!=':
            return ne_filter
        elif operator == '!~':
            return not_match_filter
        elif operator == '!=~':
            return ne_nocase_filter
        elif operator == '!~~':
            return not_match_nocase_filter


class LiveStatusMongoStack(LiveStatusStack):
    """A Lifo queue for filter functions.

    This class inherits either from MyLifoQueue or Queue.LifoQueue
    whatever is available with the current python version.

    Public functions:
    and_elements -- takes a certain number (given as argument)
    of filters from the stack, creates a new filter and puts
    this filter on the stack. If these filters are lambda functions,
    the new filter is a boolean and of the underlying filters.
    If the filters are sql where-conditions, they are also concatenated
    with and to form a new string containing a more complex where-condition.

    or_elements --- the same, only that the single filters are
    combined with a logical or.

    """

    def __init__(self, *args, **kw):
        self.type = 'mongo'
        self.__class__.__bases__[0].__init__(self, *args, **kw)

    def not_elements(self):
        # top_filter = self.get_stack()
        # negate_filter = lambda: '\'$not\': { %s }' % top_filter()
        # mongodb doesn't have the not-operator like sql, which can negate
        # a complete expression. Mongodb $not can only reverse one operator
        # at a time. This would require rewriting of the whole expression.
        # So instead of deciding whether a record can pass the filter or not,
        # we let it pass in any case. That's no problem, because the result
        # of the database query will have to go through the in-memory-objects
        # filter too.
        # todo: check if all this speech is really true! Obviously not because of the current unit tests results!!!
        negate_filter = lambda: '\'time\' : { \'$exists\' : True }'

        self.put_stack(negate_filter)

    def and_elements(self, num):
        """Take num filters from the stack, and append them and return the result"""
        if num > 1:
            filters = []
            for _ in range(num):
                filters.append(self.get_stack())
            # Take from the stack:
            # Make a combined anded function
            # Put it on the stack
            logger.debug("[Logstore MongoDB] Filter is %s", str(filters))
            and_clause = lambda: '\'$and\' : [%s]' % ', '.join('{ ' + x() + ' }' for x in filters)
            logger.debug("[Logstore MongoDB] and_elements %s", str(and_clause))
            self.put_stack(and_clause)

    def or_elements(self, num):
        """Take num filters from the stack, or them and put the result back"""
        if num > 1:
            filters = []
            for _ in range(num):
                filters.append(self.get_stack())
            or_clause = lambda: '\'$or\' : [%s]' % ', '.join('{ ' + x() + ' }' for x in filters)
            self.put_stack(or_clause)

    def get_stack(self):
        """Return the top element from the stack or a filter which is always true"""
        if self.qsize():
            return self.get()
        return lambda: ''
