import logging
from time import time

from src.common.base_module import BaseModule
import g


class TestModule(BaseModule):
    def __init__(self):
        self.log = logging.getLogger('TestModule')
        self.log.setLevel("DEBUG")

        # just for testing purposes - as new value for test_attrib
        self.counter = 0

        g.td.register_handler(
            self.processing_func_test_attrib,  # function (or bound method) to call
            'ip',                  # entity type
            ('test_timestamp', ),  # tuple/list/set of attributes to watch (their update triggers call of the registered method)
            ('test_attrib', )     # tuple/list/set of attributes the method may change
        )
        g.td.register_handler(
            self.processing_func_timestamp,  # function (or bound method) to call
            'ip',  # entity type
            ('test_list',), # tuple/list/set of attributes to watch (their update triggers call of the registered method)
            ('test_timestamp',)  # tuple/list/set of attributes the method may change
        )

    def processing_func_timestamp(self, etype, ekey, record, updates):
        """
        :param etype: entity type
        :param ekey: entity identificator
        :param record: instance of Record as database record cache
        :param updates: list of all attributes whose update triggered this call and
          their new values (or events and their parameters) as a list of 3-tuples: [(attr, val, old_val), (!event, param), ...]
        :return: new request updates
        """
        print("Hello from TestModule - processing_func_timestamp")
        current_time = time()
        return [('set', 'test_timestamp', current_time)]

    def processing_func_test_attrib(self, etype, ekey, record, updates):
        """
        :param etype: entity type
        :param ekey: entity identificator
        :param record: instance of Record as database record cache
        :param updates: list of all attributes whose update triggered this call and
          their new values (or events and their parameters) as a list of 3-tuples: [(attr, val, old_val), (!event, param), ...]
        :return: new request updates
        """
        print("Hello from TestModule - processing_func_attrib")
        self.counter += 1
        return [('set', 'test_attrib', self.counter)]
