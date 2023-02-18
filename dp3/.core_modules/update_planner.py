"""
Core module for dp3 - adds NRU (next regular update) fields to the newly added entity.
Updater uses NRU fields to issue corresponding regular updates at the specified time.
Various modules may hook their functions to the regular updates.

Note:
    Core modules needs to be enabled in worker.py and not in config!
"""
import logging
from datetime import timedelta

from dp3 import g
from dp3.common.base_module import BaseModule


class UpdatePlanner(BaseModule):
    def __init__(self):
        # the name of logger should be the same as the name of module itself
        self.log = logging.getLogger("UpdatePlanner")
        # logging level depends on user (DEBUG is useful while the module is tested or implemented)
        self.log.setLevel("DEBUG")

        # register handler for every etype
        for etype in g.model_spec:
            g.td.register_handler(
                self.processing_function,  # function to call (callback)
                etype,  # entity type, which actions will be watched (e.g. "ip")
                # tuple/list/set of attributes to watch
                # (their update triggers call of the registered method)
                ("!NEW",),
                ("_nru1d", "_nru1w"),  # tuple/list/set of attributes the method may change
            )
            self.log.debug(f"Registred handler for {etype}.")

    def processing_function(self, etype, ekey, record, updates):
        """
        Args:
            etype: entity type (e.g. 'ip')
            ekey: entity identificator (e.g. "1.2.3.4")
            record: instance of Record as database record cache
            updates: list of all attributes whose update triggered this call and
                their new values (or events and their parameters) as a list of 3-tuples:
                [(attr, new_val, old_val), (!event, param), ...]
        Returns:
             new request updates
        """

        return [
            {"op": "set", "attr": "_nru1d", "val": record["ts_added"] + timedelta(days=1)},
            {"op": "set", "attr": "_nru1w", "val": record["ts_added"] + timedelta(days=7)},
        ]
