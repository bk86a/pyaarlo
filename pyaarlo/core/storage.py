import fnmatch
import pickle
import os
import pprint
from typing import Union

from .cfg import ArloCfg
from .logger import ArloLogger


class ArloStorage:
    
    def __init__(self, cfg: ArloCfg, log: ArloLogger):
        self._cfg: ArloCfg = cfg
        self._log: ArloLogger = log

        self._state_file: Union[str, None] = self._cfg.state_file
        self._db = {}

        self._create_storage_directory()
        self.load()

        self._log.debug("storage: created")

    def _create_storage_directory(self):
        """Create storage area.
        """
        try:
            if not os.path.exists(self._cfg.storage_dir):
                os.mkdir(self._cfg.storage_dir)
        except Exception as _e:
            self._log.warning(f"Problem creating {self._cfg.storage_dir}")

    def _ekey(self, key):
        return key if not isinstance(key, list) else "/".join(key)

    def _keys_matching(self, key):
        mkeys = []
        ekey = self._ekey(key)
        # Use a list copy of keys to be thread-safe against concurrent modifications
        for mkey in list(self._db.keys()):
            if fnmatch.fnmatch(mkey, ekey):
                mkeys.append(mkey)
        return mkeys

    def load(self):
        if self._state_file is not None:
            try:
                with open(self._state_file, "rb") as dump:
                    self._db = pickle.load(dump)
            except Exception:
                self._log.debug("storage: file not read")

    def save(self):
        if self._state_file is not None:
            try:
                # Shallow copy the dict atomically to prevent iteration errors during pickle
                db_copy = self._db.copy()
                with open(self._state_file, "wb") as dump:
                    pickle.dump(db_copy, dump)
            except Exception:
                self._log.warning("storage: file not written")

    def file_name(self):
        return self._state_file

    def get(self, key, default=None):
        ekey = self._ekey(key)
        return self._db.get(ekey, default)

    def get_matching(self, key, default=None):
        gets = []
        for mkey in self._keys_matching(key):
            gets.append((mkey, self._db.get(mkey, default)))
        return gets

    def keys_matching(self, key):
        return self._keys_matching(key)

    def set(self, key, value, prefix=""):
        ekey = self._ekey(key)
        output = "set:" + ekey + "=" + str(value)
        self._log.vdebug(f"{prefix}: {output[:80]}")
        self._db[ekey] = value
        return value

    def unset(self, key):
        ekey = self._ekey(key)
        if ekey in self._db:
            del self._db[ekey]

    def clear(self):
        self._db = {}

    def dump(self):
        pprint.pprint(self._db.copy())
