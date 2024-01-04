from __future__ import annotations

import hmac
import json
import os
from queue import PriorityQueue
from typing import Any


class UniquePriorityQueue(PriorityQueue):
    def _init(self, maxsize):
        super()._init(maxsize)
        self.values = set()

    def _put(self, item: tuple[int, Any]):
        if item[1] not in self.values:
            self.values.add(item[1])
            super()._put(item)

    def _get(self):
        item = super()._get()
        self.values.remove(item[1])
        return item


class HMAC_anonymiser:
    def __init__(self, key: str | None = None, key_env_var_name: str = "HMAC_KEY", anon_map: dict | None = None):
        if key is None:
            key = os.environ[key_env_var_name]
        self.key = bytes.fromhex(key)
        self.anon_map: dict[str, str] = {} if anon_map is None else anon_map

    def anonymise(self, data: int | str | None, safe: bool = False) -> str | None:
        """Anonymise the provided data.

        Parameters
        ----------
        data : int | str | None
            Input data. If None, the function simply returns None.
        safe : bool, optional
            Whether the anonymiser should first check that the input data are not the
            result of a previous anonymisation. False by default.

        Returns
        -------
        str
            Anonymised data.
        """
        if data is not None:
            if not safe or data not in self.inverse_anon_map:
                data_str = str(data)
                data = self.anon_map.get(data_str)
                if data is None:
                    data = hmac.digest(self.key, data_str.encode('utf-8', 'surrogatepass'), 'sha256').hex()
                    self.anon_map[data_str] = data
        return data

    def update_from_disk(self, save_path):
        if save_path.exists():
            self.anon_map.update(json.loads(save_path.read_text()))

    def save_map(self, save_path):
        save_path.parent.mkdir(exist_ok=True, parents=True)
        save_path.write_text(json.dumps(self.anon_map))

    @property
    def inverse_anon_map(self) -> dict[str, str]:
        return {value: key for key, value in self.anon_map.items()}
