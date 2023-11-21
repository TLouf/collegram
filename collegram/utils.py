from __future__ import annotations

import hmac
import json
import os


class HMAC_anonymiser:
    def __init__(self, key: str | None = None, key_env_var_name: str = "HMAC_KEY", anon_map: dict | None = None):
        if key is None:
            key = os.environ[key_env_var_name]
        self.key = bytes.fromhex(key)
        self.anon_map = {} if anon_map is None else anon_map

    def anonymise(self, data: int | str | None):
        if data is not None:
            data_str = str(data)
            data = self.anon_map.get(data_str)
            if data is None:
                data = hmac.digest(self.key, data_str.encode('utf-8'), 'sha256').hex()
                self.anon_map[data_str] = data
        return data

    def update_from_disk(self, save_path):
        if save_path.exists():
            self.anon_map.update(json.loads(save_path.read_text()))

    @property
    def inverse_anon_map(self):
        return {value: key for key, value in self.anon_map.items()}
