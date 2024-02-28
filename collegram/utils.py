from __future__ import annotations

import datetime
import hmac
import json
import os
from collections import defaultdict
from queue import PriorityQueue
from typing import TYPE_CHECKING, Any, overload

import fsspec
import polars as pl
from bidict import bidict

if TYPE_CHECKING:
    from pathlib import Path

LOCAL_FS = fsspec.filesystem("local")


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
    def __init__(
        self,
        key: str | None = None,
        key_env_var_name: str = "HMAC_KEY",
        anon_map: bidict | None = None,
        save_path: Path | None = None,
        fs: fsspec.AbstractFileSystem = LOCAL_FS,
    ):
        if key is None:
            key = os.environ[key_env_var_name]
        self.key = bytes.fromhex(key)
        self.anon_map: bidict[str, str] = bidict() if anon_map is None else anon_map
        self.save_path = save_path
        self.fs = fs
        if save_path is not None:
            self.update_from_disk()

    @overload
    def anonymise(self, data: int | str, safe: bool = False) -> str:
        ...

    @overload
    def anonymise(self, data: None, safe: bool = False) -> None:
        ...

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
                    data = hmac.digest(
                        self.key, data_str.encode("utf-8", "surrogatepass"), "sha256"
                    ).hex()
                    self.anon_map[data_str] = data
        return data

    def update_from_disk(self, save_path: Path | None = None):
        save_path = str(save_path if save_path is not None else self.save_path)
        if self.fs.exists(save_path):
            with self.fs.open(save_path, "r") as f:
                d = json.load(f)
            self.anon_map.update(d)

    def save_map(self, save_path: Path | None = None):
        save_path = save_path if save_path is not None else self.save_path
        if save_path is None:
            raise ValueError('no save_path set or passed here.')
        parent = str(save_path.parent)
        self.fs.mkdirs(parent, exist_ok=True)
        with self.fs.open(str(save_path), "w") as f:
            json.dump(dict(self.anon_map), f)

    @property
    def inverse_anon_map(self) -> bidict[str, str]:
        return self.anon_map.inverse


def read_nth_to_last_line(path, fs: fsspec.AbstractFileSystem = LOCAL_FS, n=1):
    """Returns the nth before last line of a file (n=1 gives last line)

    https://stackoverflow.com/questions/46258499/how-to-read-the-last-line-of-a-file-in-python
    """
    num_newlines = 0
    with fs.open(str(path), "rb") as f:
        try:
            f.seek(-2, os.SEEK_END)
            while num_newlines < n:
                f.seek(-2, os.SEEK_CUR)
                if f.read(1) == b"\n":
                    num_newlines += 1
        except (OSError, ValueError):
            # catch OSError in case of a one line file
            f.seek(0)
        last_line = f.readline().decode()
    return last_line


def get_last_modif_time(fpath: Path) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(fpath.lstat().st_mtime)

PY_PL_DTYPES_MAP = defaultdict(
    lambda: pl.Null,
    {
        bool: pl.Boolean,
        int: pl.Int64,
        float: pl.Float64,
        str: pl.Utf8,
        list: pl.List,
        dict: pl.Struct,
        datetime.datetime: pl.Datetime,
    },
)
