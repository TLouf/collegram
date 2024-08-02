from __future__ import annotations

import datetime
import hmac
import os
import sys
import typing
from collections import defaultdict
from queue import PriorityQueue

if sys.version_info >= (3, 10):
    from types import NoneType
else:
    NoneType = type(None)
from typing import TYPE_CHECKING, Any, overload

import fsspec
import polars as pl
from bidict import bidict

if TYPE_CHECKING:
    from pathlib import Path

LOCAL_FS: fsspec.AbstractFileSystem = fsspec.filesystem("local")


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
        save_func,
        key: str | bytes | None = None,
        key_env_var_name: str = "HMAC_KEY",
        anon_map: bidict | None = None,
    ):
        self.save_func = save_func
        if key is None:
            key = os.environ[key_env_var_name]
        if isinstance(key, str):
            key = bytes.fromhex(key)
        self.key = key
        self.anon_map: bidict[str, str] = bidict() if anon_map is None else anon_map

    @overload
    def anonymise(self, data: int | str, safe: bool = False) -> str: ...

    @overload
    def anonymise(self, data: None, safe: bool = False) -> None: ...

    def anonymise(self, data: int | str | None, safe: bool = False) -> str | None:
        """Anonymise the provided data.
        TODO: reimplement safe somehow?

        Parameters
        ----------
        data : int | str | None
            Input data. If None, the function simply returns None.
        safe : bool, optional
            Currently has no effect. Whether the anonymiser should first check that the
            input data are not the result of a previous anonymisation. False by default.

        Returns
        -------
        str
            Anonymised data.
        """
        if data is None:
            anon_data = None
        else:
            data_str = str(data)
            anon_data = self.anon_map.get(data_str)
            if anon_data is None:
                anon_data = hmac.digest(
                    self.key, data_str.encode("utf-8", "surrogatepass"), "sha256"
                ).hex()
                self.anon_map[data_str] = anon_data
                self.save_func(data_str, anon_data)
        return anon_data

    @property
    def inverse_anon_map(self) -> bidict[str, str]:
        return self.anon_map.inverse


def insert_into_postgres(conn, table: str, values: dict | list[dict]):
    """values: if a list, dicts all assumed to have the same keys"""
    if isinstance(values, dict):
        values = [values]

    cur = None
    try:
        list_cols = list(values[0].keys())
        col_names_str = f"({', '.join(list_cols)})"
        col_values_fmt = ", ".join([f"%({k})s" for k in list_cols])
        query = f"INSERT INTO {table} {col_names_str} VALUES ({col_values_fmt})"
        cur = conn.cursor()
        for v in values:
            cur.execute(
                query,
                v,
            )
        conn.commit()

    except Exception as e:
        print(f"ERROR INSERTING {table}")
        print(e)
        cur.execute("ROLLBACK")
        conn.commit()
    finally:
        cur.close()


def read_nth_to_last_line(path, fs: fsspec.AbstractFileSystem = LOCAL_FS, n=1):
    """Returns the nth to last line of a file (n=1 gives last line)

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


def get_last_modif_time(
    fpath: Path, fs: fsspec.AbstractFileSystem = LOCAL_FS
) -> datetime.datetime:
    return fs.modified(fpath)


def safe_dict_update(dict1, dict2, paths: list[str], list_entries_are_unique=False):
    """
    Allows to update `dict1` from `dict2` while preserving dict / list entries found in
    paths `paths`.

    Assumptions:
    - last part is always assumed to be accessing a dict key
    """
    dict_out = {**dict1, **dict2}
    for p in paths:
        parts = p.split(".")
        obj_out = dict_out
        obj1 = dict1
        deeper_obj_out = follow_path(obj_out, parts[0])
        deeper_obj1 = follow_path(obj1, parts[0])
        if len(parts) > 1:
            for part in parts[1:]:
                if deeper_obj_out is None or deeper_obj1 is None:
                    break
                obj_out = deeper_obj_out
                obj1 = deeper_obj1
                deeper_obj_out = follow_path(obj_out, part)
                deeper_obj1 = follow_path(obj1, part)
        if deeper_obj_out is None:
            # If `deeper_obj_out` has been set, it has priority, otherwise:
            obj_out[part[-1]] = deeper_obj1
        elif isinstance(deeper_obj_out, dict) and isinstance(deeper_obj1, dict):
            obj_out[part[-1]] = {**deeper_obj1, **deeper_obj_out}
        elif isinstance(deeper_obj_out, list) and isinstance(deeper_obj1, list):
            obj_out[part[-1]] = deeper_obj1 + [
                x
                for x in deeper_obj_out
                if not list_entries_are_unique or x not in deeper_obj1
            ]
        elif deeper_obj1 is not None and type(deeper_obj_out) != type(deeper_obj1):
            raise ValueError("Types not matching at provided paths")
    return dict_out


def follow_path(obj: list | dict, part: str):
    if part.isdigit():
        try:
            obj_out = obj[int(part)]
        except IndexError:
            obj_out = None
    elif ":" in part:
        # For instance, take the dict in which 'id' is 'x': encoded as 'id:x'
        key, value = part.split(":")
        for d in obj:
            if (
                d.get(key) == value
                or isinstance(value, str)
                and value.isdigit()
                and d.get(key) == int(value)
            ):
                return d
        obj_out = None
    else:
        obj_out = obj.get(part)
    return obj_out


PY_PL_DTYPES_MAP = defaultdict(
    lambda: pl.Null,
    {
        bool: pl.Boolean,
        int: pl.Int64,
        float: pl.Float64,
        str: pl.Utf8,
        list: pl.List,
        dict: pl.Struct,
        datetime.datetime: pl.Datetime(time_zone="UTC"),
    },
)


def py_to_pl_types(dtype):
    """
    Handles Optional args
    """
    inner_dtype = typing.get_args(dtype)
    if len(inner_dtype) == 0:
        inner_dtype = dtype
    elif len(inner_dtype) == 1:
        inner_dtype = inner_dtype[0]
    elif len(inner_dtype) == 2:
        inner_dtype = inner_dtype[0] if inner_dtype[0] != NoneType else inner_dtype[1]
    else:
        raise ValueError(
            f"Got composite type {dtype} which is not compatible with parquet."
        )
    origin = typing.get_origin(inner_dtype)
    if origin is None:
        return PY_PL_DTYPES_MAP.get(inner_dtype)
    else:
        return PY_PL_DTYPES_MAP[origin](
            PY_PL_DTYPES_MAP.get(typing.get_args(inner_dtype)[0])
        )
