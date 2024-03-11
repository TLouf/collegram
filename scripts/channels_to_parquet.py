import datetime
import json
import re
from pathlib import Path
from pprint import pprint

import fsspec
import polars as pl
import setup
from tqdm import tqdm

import collegram
from collegram.paths import ChannelPaths, ProjectPaths


def yield_chan(chan_ids: set[str], user_schema: dict, project_paths: ProjectPaths, fs: fsspec.AbstractFileSystem):
    for i in chan_ids:
        chan_paths = ChannelPaths(i, project_paths)
        c = json.loads(fs.read_text(chan_paths.channel))
        if 'last_queried_at' not in c:
            c['last_queried_at'] = fs.modified(chan_paths.channel)
        participants = c.pop('participants', None)
        if participants:
            users_df = pl.DataFrame(map(collegram.users.flatten_dict, participants), schema=user_schema)
            with fs.open(chan_paths.users_table, 'wb') as f:
                users_df.write_parquet(f)
        yield c


def has_been_modified_since(anon_chan_id: str, since: datetime.datetime, project_paths: ProjectPaths, fs: fsspec.AbstractFileSystem):
    chan_paths = ChannelPaths(anon_chan_id, project_paths)
    return fs.modified(chan_paths.channel) > since


if __name__ == '__main__':
    fs = collegram.utils.LOCAL_FS
    paths = ProjectPaths()
    logger = setup.init_logging(paths.proj / 'scripts' / __file__)
    dummy_chan_paths = ChannelPaths('id', paths)
    fs.mkdirs(dummy_chan_paths.users_table.parent, exist_ok=True)
    fs.mkdirs(paths.channels_table.parent, exist_ok=True)

    chans = list(fs.ls(dummy_chan_paths.channel.parent))

    user_schema = collegram.users.get_pl_schema()
    chan_schema = collegram.channels.get_pl_schema()
    anon_chan_ids_to_add = set([p.stem for p in dummy_chan_paths.channel.parent.iterdir()])
    if paths.channels_table.exists():
        table_saved_at = fs.modified(paths.channels_table)
        anon_chan_ids_to_add = set(filter(lambda i: has_been_modified_since(i, table_saved_at, paths ,fs), anon_chan_ids_to_add))
        chans_df = pl.read_parquet(fs.open(paths.channels_table, 'rb').read()).filter(~pl.col('id').is_in(anon_chan_ids_to_add))
    else:
        chans_df = pl.DataFrame(schema=chan_schema)

    new_chans_df = pl.DataFrame(
        map(
            collegram.channels.flatten_dict,
            tqdm(yield_chan(anon_chan_ids_to_add, user_schema, paths, fs), total=len(anon_chan_ids_to_add))
        ),
        schema=chan_schema,
    )
    all_chans_df = pl.concat([chans_df, new_chans_df], how='diagonal').unique('id')
    all_chans_df.write_parquet(fs.open(paths.channels_table, 'wb'))
