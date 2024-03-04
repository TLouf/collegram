import datetime
import io
import re
from pathlib import Path
from pprint import pprint

import fsspec
import msgspec
import polars as pl
import setup
from tqdm import tqdm

import collegram

if __name__ == '__main__':

    fs = collegram.utils.LOCAL_FS
    paths = collegram.paths.ProjectPaths()
    logger = setup.init_logging(paths.proj / 'scripts' / __file__)
    dummy_chan_paths = collegram.paths.ChannelPaths('id', paths)
    fs.mkdirs(dummy_chan_paths.messages_table.parent, exist_ok=True)
    fs.mkdirs(dummy_chan_paths.messages_service_jsonl.parent, exist_ok=True)

    chans = sorted(fs.ls(dummy_chan_paths.messages.parent))
    for channel_dir in tqdm(chans):
        channel_dir = Path(channel_dir)
        anon_id = channel_dir.stem
        chan_paths = collegram.paths.ChannelPaths(anon_id, paths)

        saved = fs.exists(chan_paths.messages_table) and fs.exists(chan_paths.messages_service_jsonl)
        if saved:
            last_saved_at = max(
                fs.modified(chan_paths.messages_table),
                fs.modified(chan_paths.messages_service_jsonl),
            )
        else:
            last_saved_at = None
        messages = []
        for fpath in fs.glob(str(channel_dir / '*.jsonl')):
            if not saved or fs.modified(fpath) > last_saved_at:
                for m in collegram.json.yield_message(fpath):
                    if isinstance(m, collegram.json.Message):
                        messages.append(m)
                    else:
                        with open(chan_paths.messages_service_jsonl, 'ab') as f:
                            f.write(msgspec.json.encode(m))
                            f.write(b"\n")

        # If nothing new to add, skip to next channel
        if len(messages) == 0:
            print(f'skipping {anon_id}')
            continue

        m_df = pl.DataFrame(collegram.json.messages_to_dict(messages))

        if saved:
            # TODO: switch to fsspec when https://github.com/pola-rs/polars/issues/14813
            # is resolved. If not, `use_pyarrow=True`.
            m_df = pl.concat(
                [
                    pl.read_parquet(chan_paths.messages_table),
                    # pl.LazyFrame._scan_parquet(fs.open(chan_paths.messages_table, 'rb')).collect(),
                    m_df,
                ],
                how='diagonal',
            ).unique('id')

        print(f'saving {anon_id}')
        with fs.open(chan_paths.messages_table, 'wb') as f:
            m_df.select(sorted(m_df.columns)).write_parquet(f)
