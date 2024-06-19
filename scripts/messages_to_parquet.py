from pathlib import Path

import msgspec
import polars as pl
import setup
from tqdm import tqdm

import collegram


def read_messages(fpath, chan_paths):
    messages = []
    for m in collegram.json.yield_message(fpath):
        # For backwards compatibility, ignore comments, marked with non-null
        # `comments_msg_id.` Could also go back to historical raw data to
        # remove all of these messages.
        if isinstance(m, collegram.json.Message) and m.comments_msg_id is None:
            messages.append(m)
        else:
            with open(chan_paths.messages_service_jsonl, "ab") as f:
                f.write(msgspec.json.encode(m))
                f.write(b"\n")
    return messages


if __name__ == "__main__":
    fs = collegram.utils.LOCAL_FS
    paths = collegram.paths.ProjectPaths()
    logger = setup.init_logging(paths.proj / "scripts" / __file__)
    dummy_chan_paths = collegram.paths.ChannelPaths("id", paths)
    fs.mkdirs(dummy_chan_paths.messages_table.parent, exist_ok=True)
    fs.mkdirs(dummy_chan_paths.messages_service_jsonl.parent, exist_ok=True)

    schema = collegram.json.get_pl_schema()
    # Casting struct with fields to generic struct fails, so:
    cast_schema = {k: v for k, v in schema.items() if v != pl.Struct}
    chans = sorted(fs.ls(dummy_chan_paths.messages.parent))
    for channel_dir in tqdm(chans):
        channel_dir = Path(channel_dir)
        anon_id = channel_dir.stem
        chan_paths = collegram.paths.ChannelPaths(anon_id, paths)

        saved = fs.exists(chan_paths.messages_table) and fs.exists(
            chan_paths.messages_service_jsonl
        )
        last_saved_at = None
        if saved:
            last_saved_at = max(
                fs.modified(chan_paths.messages_table),
                fs.modified(chan_paths.messages_service_jsonl),
            )
            try:
                saved_df = pl.read_parquet(
                    fs.open(chan_paths.messages_table, "rb").read()
                ).cast(cast_schema)
            except pl.ColumnNotFoundError:
                print(f"missing column in df saved for {anon_id}, replacing")
                saved = False

        messages = []
        for fpath in fs.glob(str(channel_dir / "*.jsonl")):
            if not saved or fs.modified(fpath) > last_saved_at:
                try:
                    chunk_msgs = read_messages(fpath, chan_paths)
                except msgspec.DecodeError:
                    lines = []
                    with fs.open(str(fpath), "r") as f:
                        for li in f:
                            li = li.strip("\n")
                            if li:
                                for p in li.split("}{"):
                                    if not p.startswith("{"):
                                        p = "{" + p
                                    if not p.endswith("}"):
                                        p = p + "}"
                                    lines.append(p)

                    with fs.open(str(fpath), "w") as f:
                        f.write("\n".join(lines))
                        f.write("\n")
                    chunk_msgs = read_messages(fpath, chan_paths)
                messages.extend(chunk_msgs)

        # If nothing new to add, skip to next channel
        if len(messages) == 0:
            print(f"skipping {anon_id}")
            continue

        input_d = collegram.json.messages_to_dict(messages)
        reactions = input_d.pop("reactions")
        m_df = pl.DataFrame(input_d, schema=cast_schema).with_columns(
            reactions=pl.Series(reactions)
        )

        if saved:
            saved_has_reactions = saved_df.get_column("reactions").dtype == pl.Struct
            m_has_reactions = m_df.get_column("reactions").dtype == pl.Struct
            if m_has_reactions or saved_has_reactions:
                # New reactions may have been added, or some removed, so get the union
                # of set of reactions as struct keys, filling with nulls
                reactions_schema = {
                    **(
                        saved_df.get_column("reactions").struct.schema
                        if saved_has_reactions
                        else {}
                    ),
                    **(
                        m_df.get_column("reactions").struct.schema
                        if m_has_reactions
                        else {}
                    ),
                }
                m_df = (
                    pl.concat(
                        [
                            saved_df.unnest("reactions")
                            if saved_has_reactions
                            else saved_df.drop("reactions"),
                            m_df.unnest("reactions")
                            if m_has_reactions
                            else m_df.drop("reactions"),
                        ],
                        how="diagonal",
                    )
                    .select(
                        *set(saved_df.columns)
                        .union(m_df.columns)
                        .difference(["reactions"]),
                        reactions=pl.struct(*reactions_schema.keys()),
                    )
                    .unique("id")
                )
            else:
                m_df = pl.concat(
                    [
                        saved_df,
                        m_df,
                    ],
                    how="diagonal",
                ).unique("id")

        print(f"saving {anon_id}")
        with fs.open(chan_paths.messages_table, "wb") as f:
            m_df.select(sorted(m_df.columns)).write_parquet(f)
