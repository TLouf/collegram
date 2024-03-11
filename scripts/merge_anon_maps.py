import json

import polars as pl
import setup
from dotenv import load_dotenv

import collegram

if __name__ == "__main__":
    load_dotenv()

    paths = collegram.paths.ProjectPaths()
    save_path = paths.interim_data / "anon_map.parquet"
    script_path = paths.proj / "scripts" / __file__
    logger = setup.init_logging(script_path)

    anon_maps_path = paths.raw_data / "anon_maps"
    base_anon = collegram.utils.HMAC_anonymiser()
    if save_path.exists():
        it = [
            p
            for p in anon_maps_path.iterdir()
            if p.lstat().st_mtime > save_path.lstat().st_mtime
        ]
    else:
        it = anon_maps_path.iterdir()
    out_d = {}
    for i, p in enumerate(it):
        logger.info(str(i))
        out_d.update(json.loads(p.read_text()))

    input_d = {"original": list(out_d.keys()), "hash": list(out_d.values())}
    del out_d
    new_anon_map_df = pl.DataFrame(
        input_d, schema={"original": pl.Utf8, "hash": pl.Utf8}
    )

    if save_path.exists():
        old_anon_map_df = pl.read_parquet(save_path)
        anon_map_df = pl.concat([old_anon_map_df, new_anon_map_df]).unique()
    else:
        anon_map_df = new_anon_map_df

    # check values are actually unique
    if anon_map_df["hash"].is_duplicated().sum() > 0:
        logger.error("values are not unique")
        breakpoint()
    for r in anon_map_df.iter_rows():
        if r[0] is not None and base_anon.anonymise(r[0]) != r[1]:
            logger.error("values do not match anonymiser")
            breakpoint()
    anon_map_df.write_parquet(save_path)
