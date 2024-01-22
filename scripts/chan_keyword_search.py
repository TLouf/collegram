import os

from dotenv import load_dotenv

import collegram

if __name__ == '__main__':
    load_dotenv()
    paths = collegram.paths.ProjectPaths()
    channels_dir = paths.raw_data / 'channels'
    client = collegram.client.connect(
        os.environ['API_ID'], os.environ['API_HASH'], os.environ["PHONE_NUMBER"],
        session=str(paths.proj / 'anon.session')
    )
    output_file = paths.interim_data / "channels.txt"
    channels = set(output_file.read_text().strip().split("\n"))
    keywords = set((paths.ext_data / "keywords.txt").read_text().strip().split("\n"))
    searched_kw_path = (paths.ext_data / "searched_keywords.txt")
    searched_keywords = set(searched_kw_path.read_text().strip().split("\n"))
    keywords_to_search = keywords.difference(searched_keywords)
    for i, kw in enumerate(keywords_to_search):
        tgdb_chans = collegram.channels.search_from_tgdb(client, kw)
        api_chans = collegram.channels.search_from_api(client, kw)
        channels = channels.union(tgdb_chans).union(api_chans)
        output_file.write_text("\n".join(channels))
        searched_keywords.add(kw)
        searched_kw_path.write_text("\n".join(searched_keywords))
        print(f"{i} / {len(keywords_to_search)}: {len(tgdb_chans)} from TGDB, {len(api_chans)} from API")
