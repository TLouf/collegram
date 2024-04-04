# Snowballing workflow

Here's some pseudocode corresponding to our snowballing channel exploration run in
`scripts/channel_expansion.py`:

```python
credentials = get_credentials_from_env()
api_client = init_client(credentials)
# `api_client` is omitted in the following

priority_queue = first seed channels with priority 1
while priority_queue is not empty:
	channel_id = get_id_with_lowest_priority_from(priority_queue)
    full_channel_data = get_full_data(channel_id)
	all_chats = full_channel_data.get("list of chats")

	for chat in all_chats:
        full_chat_data = get_full_data(chat)
		users = get_users(chat)

        # Get up to 100 similar channels (API key needs to be attached to a premium
        # account for that)
		recommended_channels = get_recommendations(chat)
		for rec in recommended_channels:
			add_to_priority_queue(rec)
            full_rec_data = get_full_data(rec)
			anoned_rec = anonymise(full_rec_data)
			save(anoned_rec)

		anoned_full_chat_data = anonymise(full_chat_data)
		anoned_full_chat_data['users'] = anonymise(users)
		anoned_full_chat_data['recommended_channels'] = anonymise(recommended_channels)
		save(anoned_full_chat_data)

		forwarded_channels = []
		for interval in time_intervals:
			forwarded_channels = save_all_messages_and_forwards(chat, interval)

		for fwd in forwarded_channels.unique():
			add_to_priority_queue(fwd)
            full_fwd_data = get_full_data(rec)
			anoned_fwd = anonymise(full_fwd_data)
			save(anoned_fwd)
```

Caveats:
- when calling `get_full_data(channel_id)`, the channel must have been "seen" by this
  API client in the past (in which case we have a corresponding `access_hash`). Else,
  can use the user name corresponding to the channel, but this is not a fixed property!
- to know if it's been seen, it uses a key-dependent `.session` file containing a SQLite
  DB that gets populated during collection. PB: how to pass this file around? get from
  Minio on Nuclio function init, and save in Minio when Nuclio function gets stopped?
