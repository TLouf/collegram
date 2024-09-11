## Messages

### Table columns

- ('comments_msg_id', Int64),
- ('date', Datetime(time_unit='us', time_zone='UTC')),
- ('edit_date', Datetime(time_unit='us', time_zone='UTC')),
- ('edit_wide', Boolean),
- (forwards', Int64),
- ('from_id', String),
- ('from_scheduled', Boolean),
- ('from_type', String),
- ('fwd_from_date', Datetime(time_unit='us', time_zone='UTC')),
- ('fwd_from_id', String),
- ('fwd_from_msg_id', Int64),
- ('fwd_from_type', String),
- ('has_comments', Boolean),
- ('id', Int64),
- ('invert_media', Boolean),
- ('legacy', Boolean),
- ('media_id', Int64),
- ('media_type', String),
- ('media_unread', Boolean),
- ('mentioned', Boolean),
- ('message', String),
- ('noforwards', Boolean),
- ('nr_replies', Int64),
- ('out', Boolean),
- ('pinned', Boolean),
- ('post', Boolean),
- ('reactions', Struct),
- ('replies_to_chan_id', String),
- ('replies_to_msg_id', Int64),
- ('replies_to_thread_msg_id', Int64),
- ('silent', Boolean),
- ('text_mentions', List(String)),
- ('text_urls', List(String)),
- ('via_bot_id', Int64),
- ('views', Int64)

### Modified from raw data

- media >
    - _ > "media_type"
    - id >"media_id"
- from_id >
    - _ > "from_type"
    - (channel_id or user_id or chat_id) > "from_id"
- reply_to >
    - reply_to_msg_id > "replies_to_msg_id"
    - reply_to_top_id > "replies_to_thread_msg_id"
    - reply_to_peer_id.channel_id > "replies_to_chan_id"
- fwd_from >
    - date > "fwd_from_date"
    - from_id._ > "fwd_from_type"
    - from_id.(channel_id or user_id or chat_id) > "fwd_from_id"
    - channel_post > "fwd_from_msg_id"
- replies >
    - replies > "nr_replies"
    - comments > "has_comments"
- reactions > dictionary, mapping, for each element in message.reactions.results (denoted r):
    - a reaction (str, can be emoji directly (from r.emoticon) or document_id (from r.emoticon))
    - to a count (from r.count)


## Channels

### Table columns

- ('banned_count', Int64),
- ('can_delete_channel', Boolean),
- ('can_view_participants', Boolean),
- ('has_scheduled', Boolean),
- ('admins_count', Int64),
- ('verified', Boolean),
- ('stories_unavailable', Boolean),
- ('stories_hidden_min', Boolean),
- ('requests_pending', Int64),
- ('has_geo', Boolean),
- ('stats_dc', Int64),
- ('creator', Boolean),
- ('stories_max_id', Int64),
- ('pts', Int64),
- ('participants_hidden', Boolean),
- ('megagroup', Boolean),
- ('slowmode_next_send_date', Datetime(time_unit='us', time_zone=None)),
- ('linked_chat_id', String),
- ('call_active', Boolean),
- ('join_request', Boolean),
- ('stories_hidden', Boolean),
- ('can_set_username', Boolean),
- ('level', Int64),
- ('unread_count', Int64),
- ('can_view_stats', Boolean),
- ('access_hash', Int64),
- ('theme_emoticon', String),
- ('hidden_prehistory', Boolean),
- ('title', String),
- ('call_not_empty', Boolean),
- ('available_min_id', Int64),
- ('usernames', List(String)),
- ('read_outbox_max_id', Int64),
- ('about', String),
- ('join_to_send', Boolean),
- ('broadcast', Boolean),
- ('can_set_stickers', Boolean),
- ('antispam', Boolean),
- ('pinned_msg_id', Int64),
- ('min', Boolean),
- ('date', Datetime(time_unit='us', time_zone=None)),
- ('noforwards', Boolean),
- ('username', String),
- ('blocked', Boolean),
- ('slowmode_enabled', Boolean),
- ('migrated_from_max_id', Int64),
- ('participants_count', Int64),
- ('gigagroup', Boolean),
- ('translations_disabled', Boolean),
- ('folder_id', Int64),
- ('signatures', Boolean),
- ('view_forum_as_messages', Boolean),
- ('online_count', Int64),
- ('stories_pinned_available', Boolean),
- ('id', String),
- ('read_inbox_max_id', Int64),
- ('has_link', Boolean),
- ('slowmode_seconds', Int64),
- ('left', Boolean),
- ('scam', Boolean),
- ('forum', Boolean),
- ('ttl_period', Int64),
- ('kicked_count', Int64),
- ('restricted', Boolean),
- ('migrated_from_chat_id', String),
- ('fake', Boolean),
- ('can_set_location', Boolean),
- ('forwards_from', List(String)),
- ('migrated_to', String),
- ('bot_ids', List(Int64)),
- ('linked_chats_ids', List(String)),
- ('recommended_channels', List(String)),
- ('location_point', List(Float64)),
- ('location_str', String),
- ('last_queried_at', Datetime(time_unit='us', time_zone=None)),
- ('sticker_set_id', Int64),
- ('document_count', Int64),
- ('message_count', Int64),
- ('gif_count', Int64),
- ('music_count', Int64),
- ('photo_count', Int64),
- ('url_count', Int64),
- ('video_count', Int64),
- ('voice_count', Int64)

### Modified from raw data

- migrated_to >
    - channel_id > "migrated_to"
- usernames >
    - [i].username for all i > "usernames"
- sticker_set >
    - id > sticker_set_id
- bot_info >
    - [i].user_id for all i > bot_ids

### Added metadata

- last_queried_at: linked to collection, last time we collected from this channel
- recommended_channels: list of channel IDs recommended by Telegram (see `channels.GetChannelRecommendationsRequest` table in D2.2)
- linked_chats_ids: list of channel IDs in the full_chat
- \<content\>_count: count of messages by content type, obtained through a call to `TelegramClient.iter_messages` with corresponding filter
