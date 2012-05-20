#!/usr/bin/python

import re
import sqlalchemy
import solr
from editing import MusicBrainzClient
import discogs_client as discogs
import pprint
import urllib
import time
from utils import mangle_name, join_names, out, colored_out, bcolors
import config as cfg

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute("SET search_path TO musicbrainz")

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

"""
CREATE TABLE bot_medium_format_discogs (
    gid uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_medium_format_discogs_pkey PRIMARY KEY (gid)
);
"""

query = """
WITH
    vinyl_releases AS (
        SELECT r.id, u.url, m.format, m.position
        FROM release r
            JOIN medium m ON m.release = r.id
            JOIN l_release_url l ON l.entity0 = r.id AND l.link IN (SELECT id FROM link WHERE link_type = 76)
            JOIN url u ON u.id = l.entity1
        WHERE (m.format IN (7) OR m.format IS NULL)
            /* releases with only one medium */
            AND NOT EXISTS (SELECT 1 FROM medium m2 WHERE m2.release = r.id AND m2.id <> m.id)
            /* discogs link should only be linked to this release */
            AND NOT EXISTS (SELECT 1 FROM l_release_url WHERE l_release_url.entity1 = u.id AND l_release_url.entity0 <> r.id)
            /* this release should not have another discogs link attached */
            AND NOT EXISTS (SELECT 1 FROM l_release_url WHERE l_release_url.entity0 = r.id AND l_release_url.entity1 <> u.id)
            AND l.edits_pending = 0
    )
SELECT r.id, r.gid, r.name, ta.url, ta.format, ac.name, ta.position, b.processed
FROM vinyl_releases ta
JOIN s_release r ON ta.id = r.id
JOIN s_artist_credit ac ON r.artist_credit=ac.id
LEFT JOIN bot_medium_format_discogs b ON r.gid = b.gid
ORDER BY b.processed NULLS FIRST, r.artist_credit, r.id
LIMIT 1000
"""

def discogs_get_format(release_url):
    m = re.match(r'http://www.discogs.com/release/([0-9]+)', release_url)
    if m:
        release_id = int(m.group(1))
        release = discogs.Release(release_id)
        for format in release.data['formats']:
            if ('descriptions' not in format):
                continue
            if (format['name'] == 'Vinyl') and ('12"' in format['descriptions'] or 'LP' in format['descriptions']):
                return '12"'
            if (format['name'] == 'Vinyl') and ('7"' in format['descriptions']):
                return '7"'
            if (format['name'] == 'Vinyl') and ('10"' in format['descriptions']):
                return '10"'
            if (format['name'] == 'CD'):
                return 'CD'
            if (format['name'] == 'CDr'):
                return 'CDr'
            if (format['name'] == 'Cassette'):
                return 'Cassette'
            if (format['name'] == 'File'):
                return 'DigitalMedia'

    return None

DISCOGS_MB_FORMATS_MAPPING = {
    '12"': 31,
    '10"': 30,
    '7"' : 29,
    'CD' : 1,
    'CDr' : 33,
    'Cassette' : 8,
    'DigitalMedia': 12
}

discogs.user_agent = 'MusicBrainzBot/0.1 +https://github.com/murdos/musicbrainz-bot'

for id, gid, name, url, format, ac_name, position, processed in db.execute(query):
    colored_out(bcolors.OKBLUE, 'Looking up release "%s" by "%s" http://musicbrainz.org/release/%s' % (name, ac_name, gid))

    discogs_format = discogs_get_format(url)
    if discogs_format:
        colored_out(bcolors.HEADER, ' * using %s, found format: %s' % (url,discogs_format))
        edit_note = 'Setting medium format from attached Discogs link (%s)' % url
        out(' * edit note: %s' % (edit_note,))
        mb.set_release_medium_format(gid, position, format, DISCOGS_MB_FORMATS_MAPPING[discogs_format], edit_note, True)
        time.sleep(5)
    else:
        colored_out(bcolors.FAIL, ' * using %s, no matching format has been found' % (url,))

    if processed is None:
        db.execute("INSERT INTO bot_medium_format_discogs (gid) VALUES (%s)", (gid,))
    else:
        db.execute("UPDATE bot_medium_format_discogs SET processed = now() WHERE gid = %s", (gid,))
