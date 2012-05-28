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

discogs.user_agent = 'MusicBrainzBot/0.1 +https://github.com/murdos/musicbrainz-bot'

"""
CREATE TABLE bot_discogs_medium_format (
    gid uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_discogs_medium_format_pkey PRIMARY KEY (gid)
);
"""

query = """
WITH
    releases_with_fuzzy_format AS (
        SELECT r.id, u.url AS discogs_url, m.format, m.position
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
            AND NOT EXISTS (SELECT 1 FROM l_release_url WHERE l_release_url.entity0 = r.id AND l_release_url.entity1 <> u.id
                                    AND l_release_url.link IN (SELECT id FROM link WHERE link_type = 76))
            AND l.edits_pending = 0
    )
SELECT r.id, r.gid, r.name, ra.discogs_url, ra.format, ac.name AS ac_name, ra.position, b.processed
FROM releases_with_fuzzy_format ra
JOIN s_release r ON ra.id = r.id
JOIN s_artist_credit ac ON r.artist_credit=ac.id
LEFT JOIN bot_discogs_medium_format b ON r.gid = b.gid
ORDER BY b.processed NULLS FIRST, r.artist_credit, r.id
LIMIT 1000
"""

def discogs_get_format(release_url):
    m = re.match(r'http://www.discogs.com/release/([0-9]+)', release_url)
    if m:
        release_id = int(m.group(1))
        release = discogs.Release(release_id)
        for format in release.data['formats']:

            if (format['name'] == 'CD'):
                return 'CD'
            if (format['name'] == 'CDr'):
                return 'CDr'
            if (format['name'] == 'Cassette'):
                return 'Cassette'
            if (format['name'] == 'File'):
                return 'DigitalMedia'

            if ('descriptions' not in format):
                continue
            if (format['name'] == 'Vinyl') and ('12"' in format['descriptions'] or 'LP' in format['descriptions']):
                return '12"'
            if (format['name'] == 'Vinyl') and ('7"' in format['descriptions']):
                return '7"'
            if (format['name'] == 'Vinyl') and ('10"' in format['descriptions']):
                return '10"'
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

for release in db.execute(query):
    colored_out(bcolors.OKBLUE, 'Looking up release "%s" by "%s" http://musicbrainz.org/release/%s' % (release['name'], release['ac_name'], release['gid']))

    discogs_format = discogs_get_format(release['discogs_url'])
    if discogs_format:
        colored_out(bcolors.HEADER, ' * using %s, found format: %s' % (release['discogs_url'], discogs_format))
        edit_note = 'Setting medium format from attached Discogs link (%s)' % release['discogs_url']
        out(' * edit note: %s' % (edit_note,))
        mb.set_release_medium_format(release['gid'], release['position'], release['format'], DISCOGS_MB_FORMATS_MAPPING[discogs_format], edit_note, True)
        time.sleep(5)
    else:
        colored_out(bcolors.FAIL, ' * using %s, no matching format has been found' % (release['discogs_url'],))

    if release['processed'] is None:
        db.execute("INSERT INTO bot_discogs_medium_format (gid) VALUES (%s)", (release['gid'],))
    else:
        db.execute("UPDATE bot_discogs_medium_format SET processed = now() WHERE gid = %s", (release['gid'],))
