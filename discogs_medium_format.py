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
db.execute("SET search_path TO musicbrainz, %s" % cfg.BOT_SCHEMA_DB)

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

discogs.user_agent = 'MusicBrainzBot/0.1 +https://github.com/murdos/musicbrainz-bot'

"""
CREATE TABLE bot_discogs_medium_format (
    medium integer NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_discogs_medium_format_pkey PRIMARY KEY (medium)
);
"""

query = """
WITH
    mediums_with_fuzzy_format AS (
        SELECT r.id AS release_id, m.position, m.id AS medium_id, u.url AS discogs_url, m.format
        FROM release r
            JOIN medium m ON m.release = r.id
            JOIN l_release_url l ON l.entity0 = r.id AND l.link IN (SELECT id FROM link WHERE link_type = 76)
            JOIN url u ON u.id = l.entity1
        WHERE (m.format IN (7) OR m.format IS NULL)
            /* discogs link should only be linked to this release */
            AND NOT EXISTS (SELECT 1 FROM l_release_url WHERE l_release_url.entity1 = u.id AND l_release_url.entity0 <> r.id)
            /* this release should not have another discogs link attached */
            AND NOT EXISTS (SELECT 1 FROM l_release_url WHERE l_release_url.entity0 = r.id AND l_release_url.entity1 <> u.id
                                    AND l_release_url.link IN (SELECT id FROM link WHERE link_type = 76))
            AND l.edits_pending = 0
    )
SELECT ra.release_id, r.gid, ra.medium_id, r.name, ra.discogs_url, ra.position, ra.format, ac.name AS ac_name, b.processed
FROM mediums_with_fuzzy_format ra
JOIN release r ON ra.release_id = r.id
JOIN artist_credit ac ON r.artist_credit=ac.id
LEFT JOIN bot_discogs_medium_format b ON ra.medium_id = b.medium
ORDER BY b.processed NULLS FIRST, r.artist_credit, r.id, ra.position
LIMIT 1000
"""

def discogs_get_medium_format(release, medium_no):
    if len(release.data['formats']) > 1:
        return None
    for format in release.data['formats']:
        if format['name'] == 'CD':
            return 'CD'
        elif format['name'] == 'CDr':
            return 'CDr'
        elif format['name'] == 'Cassette':
            return 'Cassette'
        elif format['name'] == 'File':
            return 'DigitalMedia'
        elif format['name'] in ('Vinyl', 'Shellac'):
            if 'descriptions' not in format:
                return "Vinyl"
            elif '12"' in format['descriptions'] or 'LP' in format['descriptions']:
                return '12"'
            elif '7"' in format['descriptions']:
                return '7"'
            elif '10"' in format['descriptions']:
                return '10"'
    return None

DISCOGS_MB_FORMATS_MAPPING = {
    'Vinyl': 7,
    '12"': 31,
    '10"': 30,
    '7"' : 29,
    'CD' : 1,
    'CDr' : 33,
    'Cassette' : 8,
    'DigitalMedia': 12
}

for medium in db.execute(query):
    colored_out(bcolors.OKBLUE, 'Looking up medium #%s of release "%s" by "%s" http://musicbrainz.org/release/%s' % (medium['position'], medium['name'], medium['ac_name'], medium['gid']))

    m = re.match(r'http://www.discogs.com/release/([0-9]+)', medium['discogs_url'])
    if m:
        discogs_release = discogs.Release(int(m.group(1)))

    discogs_format = discogs_get_medium_format(discogs_release, medium['position'])
    if discogs_format:
        colored_out(bcolors.HEADER, ' * using %s, found format: %s' % (medium['discogs_url'], discogs_format))
        edit_note = 'Setting medium format from attached Discogs link (%s)' % medium['discogs_url']
        out(' * edit note: %s' % (edit_note,))
        mb.set_release_medium_format(medium['gid'], medium['position'], medium['format'], DISCOGS_MB_FORMATS_MAPPING[discogs_format], edit_note, True)
    else:
        colored_out(bcolors.FAIL, ' * using %s, no matching format has been found' % (medium['discogs_url'],))

    if medium['processed'] is None:
        db.execute("INSERT INTO bot_discogs_medium_format (medium) VALUES (%s)", (medium['medium_id'],))
    else:
        db.execute("UPDATE bot_discogs_medium_format SET processed = now() WHERE medium = %s", (medium['medium_id'],))
