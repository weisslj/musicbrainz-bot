#!/usr/bin/python
# -*- coding: utf-8 -*-

import sys
import os
import datetime
import re
import sqlalchemy
from editing import MusicBrainzClient
import pprint
import urllib
import time
from mbbot.utils.pidfile import PIDFile
from mbbot.wp.wikipage import WikiPage
from mbbot.wp.analysis import determine_authority_identifiers
from utils import mangle_name, join_names, out, colored_out, bcolors
import config as cfg

VIAF_RELATIONSHIP_TYPES = {'artist': 310}

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute("SET search_path TO musicbrainz, %s" % cfg.BOT_SCHEMA_DB)

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

"""
CREATE TABLE bot_wp_artist_viaf (
    gid uuid NOT NULL,
    lang character varying(2),
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_wp_artist_viaf_pkey PRIMARY KEY (gid, lang)
);
"""

query = """

WITH
    artists_wo_viaf AS (
        SELECT DISTINCT a.id AS artist_id, a.gid AS artist_gid, u.url AS wp_url
        FROM artist a
            JOIN l_artist_url l ON l.entity0 = a.id AND l.link IN (SELECT id FROM link WHERE link_type = 179)
            JOIN url u ON u.id = l.entity1 AND u.url LIKE 'http://%%.wikipedia.org/wiki/%%' AND substring(u.url from 8 for 2) IN ('en', 'fr')
        WHERE 
            /* No existing VIAF relationship */
            NOT EXISTS (SELECT 1 FROM l_artist_url WHERE l_artist_url.entity0 = a.id AND l_artist_url.link IN (SELECT id FROM link WHERE link_type = """+str(VIAF_RELATIONSHIP_TYPES['artist'])+"""))
            /* WP link should only be linked to this artist */
            AND NOT EXISTS (SELECT 1 FROM l_artist_url WHERE l_artist_url.entity1 = u.id AND l_artist_url.entity0 <> a.id)
            AND l.edits_pending = 0
            AND a.gid NOT IN ('89ad4ac3-39f7-470e-963a-56509c546377')
    )
SELECT a.id, a.gid, a.name, awf.wp_url, b.processed
FROM artists_wo_viaf awf
JOIN s_artist a ON awf.artist_id = a.id
LEFT JOIN bot_wp_artist_viaf b ON a.gid = b.gid AND b.lang = substring(awf.wp_url from 8 for 2)
ORDER BY b.processed NULLS FIRST, a.id
LIMIT 1000
"""

def main():
    seen = set()
    matched = set()
    for artist in db.execute(query):
        if artist['gid'] in matched:
            continue

        colored_out(bcolors.OKBLUE, 'Looking up artist "%s" http://musicbrainz.org/artist/%s' % (artist['name'], artist['gid']))
        out(' * wiki:', artist['wp_url'])

        page = WikiPage.fetch(artist['wp_url'], False)
        identifiers = determine_authority_identifiers(page)
        if 'VIAF' in identifiers:
            if not isinstance(identifiers['VIAF'], basestring):
                colored_out(bcolors.FAIL, ' * multiple VIAF found: %s' % ', '.join(identifiers['VIAF']))
            else:
                viaf_url = 'http://viaf.org/viaf/%s' % identifiers['VIAF']
                edit_note = 'From %s' % (artist['wp_url'],)
                colored_out(bcolors.OKGREEN, ' * found VIAF:', viaf_url)
                out(' * edit note:', edit_note.replace('\n', ' '))
                time.sleep(3)
                mb.add_url('artist', artist['gid'], str(VIAF_RELATIONSHIP_TYPES['artist']), viaf_url, edit_note)
                matched.add(artist['gid'])

        if artist['processed'] is None and artist['gid'] not in seen:
            db.execute("INSERT INTO bot_wp_artist_viaf (gid, lang) VALUES (%s, %s)", (artist['gid'], page.lang))
        else:
            db.execute("UPDATE bot_wp_artist_viaf SET processed = now() WHERE (gid, lang) = (%s, %s)", (artist['gid'], page.lang))
        seen.add(artist['gid'])

if __name__ == '__main__':
    with PIDFile('/tmp/mbbot_wp_artist_viaf.pid'):
        main()
