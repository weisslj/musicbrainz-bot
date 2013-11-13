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
CREATE TABLE bot_discogs_release_packaging (
    release uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_discogs_release_packaging_pkey PRIMARY KEY (release)
);
"""

query = """
WITH
    releases_wo_packaging AS (
        SELECT r.id AS release_id, u.url AS discogs_url
        FROM release r
            JOIN l_release_url l ON l.entity0 = r.id AND l.link IN (SELECT id FROM link WHERE link_type = 76)
            JOIN url u ON u.id = l.entity1
        WHERE r.packaging IS NULL 
            /* discogs link should only be linked to this release */
            AND NOT EXISTS (SELECT 1 FROM l_release_url WHERE l_release_url.entity1 = u.id AND l_release_url.entity0 <> r.id)
            /* this release should not have another discogs link attached */
            AND NOT EXISTS (SELECT 1 FROM l_release_url WHERE l_release_url.entity0 = r.id AND l_release_url.entity1 <> u.id
                                    AND l_release_url.link IN (SELECT id FROM link WHERE link_type = 76))
            AND l.edits_pending = 0
    )
SELECT ra.release_id, r.gid, r.name, r.packaging, ra.discogs_url, ac.name AS ac_name, b.processed
FROM releases_wo_packaging ra
JOIN s_release r ON ra.release_id = r.id
JOIN s_artist_credit ac ON r.artist_credit=ac.id
LEFT JOIN bot_discogs_release_packaging b ON r.gid = b.release
ORDER BY b.processed NULLS FIRST, r.artist_credit, r.id
LIMIT 5000
"""

def discogs_get_release_packaging(discogs_release):
    #if len(discogs_release.data['formats']) > 1:
    #    return None
    for format in discogs_release.data['formats']:

        if 'text' not in format:
            print 'No text found for format %s' % format['name']
            continue
        
        freetext = format['text'].lower().replace('-', '').replace(' ', '')
        colored_out(bcolors.HEADER, ' * Discogs format text: %s' % freetext)
        if 'cardboard' in freetext or 'paper' in freetext:
            return "cardboard/paper sleeve";
        elif 'digipak' in freetext or 'digipack' in freetext:
            return "digipak";
        elif 'keepcase' in freetext:
            return "keep case";
        elif 'jewel' in freetext:
            if 'slim' in freetext:
                return "slim jewel case"
            else:
                return "jewel case"

    return None

DISCOGS_MB_PACKAGING_MAPPING = {
    'jewel case': 1,
    'slim jewel case': 2,
    'digipak': 3,
    'cardboard/paper sleeve' : 4,
    'other' : 5,
    'keep case' : 6,
    'none' : 7,
}

for release in db.execute(query):
    colored_out(bcolors.OKBLUE, 'Looking up release "%s" by "%s" http://musicbrainz.org/release/%s' % (release['name'], release['ac_name'], release['gid']))

    m = re.match(r'http://www.discogs.com/release/([0-9]+)', release['discogs_url'])
    if m:
        discogs_release = discogs.Release(int(m.group(1)))

    discogs_packaging = discogs_get_release_packaging(discogs_release)
    if discogs_packaging:
        colored_out(bcolors.OKGREEN, ' * using %s, found packaging: %s' % (release['discogs_url'], discogs_packaging))
        edit_note = 'Setting release packaging from attached Discogs link (%s)' % release['discogs_url']
        out(' * edit note: %s' % (edit_note,))
        mb.set_release_packaging(release['gid'], release['packaging'], DISCOGS_MB_PACKAGING_MAPPING[discogs_packaging], edit_note, True)
    else:
        colored_out(bcolors.NONE, ' * using %s, no matching packaging has been found' % (release['discogs_url'],))

    time.sleep(2)
    if release['processed'] is None:
        db.execute("INSERT INTO bot_discogs_release_packaging (release) VALUES (%s)", (release['gid'],))
    else:
        db.execute("UPDATE bot_discogs_release_packaging SET processed = now() WHERE release = %s", (release['gid'],))
