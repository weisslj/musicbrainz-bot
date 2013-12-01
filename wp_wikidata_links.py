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
import httplib2
import time
import socket
from mbbot.utils.pidfile import PIDFile
from mbbot.wp.wikipage import WikiPage
from utils import mangle_name, join_names, out, colored_out, bcolors
import config as cfg

#ENTITY_TYPE = sys.argv[1] if len(sys.argv) > 1 else 'artist'

WIKIPEDIA_RELATIONSHIP_TYPES = {'artist': 179, 'label': 216, 'release-group': 89, 'work': 279, 'area': 355, 'place': 595}
WIKIDATA_RELATIONSHIP_TYPES = {'artist': 352, 'label': 354, 'release-group': 353, 'work': 351, 'area': 358, 'place': 594}

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute("SET search_path TO musicbrainz, %s" % cfg.BOT_SCHEMA_DB)

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

"""
CREATE TABLE mbbot.bot_wp_wikidata_links (
    gid uuid NOT NULL,
    lang character varying(10),
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_wp_wikidata_links_pkey PRIMARY KEY (gid, lang)
);
"""

def main(ENTITY_TYPE):

    entity_type_table = ENTITY_TYPE.replace('-', '_')
    url_relationship_table = 'l_%s_url' % entity_type_table if ENTITY_TYPE != 'work' else 'l_url_%s' % entity_type_table
    main_entity_entity_point = "entity0" if ENTITY_TYPE != 'work' else "entity1"
    url_entity_point = "entity1" if ENTITY_TYPE != 'work' else "entity0"

    query = """
    WITH
        entities_wo_wikidata AS (
            SELECT DISTINCT e.id AS entity_id, e.gid AS entity_gid, u.url AS wp_url, substring(u.url from '//(([a-z]|-)+)\\.') as wp_lang
            FROM """+entity_type_table+""" e
                JOIN """+url_relationship_table+""" l ON l."""+main_entity_entity_point+""" = e.id AND l.link IN (SELECT id FROM link WHERE link_type = """+str(WIKIPEDIA_RELATIONSHIP_TYPES[ENTITY_TYPE])+""")
                JOIN url u ON u.id = l."""+url_entity_point+""" AND u.url LIKE 'http://%%.wikipedia.org/wiki/%%'
            WHERE 
                /* No existing WikiData relationship for this entity */
                NOT EXISTS (SELECT 1 FROM """+url_relationship_table+""" ol WHERE ol."""+main_entity_entity_point+""" = e.id AND ol.link IN (SELECT id FROM link WHERE link_type = """+str(WIKIDATA_RELATIONSHIP_TYPES[ENTITY_TYPE])+"""))
                /* WP link should only be linked to this entity */
                AND NOT EXISTS (SELECT 1 FROM """+url_relationship_table+""" ol WHERE ol."""+url_entity_point+""" = u.id AND ol."""+main_entity_entity_point+""" <> e.id)
                AND l.edits_pending = 0
        )
    SELECT e.id, e.gid, e.name, ewf.wp_url, b.processed
    FROM entities_wo_wikidata ewf
    JOIN """+entity_type_table+""" e ON ewf.entity_id = e.id
    LEFT JOIN bot_wp_wikidata_links b ON e.gid = b.gid AND b.lang = ewf.wp_lang
    ORDER BY b.processed NULLS FIRST, e.id
    LIMIT 500
    """

    seen = set()
    matched = set()
    for entity in db.execute(query):
        if entity['gid'] in matched:
            continue

        colored_out(bcolors.OKBLUE, 'Looking up entity "%s" http://musicbrainz.org/%s/%s' % (entity['name'], ENTITY_TYPE, entity['gid']))
        out(' * wiki:', entity['wp_url'])

        page = WikiPage.fetch(entity['wp_url'], False)
        if page.wikidata_id:
            wikidata_url = 'http://www.wikidata.org/wiki/%s' % page.wikidata_id.upper()
            edit_note = 'From %s' % (entity['wp_url'],)
            colored_out(bcolors.OKGREEN, ' * found WikiData identifier:', wikidata_url)
            time.sleep(1)
            out(' * edit note:', edit_note.replace('\n', ' '))
            mb.add_url(ENTITY_TYPE.replace('-', '_'), entity['gid'], str(WIKIDATA_RELATIONSHIP_TYPES[ENTITY_TYPE]), wikidata_url, edit_note, True)
            matched.add(entity['gid'])

        if entity['processed'] is None and entity['gid'] not in seen:
            db.execute("INSERT INTO bot_wp_wikidata_links (gid, lang) VALUES (%s, %s)", (entity['gid'], page.lang))
        else:
            db.execute("UPDATE bot_wp_wikidata_links SET processed = now() WHERE (gid, lang) = (%s, %s)", (entity['gid'], page.lang))
        seen.add(entity['gid'])
    stats['seen'][ENTITY_TYPE] = len(seen)
    stats['matched'][ENTITY_TYPE] = len(matched)

stats = {'seen': {}, 'matched': {}}
if __name__ == '__main__':
    with PIDFile('/tmp/mbbot_wp_wikidata_links.pid'):
        ENTITY_TYPES = ('place', 'release-group', 'artist', 'work', 'label')
        if len(sys.argv) > 1 and sys.argv[1] in ENTITY_TYPES:
            main(sys.argv[1])
        else:
            for entity_type in ENTITY_TYPES:
                main(entity_type)
	print '\nStats:'
        for entity_type in ENTITY_TYPES:
            print ' * %s : %s / %s' % (entity_type, stats['matched'][entity_type], stats['seen'][entity_type])
