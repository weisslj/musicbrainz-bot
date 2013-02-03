#!/usr/bin/python

import re
import sqlalchemy
import solr
from editing import MusicBrainzClient
from mbbot.source.secondhandsongs import SHSWebService
from picard.similarity import similarity2
from kitchen.text.converters import to_unicode
import pprint
import urllib
import urllib2
import time
from utils import mangle_name, join_names, out, colored_out, bcolors
import config as cfg

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute("SET search_path TO musicbrainz, %s" % cfg.BOT_SCHEMA_DB)

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)
shs = SHSWebService()

"""
CREATE TABLE mbbot.bot_shs_link_artist (
    artist uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_shs_link_artist_pkey PRIMARY KEY (artist)
);
"""

query = """
WITH
    artists_wo_shs AS (
        SELECT DISTINCT a.id AS artist_id, a.gid AS artist_gid, w.id AS work_id, w.gid AS work_gid, u.url AS shs_url
        FROM artist a
            JOIN l_artist_work law ON law.entity0 = a.id AND law.link IN (SELECT id FROM link WHERE link_type in (167,168,165))
            JOIN work w ON law.entity1 = w.id
            JOIN l_url_work l ON l.entity1 = w.id AND l.link IN (SELECT id FROM link WHERE link_type = 280)
            JOIN url u ON u.id = l.entity0
        WHERE NOT EXISTS (SELECT 1 FROM l_artist_url WHERE l_artist_url.entity0 = a.id AND l_artist_url.link IN (SELECT id FROM link WHERE link_type in (307)))
            AND url LIKE '%%/work/%%'
            /* SHS link should only be linked to this work */
            AND NOT EXISTS (SELECT 1 FROM l_url_work WHERE l_url_work.entity0 = u.id AND l_url_work.entity1 <> w.id)
            /* this work should not have another SHS link attached */
            AND NOT EXISTS (SELECT 1 FROM l_url_work WHERE l_url_work.entity1 = w.id AND l_url_work.entity0 <> u.id
                                    AND l_url_work.link IN (SELECT id FROM link WHERE link_type = 280))
            AND l.edits_pending = 0
            AND law.edits_pending = 0
            /* Not [unknown] */
            AND a.gid NOT IN ('125ec42a-7229-4250-afc5-e057484327fe')
    )
SELECT a.id, a.gid, a.name, aws.shs_url, aws.work_id, aws.work_gid, b.processed
FROM artists_wo_shs aws
JOIN s_artist a ON aws.artist_id = a.id
LEFT JOIN bot_shs_link_artist b ON a.gid = b.artist
ORDER BY b.processed NULLS FIRST, a.id
LIMIT 1000
"""

seen_artists = set()
matched_artists = set()
for artist in db.execute(query):
    if artist['gid'] in matched_artists:
        continue

    colored_out(bcolors.OKBLUE, 'Looking up artist "%s" http://musicbrainz.org/artist/%s' % (artist['name'], artist['gid']))

    m = re.match(r'http://www.secondhandsongs.com/work/([0-9]+)', artist['shs_url'])
    if m:
        shs_work = shs.lookup_work(int(m.group(1)))
    else:
        continue
    
    artist_uri = None
    shs_artists = []
    # credits of actual work
    if 'credits' in shs_work and len(shs_work['credits']) > 0:
        shs_artists.extend(shs_work['credits'])
    # credits of original work
    if 'originalCredits' in shs_work and len(shs_work['originalCredits']) > 0:
        shs_artists.extend(shs_work['originalCredits'])
    # performer of original recording (bands are often wrongly credited as composer/writer in MB)
    if 'original' in shs_work and shs_work['original'] is not None:
        m = re.match(r'http://www.secondhandsongs.com/performance/([0-9]+)', shs_work['original']['uri'])
        if m:
            try:
                original = shs.lookup('recording', int(m.group(1)))
                if 'performer' in original:
                    shs_artists.append(original['performer']['artist'])
            except ValueError:
                pass
            except urllib2.HTTPError:
                pass
    for shs_artist in shs_artists:
        shs_artist_name = mangle_name(re.sub(' \[\d+\]$', '', shs_artist['commonName']))
        mb_artist_name = mangle_name(artist['name'])
        if shs_artist_name == mb_artist_name:
            artist_uri = shs_artist['uri']
            break
        elif similarity2(to_unicode(shs_artist_name), to_unicode(mb_artist_name)) > 0.85:
            print "%s => similarity = %.2f" % (shs_artist['commonName'], similarity2(to_unicode(shs_artist_name), to_unicode(mb_artist_name)))
            artist_uri = shs_artist['uri']
            break

    if artist_uri:
        matched_artists.add(artist['gid'])
        colored_out(bcolors.HEADER, ' * using %s, found artist SHS URL: %s' % (artist['shs_url'], artist_uri))
        edit_note = 'Guessing artist SecondHandSongs URL from work http://musicbrainz.org/work/%s linked to %s' % (artist['work_gid'], artist['shs_url'])
        out(' * edit note: %s' % (edit_note,))
        
        mb.add_url('artist', artist['gid'], str(307), artist_uri, edit_note)
    else:
        colored_out(bcolors.NONE, ' * using %s, no artist SHS URL has been found' % (artist['shs_url'],))

    if artist['processed'] is None and artist['gid'] not in seen_artists:
        db.execute("INSERT INTO bot_shs_link_artist (artist) VALUES (%s)", (artist['gid'],))
    else:
        db.execute("UPDATE bot_shs_link_artist SET processed = now() WHERE artist = %s", (artist['gid'],))
    seen_artists.add(artist['gid'])
