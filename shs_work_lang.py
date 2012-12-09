#!/usr/bin/python

import re
import sqlalchemy
import solr
from editing import MusicBrainzClient
from mbbot.source.secondhandsongs import SHSWebService
import pprint
import urllib
import time
from utils import mangle_name, join_names, out, colored_out, bcolors
import config as cfg

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute("SET search_path TO musicbrainz, %s" % cfg.BOT_SCHEMA_DB)

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)
shs = SHSWebService()

"""
CREATE TABLE bot_shs_work_lang (
    work uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_shs_work_lang_pkey PRIMARY KEY (work)
);
"""

query = """
WITH
    works_wo_lang AS (
        SELECT w.id AS work_id, u.url AS shs_url
        FROM work w
            JOIN l_url_work l ON l.entity1 = w.id AND l.link IN (SELECT id FROM link WHERE link_type = 280)
            JOIN url u ON u.id = l.entity0
        WHERE language IS NULL AND url NOT LIKE '%%/performance/%%'
            /* SHS link should only be linked to this work */
            AND NOT EXISTS (SELECT 1 FROM l_url_work WHERE l_url_work.entity0 = u.id AND l_url_work.entity1 <> w.id)
            /* this work should not have another SHS link attached */
            AND NOT EXISTS (SELECT 1 FROM l_url_work WHERE l_url_work.entity1 = w.id AND l_url_work.entity0 <> u.id
                                    AND l_url_work.link IN (SELECT id FROM link WHERE link_type = 280))
            AND l.edits_pending = 0
    )
SELECT w.id, w.gid, w.name, w.language, wwol.shs_url, b.processed
FROM works_wo_lang wwol
JOIN s_work w ON wwol.work_id = w.id
LEFT JOIN bot_shs_work_lang b ON w.gid = b.work
ORDER BY b.processed NULLS FIRST, w.id
LIMIT 150
"""

iswcs_query = """
SELECT iswc from iswc
WHERE work = %s
ORDER BY iswc
"""

# select '"'||name ||'": ' || id || ',' from language where frequency = 2 order by id;
SHS_MB_LANG_MAPPING = {
 "Arabic": 18,
 "Chinese": 76,
 "Czech": 98,
 "Danish": 100,
 "Dutch": 113,
 "English": 120,
 "Finnish": 131,
 "French": 134,
 "German": 145,
 "Greek": 159,
 "Italian": 195,
 "Japanese": 198,
 "[Multiple languages]": 284,
 "Norwegian": 309,
 "Polish": 338,
 "Portuguese": 340,
 "Russian": 353,
 "Spanish": 393,
 "Swedish": 403,
 "Turkish": 433,
}

for work in db.execute(query):
    colored_out(bcolors.OKBLUE, 'Looking up work "%s" http://musicbrainz.org/work/%s' % (work['name'], work['gid']))

    m = re.match(r'http://www.secondhandsongs.com/work/([0-9]+)', work['shs_url'])
    if m:
        shs_work = shs.lookup_work(int(m.group(1)))
    else:
        continue
        
    if 'language' in shs_work:
        work = dict(work)
        shs_lang = shs_work['language']
        
        if shs_lang not in SHS_MB_LANG_MAPPING:
            colored_out(bcolors.FAIL, ' * No mapping defined for language ''%s' % shs_lang)
            continue

        work['iswcs'] = []
        for (iswc,) in db.execute(iswcs_query, work['id']):
            work['iswcs'].append(iswc)
        work['language'] = SHS_MB_LANG_MAPPING[shs_lang]
        update = ('language',)            

        colored_out(bcolors.HEADER, ' * using %s, found language: %s' % (work['shs_url'], shs_lang))
        edit_note = 'Setting work language from attached SecondHandSongs link (%s)' % work['shs_url']
        out(' * edit note: %s' % (edit_note,))
        
        mb.edit_work(work, update, edit_note)
        
    else:
        colored_out(bcolors.NONE, ' * using %s, no language has been found' % (work['shs_url'],))

    if work['processed'] is None:
        db.execute("INSERT INTO bot_shs_work_lang (work) VALUES (%s)", (work['gid'],))
    else:
        db.execute("UPDATE bot_shs_work_lang SET processed = now() WHERE work = %s", (work['gid'],))
