#!/usr/bin/python

import re
import sqlalchemy
from editing import MusicBrainzClient
import time
from utils import out, colored_out, bcolors
import config as cfg

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute("SET search_path TO musicbrainz, %s" % cfg.BOT_SCHEMA_DB)

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

query = """
    SELECT DISTINCT r.id, r.gid, r.name, r.comment, lrw.id AS rel_id, lt.id AS link_type, r.artist_credit
    FROM recording r
        JOIN l_recording_work lrw ON lrw.entity0 = r.id
        JOIN link l ON l.id = lrw.link
        JOIN link_type lt ON l.link_type = lt.id
        JOIN link_attribute la ON la.link = l.id
        JOIN link_attribute_type lat ON la.attribute_type = lat.id AND lat.name = 'live'
    WHERE r.comment ~ E'live, \\\\d{4}(-\\\\d{2})?(-\\\\d{2})?:'
        AND l.begin_date_year IS NULL
        AND l.end_date_year IS NULL
        AND lt.name = 'performance'
        AND r.edits_pending = 0 AND lrw.edits_pending = 0
        /* Only one linked work */
        AND NOT EXISTS (SELECT 1 FROM l_recording_work lrw2 WHERE lrw2.entity0 = r.id AND lrw2.entity1 <> lrw.entity1)
    ORDER BY r.artist_credit
    LIMIT 250
"""

date_re = re.compile(r'live, (\d{4})(?:-(\d{2}))?(?:-(\d{2}))?:', re.I)
for recording in db.execute(query):

    m = re.match(date_re, recording['comment'])
    if m is None:
        continue

    date = {'year': int(m.group(1))}

    if m.group(2) is not None:
        date['month'] = int(m.group(2))
    if m.group(3) is not None:
        date['day'] = int(m.group(3))
  
    colored_out(bcolors.OKBLUE, 'Setting performance relationships dates of http://musicbrainz.org/recording/%s "%s (%s)"' % (recording['gid'], recording['name'], recording['comment']))

    attributes = {}
    edit_note = 'Setting relationship dates from recording comment: "%s"' % recording['comment']
    colored_out(bcolors.NONE, " * new date:", date)
    
    time.sleep(15) 
    mb.edit_relationship(recording['rel_id'], 'recording', 'work', recording['link_type'], recording['link_type'], attributes, date, date, edit_note, True)
