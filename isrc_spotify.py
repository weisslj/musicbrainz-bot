#!/usr/bin/python
# -*- coding: utf-8 -*-

# (c) 2012 Ian Weller, Aurélien Mino
# This program is free software. It comes without any warranty, to the extent
# permitted by applicable law. You can redistribute it and/or modify it under
# the terms of the Do What The Fuck You Want To Public License, Version 2, as
# published by Sam Hocevar. See COPYING for more details.

import re
import urllib
import urllib2
import mechanize
import sqlalchemy
import musicbrainzngs
import json
import time
from kitchen.text.converters import to_bytes, to_unicode
from datetime import datetime
from picard.similarity import similarity2
from editing import MusicBrainzClient
from utils import out, colored_out, bcolors
import config as cfg

import codecs

'''
CREATE TABLE bot_isrc_spotify (
    release uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_isrc_spotify_pkey PRIMARY KEY (release)
)
'''

musicbrainzngs.set_useragent(
    "musicbrainz-bot",
    "1.0",
    "%s/user/%s" % (cfg.MB_SITE, cfg.MB_USERNAME)
)

query_releases_wo_isrcs = '''
WITH
    releases_wo_isrcs AS (
        SELECT DISTINCT r.id, r.gid, r.name, r.barcode, r_country.iso_code AS country, r.artist_credit
        FROM s_release r
            JOIN medium ON medium.release = r.id
            LEFT JOIN country r_country ON r.country = r_country.id
            JOIN artist_credit ac ON r.artist_credit = ac.id
            JOIN artist_credit_name acn ON acn.artist_credit = ac.id
            JOIN artist a ON a.id = acn.artist
            LEFT JOIN country a_country ON a.country = a_country.id
        WHERE r.barcode IS NOT NULL AND r.barcode != ''
            /* Release has no ISRCs */
            AND NOT EXISTS (SELECT 1 FROM track JOIN isrc ON isrc.recording = track.recording WHERE medium.tracklist = track.tracklist)
            /* AND a_country.iso_code = 'FR' AND r_country.iso_code = 'FR' */
    )
SELECT r.id, r.gid, r.name, r.barcode, ac.name AS artist, b.processed
FROM releases_wo_isrcs tr
JOIN s_release r ON tr.id = r.id
JOIN s_artist_credit ac ON r.artist_credit = ac.id
LEFT JOIN bot_isrc_spotify b ON b.release = r.gid
ORDER BY b.processed NULLS FIRST, r.artist_credit
LIMIT 100
'''

query_tracks = '''
SELECT r.gid, r.name, r.length, m.position || '.' || t.position AS position
FROM medium m
    JOIN track t ON m.tracklist = t.tracklist
    JOIN s_recording r ON r.id = t.recording
WHERE m.release = %s
ORDER BY m.position, t.position
'''

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute('SET search_path TO musicbrainz')

class SpotifyWebService(object):
    """
    This product uses a SPOTIFY API but is not endorsed, certified or otherwise
    approved in any way by Spotify. Spotify is the registered trade mark of the
    Spotify Group.
    """

    def __init__(self):
        self.last_request_time = datetime.min

    def _fetch_json(self, url, params):
        self._check_rate_limit()
        # urllib.urlencode expects str objects, not unicode
        fixed = dict([(to_bytes(b[0]), to_bytes(b[1]))
                      for b in params.items()])
        request = urllib2.Request(url + '?' + urllib.urlencode(fixed))
        request.add_header('Accept', 'application/json')
        response = urllib2.urlopen(request)
        data = json.loads(response.read())
        self.last_request_time = datetime.now()
        return data

    def _check_rate_limit(self):
        diff = datetime.now() - self.last_request_time
        if diff.total_seconds() < 2.0:
            time.sleep(2.0 - diff.total_seconds())

    def lookup(self, uri, detail=0):
        """
        Detail ranges from 0 to 2 and determines the level of detail of child
        objects (i.e. for an artist, detail changes how much information is
        returned on albums).
        """
        params = {'uri': uri}
        if detail != 0:
            if 'artist' in uri:
                extras = [None, 'album', 'albumdetail'][detail]
            elif 'album' in uri:
                extras = [None, 'track', 'trackdetail'][detail]
            else:
                extras = None
            if extras:
                params['extras'] = extras
        data = self._fetch_json('http://ws.spotify.com/lookup/1/', params)
        return data[uri.split(':')[1]]

    def search_albums(self, query):
        data = self._fetch_json('http://ws.spotify.com/search/1/album', {'q': query})
        return data['albums']

def similarity(a, b):
    return int(similarity2(to_unicode(a), to_unicode(b)) * 100)

def compare_data(mb_release, sp_release):
    name = similarity(mb_release['name'], sp_release['name'])
    artist = similarity(mb_release['artist'], sp_release['artist'])
    if abs(len(mb_release['tracks']) - len(sp_release['tracks'])) != 0:
        return 0
    track = []
    track_time_diff = []
    track_sim = []
    for i in range(len(mb_release['tracks'])):
        track.append(similarity(mb_release['tracks'][i]['name'], sp_release['tracks'][i]['name']))
        track_time_diff.append(abs(mb_release['tracks'][i]['length'] - sp_release['tracks'][i]['length']*1000)/1000)
        if track_time_diff[i] > 15:
            track_time_sim = 0
        else:
            track_time_sim = int((15 - track_time_diff[i]) / 15 * 100)
        track_sim.append(int(track[i] * 0.50) + int(track_time_sim * 0.50))
    return int(name * 0.10) + int(artist * 0.10) + int(sum(track_sim) / len(mb_release['tracks']) * 0.80)

def submit_isrcs(mb_release, sp_release):
    mbids = []
    for track in mb_release['tracks']:
            mbids.append(track['gid'])
    isrcs = []
    for track in sp_release['tracks']:
        this_isrc = []
        for extid in track['external-ids']:
            if extid['type'] == 'isrc':
                this_isrc.append(extid['id'].upper())
        isrcs.append(this_isrc)
    musicbrainzngs.submit_isrcs(dict(zip(mbids, isrcs)))

def make_html_comparison_page(mbrainz, spotify):
    with codecs.open('compare.html', mode='w', encoding='utf-8') as f:
        f.write('<html><head><meta charset="utf-8"></head><body><div style="float:left;width:50%">')
        f.write('<div style="font-weight:bold">%s</div>' % mbrainz['name'])
        f.write('<div style="font-weight:bold">%s</div>' % mbrainz['artist'])
        for track in mbrainz['tracks']:
            f.write('<div>%s: %s (%s)</div>' %
                    (track['position'],
                     track['name'],
                     int(track['length'] if track['length'] is not None else 0) ))
        f.write('</div><div style="float:right;width:50%">')
        f.write('<div style="font-weight:bold">%s</div>' % spotify['name'])
        f.write('<div style="font-weight:bold">%s</div>' % spotify['artist'])
        for track in spotify['tracks']:
            f.write('<div>%s-%s: %s (%s)</div>' %
                    (track['disc-number'], track['track-number'],
                     track['name'], int(track['length']*1000)))
        f.write('</div></body></html>')

def save_processing(mb_release):
    if mb_release['processed'] is None:
        db.execute("INSERT INTO bot_isrc_spotify (release) VALUES (%s)", (mb_release['gid']))
    else:
        db.execute("UPDATE bot_isrc_spotify SET processed = now() WHERE release = %s", (mb_release['gid']))

sws = SpotifyWebService()
musicbrainzngs.auth(cfg.MB_USERNAME, cfg.MB_PASSWORD)

for release in db.execute(query_releases_wo_isrcs):

    mb_release = dict(release)

    colored_out(bcolors.OKBLUE, 'Looking up release "%s" http://musicbrainz.org/release/%s' % (mb_release['name'], mb_release['gid']))

    sp_albums = sws.search_albums('upc:%s' % mb_release['barcode'])
    if len(sp_albums) != 1:
        if len(sp_albums) == 0:
            out(' * no spotify release found')
        if len(sp_albums) > 1:
            out(' * multiple spotify releases found')
        save_processing(mb_release)
        continue
    sp_uri = sp_albums[0]['href']
    sp_release = sws.lookup(sp_uri, detail=2)

    for track in sp_release['tracks']:
        for extid in track['external-ids']:
            if extid['type'] == 'isrc':
                if extid['id'].upper()[:2] == 'TC':
                    print 'TuneCore song IDs detected! Bailing out'
                    save_processing(mb_release)
                    continue

    mb_release['tracks'] = []
    for mb_track in db.execute(query_tracks % (mb_release['id'],)):
        mb_release['tracks'].append(mb_track)

    make_html_comparison_page(mb_release, sp_release)

    sim = compare_data(mb_release, sp_release)
    out(' * comparing with %s: metadata matched %d%%' % (sp_uri, sim))
    if sim < 85:
        out(' * not enough similarity => skipping')
    else:
        out(' * submitting ISRCs')
        submit_isrcs(mb_release, sp_release)

    save_processing(mb_release)
