#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import urllib
import urllib2

import mechanize
import sqlalchemy
from musicbrainz2.webservice import WebService, Query, WebServiceError, ReleaseIncludes

from editing import MusicBrainzClient
from utils import out, program_string
import config as cfg

'''
CREATE TABLE bot_isrc_zeroinch_submitted (
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_isrc_zeroinch_submitted_pkey PRIMARY KEY (url)
);
CREATE TABLE bot_isrc_zeroinch_missing (
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_isrc_zeroinch_missing_pkey PRIMARY KEY (url)
);
CREATE TABLE bot_isrc_zeroinch_problematic (
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_isrc_zeroinch_problematic_pkey PRIMARY KEY (url)
);
'''

class MusicBrainzWebservice(object):
    def __init__(self, username, password, server='http://musicbrainz.org'):
        self.user_agent = 'zeroinch-bot/1.0 ( %s/user/%s )' % (server, username)
        self.ws = WebService(userAgent=self.user_agent, host=re.sub(r'^http://', '', server), username=username, password=password)
        self.q = Query(self.ws)
    def get_release(self, gid):
        q = Query(self.ws)
        inc = ReleaseIncludes(tracks=True, isrcs=True)
        return q.getReleaseById(gid, include=inc)
    def submit_isrcs(self, tracks2isrcs):
        q = Query(self.ws)
        q.submitISRCs(tracks2isrcs)

def isrc_valid(isrc):
    return re.match(r'[A-Z]{2}[A-Z0-9]{3}[0-9]{7}', isrc)

class ZeroInch(object):
    def __init__(self):
        self.server = 'http://www.zero-inch.com'
        self.b = mechanize.Browser()
        self.b.set_handle_robots(False)
        self.b.set_debug_redirects(False)
        self.b.set_debug_http(False)
    def url(self, path, **kwargs):
        query = ''
        if kwargs:
            query = '?' + urllib.urlencode([(k, v.encode('utf8')) for (k, v) in kwargs.items()])
        return self.server + path + query
    def _get_pages(self, location, **kwargs):
        while True:
            self.b.open(self.url(location, **kwargs))
            page = self.b.response().read()
            yield page
            m = re.search(ur'>PAGE ([0-9]+) OF ([0-9]+)<', page)
            if not m:
                break
            page_cur, page_max = (int(x) for x in m.groups())
            if page_cur < page_max:
                kwargs['page'] = str(page_cur+1)
            else:
                break
    def get_artists(self, location, **kwargs):
        for page in self._get_pages(location, **kwargs):
            yield sorted(set(re.findall(ur'<a href="/artist/(.+?)[/"]', page)))
    def get_releases(self, artist):
        for page in self._get_pages('/artist/' + artist):
            yield sorted(set(re.findall(ur'<a href="/artist/'+re.escape(artist)+ur'/(?:album|maxi|ep)/[^/]+/([0-9]+)', page)))
    def get_release(self, artist, release):
        self.b.open(self.url('/artist/' + artist + '/album/' + release))
        page = self.b.response().read()
        m = re.search(ur'<span property="v:identifier">(.+?)</span>', page)
        identifier = m.group(1) if m else None
        tracks = list(re.findall(ur'<span class="listtext">\s*<a[^<>]*?href="/artist/[^/]+/track/[^/]+/([0-9]+(?:\?trackNo=[0-9]+)?)[^<>]*?>[^<>]*?</a>\s*</span>', page, re.DOTALL))
        return (identifier, tracks)
    def get_track(self, artist, track):
        try:
            self.b.open(self.url('/artist/' + artist + '/track/' + track))
        except urllib2.HTTPError, e:
            if e.code == 301:
                return None
        page = self.b.response().read()
        m = re.search(ur'<span property="v:identifier">(.+?)</span>', page)
        identifier = m.group(1).upper() if m else None
        return identifier if identifier and isrc_valid(identifier) else None

query_releases = '''
SELECT DISTINCT r.id, r.gid, r.barcode
FROM release r
WHERE r.barcode ~ %s
'''

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute('SET search_path TO musicbrainz, %s' % cfg.BOT_SCHEMA_DB)

zeroinch = ZeroInch()
mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)
ws = MusicBrainzWebservice(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

def identify_isrc_edit(isrcs):
    return lambda edit_nr, text: set(isrcs) == set(re.findall(r'<a href="'+cfg.MB_SITE+r'/isrc/([A-Z0-9]{12})">', text))

isrc_submitted = set(url for url, in db.execute('''SELECT url FROM bot_isrc_zeroinch_submitted'''))
isrc_missing = set(url for url, in db.execute('''SELECT url FROM bot_isrc_zeroinch_missing'''))
isrc_problematic = set(url for url, in db.execute('''SELECT url FROM bot_isrc_zeroinch_problematic'''))

#for artists in [['Gui_Boratto']]:
#for artists in zeroinch.get_artists('/label/Warp_Records'):
for artists in zeroinch.get_artists('/catalogue', cipher='all', page='1'):
    for artist in artists:
        artist_url = u'http://www.zero-inch.com/artist/%s' % artist
        if artist_url in isrc_submitted:
            out('skip artist %s' % artist)
            continue
        for releases in zeroinch.get_releases(artist):
            for release_id in releases:
                url = u'http://www.zero-inch.com/artist/%s/album/%s' % (artist, release_id)
                if url in isrc_submitted or url in isrc_missing or url in isrc_problematic:
                    out('skip release %s' % release_id)
                    continue
                release = zeroinch.get_release(artist, release_id)
                identifier = release[0].lstrip('0')
                out('http://www.zero-inch.com/artist/%s/album/%s %s' % (artist, release_id, identifier))
                if not identifier:
                    out('no barcode available, aborting!')
                    db.execute("INSERT INTO bot_isrc_zeroinch_problematic (url) VALUES (%s)", url)
                    isrc_problematic.add(url)
                    continue
                tracks = release[1]
                found = False
                for r, gid, barcode in db.execute(query_releases, identifier):
                    if identifier != barcode.lstrip('0'):
                        out('barcode does not match, aborting!')
                        continue
                    out('http://musicbrainz.org/release/%s' % gid)
                    mb_release = ws.get_release(gid)
                    mb_tracks = mb_release.getTracks()
                    if len(mb_tracks) != len(tracks):
                        out('track count does not match (%d != %d), aborting!' % (len(mb_tracks), len(tracks)))
                        continue
                    found = True
                    isrcs = [zeroinch.get_track(artist, t) for t in tracks]
                    out(isrcs)
                    tracks2isrcs = {}
                    for mb_track, isrc in zip(mb_tracks, isrcs):
                        if isrc and len(isrc) == 12 and isrc not in mb_track.getISRCs():
                            tracks2isrcs[mb_track.getId()] = isrc
                    out(tracks2isrcs.values())
                    if tracks2isrcs:
                        ws.submit_isrcs(tracks2isrcs)
                        text = u'From %s, added because of matching barcode %s.' % (url, barcode)
                        text += '\n\n%s' % program_string(__file__)
                        mb.add_edit_note(identify_isrc_edit(tracks2isrcs.values()), text)
                if found:
                    db.execute("INSERT INTO bot_isrc_zeroinch_submitted (url) VALUES (%s)", url)
                    isrc_submitted.add(url)
                else:
                    db.execute("INSERT INTO bot_isrc_zeroinch_missing (url) VALUES (%s)", url)
                    isrc_missing.add(url)
        db.execute("INSERT INTO bot_isrc_zeroinch_submitted (url) VALUES (%s)", artist_url)
        isrc_submitted.add(artist_url)
