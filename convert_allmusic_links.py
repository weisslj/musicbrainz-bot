#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import urllib
import urllib2
import socket
import time
import hashlib
import json
from optparse import OptionParser
from mbbot.utils.pidfile import PIDFile

import sqlalchemy
import mechanize

from editing import MusicBrainzClient
import config as cfg
from utils import out, program_string

'''
CREATE TABLE bot_convert_allmusic (
    gid uuid NOT NULL,
    url text NOT NULL,
    new_url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_convert_allmusic_pkey PRIMARY KEY (gid)
);
'''

def join_words(words):
    if len(words) > 1:
        return ' and '.join([', '.join(words[:-1]), words[-1]])
    else:
        return words[0]

class RoviClient(object):
    def __init__(self, api_key, shared_secret, server='http://api.rovicorp.com'):
        self.server = server
        self.api_key = api_key
        self.shared_secret = shared_secret
        self.b = mechanize.Browser()
        self.b.set_handle_robots(False)
        self.b.set_debug_redirects(False)
        self.b.set_debug_http(False)
        self.b.addheaders = [('User-agent', 'musicbrainz-bot/1.0')]

    def url(self, path, **kwargs):
        query = ''
        if kwargs:
            query = '?' + urllib.urlencode([(k, v.encode('utf8')) for (k, v) in kwargs.items()])
        return self.server + path + query

    def sig(self):
        m = hashlib.md5()
        m.update(self.api_key)
        m.update(self.shared_secret)
        m.update(str(int(time.time())))
        return m.hexdigest()

    def get_info(self, amgid):
        kwargs = {
                'apikey': self.api_key,
                'sig': self.sig()
        }
        if amgid[0] == 'P':
            kwargs['amgpopid'] = amgid
            url = self.url('/data/v1/name/info', **kwargs)
        elif amgid[0] == 'Q':
            kwargs['amgclassicalid'] = amgid
            url = self.url('/data/v1/name/info', **kwargs)
        elif amgid[0] == 'R':
            kwargs['amgpopid'] = amgid
            url = self.url('/data/v1/album/info', **kwargs)
        elif amgid[0] == 'W':
            kwargs['amgclassicalid'] = amgid
            url = self.url('/data/v1/album/info', **kwargs)
        elif amgid[0] == 'C':
            kwargs['amgclassicalid'] = amgid
            url = self.url('/data/v1/composition/info', **kwargs)
        elif amgid[0] == 'T':
            kwargs['amgpoptrackid'] = amgid
            url = self.url('/data/v1/song/info', **kwargs)
        elif amgid[0] == 'F':
            kwargs['amgclassicalid'] = amgid
            url = self.url('/data/v1/performance/info', **kwargs)
        else:
            raise Exception('unknown amgid: %s' % amgid)
        try:
            self.b.open(url)
        except urllib2.HTTPError as e:
            if e.code == 404 and amgid[0] == 'R':
                url = self.url('/data/v1/release/info', **kwargs)
                self.b.open(url)
        page = self.b.response().read()
        data = json.loads(page)
        if data['status'] == 'ok' and data['code'] == 200:
            return data
        else:
            raise Exception('error fetching work: %s' % str(data))

    def parse_info(self, data):
        if 'name' in data:
            name = data['name']
            nameid = name['ids']['nameId']
            url = u'http://www.allmusic.com/artist/%s' % nameid.lower()
            text = u'AMG Name: %s' % name['name'] if name['name'] else u''
            return (url, text)
        elif 'album' in data:
            album = data['album']
            albumid = album['ids']['albumId']
            primaryArtists = [x['name'] for x in album['primaryArtists']] if album['primaryArtists'] else []
            url = u'http://www.allmusic.com/album/%s' % albumid.lower()
            text = u'AMG Title: %s' % album['title']
            if primaryArtists:
                text += u'\nAMG Artist(s): %s' % join_words(primaryArtists)
            return (url, text)
        elif 'release' in data:
            release = data['release']
            releaseid = release['ids']['releaseId']
            albumid = release['ids']['albumId']
            primaryArtists = [x['name'] for x in release['primaryArtists']] if release['primaryArtists'] else []
            url = u'http://www.allmusic.com/album/%s' % albumid.lower()
            text = u'AMG Title: %s' % release['title']
            if primaryArtists:
                text += u'\nAMG Artist(s): %s' % join_words(primaryArtists)
            text += u'\nATTENTION: Using albumId %s instead of releaseId %s, because it is a release group link!' % (albumid, releaseid)
            return (url, text)
        elif 'composition' in data:
            composition = data['composition']
            compositionid = composition['ids']['compositionId']
            url = u'http://www.allmusic.com/composition/%s' % compositionid.lower()
            composers = [x['name'] for x in composition['composers']] if composition['composers'] else []
            text = u'AMG Title: %s' % composition['title']
            if composers:
                text += u'\nAMG Composer(s): %s' % join_words(composers)
            return (url, text)
        elif 'performance' in data:
            performance = data['performance']
            performanceid = performance['ids']['performanceId']
            url = u'http://www.allmusic.com/performance/%s' % performanceid.lower()
            text = u''
            return (url, text)
        elif 'song' in data:
            song = data['song']
            trackid = song['ids']['trackId']
            primaryArtists = [x['name'] for x in song['primaryArtists']] if song['primaryArtists'] else []
            url = u'http://www.allmusic.com/song/%s' % trackid.lower()
            text = u'AMG Title: %s' % song['title']
            if primaryArtists:
                text += u'\nAMG Artist(s): %s' % join_words(primaryArtists)
            return (url, text)
        else:
            raise Exception('unknown data: %s' % str(data))

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute('SET search_path TO musicbrainz, %s' % cfg.BOT_SCHEMA_DB)

editor_id = db.execute('''SELECT id FROM editor WHERE name = %s''', cfg.MB_USERNAME).first()[0]
mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE, editor_id=editor_id)

rc = RoviClient(cfg.ROVI_API_KEY, cfg.ROVI_SHARED_SECRET)

query_allmusic_urls = '''
SELECT url.url, url.gid
FROM url
WHERE url.edits_pending = 0 AND url ~ '^http://allmusic\.com/.*'
'''

def extract_amgid(old_url):
    m = re.match(ur'^http://allmusic\.com/(?:artist|album|work|song|performance)/(?:[^/]*-)?([pqrwctf])([0-9]+)$', old_url)
    if m:
        amgid = ('%s%9s' % m.groups()).upper()
        return amgid
    return None

processed = set(gid for gid, in db.execute('''SELECT gid FROM bot_convert_allmusic'''))

def main(verbose=False, force=False):
    normal_edits_left, edits_left = mb.edits_left()
    allmusic_urls = [(url, gid) for url, gid in db.execute(query_allmusic_urls)]
    count = len(allmusic_urls)
    for i, (url, gid) in enumerate(allmusic_urls):
        if not force and edits_left <= 0:
            break
        if gid in processed:
            continue
        if verbose:
            out(u'%d/%d - %.2f%%' % (i+1, count, (i+1) * 100.0 / count))
            out(url)
            out(u'http://musicbrainz.org/url/%s' % gid)
        amgid = extract_amgid(url)
        if amgid is None:
            if verbose:
                out('invalid url: %s' % url)
            continue
        if verbose:
            out(amgid)
        try:
            new_url, details = rc.parse_info(rc.get_info(amgid))
        except (urllib2.HTTPError, urllib2.URLError, socket.timeout) as e:
            out(e)
            continue
        text = u'Normalize to new format.\n%s\nOld: %s\nNew: %s' % (details, url, new_url)
        text += '\n\n%s' % program_string(__file__)
        try:
            out(u'%s -> %s' % (url, new_url))
            mb.edit_url(gid, url.encode('utf-8'), new_url.encode('utf-8'), text, auto=True)
            processed.add(gid)
            db.execute("INSERT INTO bot_convert_allmusic (gid,url,new_url) VALUES (%s,%s,%s)", (gid, url, new_url))
            edits_left -= 1
        except (urllib2.HTTPError, urllib2.URLError, socket.timeout) as e:
            out(e)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-v', '--verbose', action='store_true', default=False,
            help='be more verbose')
    parser.add_option('-f', '--force', action='store_true', default=False,
            help='ignore edits_left')
    (options, args) = parser.parse_args()
    with PIDFile('/tmp/mbbot_convert_allmusic_links.pid'):
        main(verbose=options.verbose, force=options.force)
