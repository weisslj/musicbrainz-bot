#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import os.path
import time
import email.utils
from optparse import OptionParser
from collections import defaultdict
import urllib
from editing import MusicBrainzClient
from gzip import GzipFile
import xml.dom.minidom

import sqlalchemy
import Levenshtein

import config as cfg
from mbbot.utils.pidfile import PIDFile
import utils
from utils import out, program_string, asciipunct

'''
CREATE TABLE bot_bbc_reviews_set (
    gid uuid NOT NULL,
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_bbc_reviews_set_pkey PRIMARY KEY (gid,url)
);
'''

bbc_sitemap_url = 'http://www.bbc.co.uk/music/sitemap-extended.xml.gz'
bbc_sitemap = 'bbc_music_sitemap.xml.gz'
cleanup_urls = ['http://wiki.musicbrainz.org/Community_Project/BBC_Review_Cleanup', 'http://wiki.musicbrainz.org/Community_Project/BBC_Review_Cleanup/Old']

def get_remote_mtime(url):
    lastmod = urllib.urlopen(url).info().getheader('Last-Modified')
    return email.utils.mktime_tz(email.utils.parsedate_tz(lastmod))

def get_local_mtime(path):
    try:
        return os.path.getmtime(path)
    except OSError:
        return -1

def download_if_modified(url, filename):
    remote_mtime = get_remote_mtime(url)
    if remote_mtime > get_local_mtime(filename):
        urllib.urlretrieve(url, filename)
        os.utime(filename, (time.time(), remote_mtime))

def load_bbc_reviews(path):
    doc = xml.dom.minidom.parse(GzipFile(path))
    for url in doc.getElementsByTagName('url'):
        loc = url.getElementsByTagName('loc')[0].firstChild.data
        if re.match(ur'http://www\.bbc\.co\.uk/music/reviews/', loc):
            d = {}
            for tag in ['loc', 'og:title', 'foaf:primaryTopic']:
                el = url.getElementsByTagName(tag)
                if el:
                    d[tag] = el[0].firstChild.data
            if len(d) == 3:
                yield (d['loc'], d['foaf:primaryTopic'], d['og:title'])

def are_similar(name1, name2):
    name1, name2 = (asciipunct(s.strip().lower()) for s in (name1, name2))
    ratio = Levenshtein.jaro_winkler(name1, name2)
    return ratio >= 0.8 or name1 in name2 or name2 in name1

def get_release_redirects(db):
    query_release_redirects = '''
        SELECT redirect.gid, r.release_group, r.artist_credit, rn.name
        FROM release_gid_redirect redirect
        JOIN release r ON r.id = redirect.new_id
        JOIN release_name rn ON r.name = rn.id
    '''
    for gid, rg, ac, name in db.execute(query_release_redirects):
        yield (gid, (rg, ac, name))

def get_release_groups(db):
    query_rgs = '''
        SELECT rg.id, rg.gid, rn.name
        FROM release_group rg
        JOIN release_name rn ON rg.name = rn.id
    '''
    for rg, gid, name in db.execute(query_rgs):
        yield (rg, (gid, name))

def get_releases(db):
    query_releases = '''
        SELECT r.gid, r.release_group, r.artist_credit, rn.name
        FROM release r
        JOIN release_name rn ON r.name = rn.id
    '''
    for gid, rg, ac, name in db.execute(query_releases):
        yield (gid, (rg, ac, name))

def get_review_urls(db):
    query_rg_reviews = '''
        SELECT l_rgu.entity0, url.url
        FROM l_release_group_url l_rgu
        JOIN link l ON l_rgu.link = l.id
        JOIN url ON url.id = l_rgu.entity1
        WHERE l.link_type = 94
    '''
    for rg, url in db.execute(query_rg_reviews):
        yield (rg, url)

def artist_credit(db, ac):
    return u''.join(u'%s%s' % (name, join_phrase if join_phrase else u'') for name, join_phrase in db.execute('''SELECT an.name,acn.join_phrase from artist_credit ac JOIN artist_credit_name acn ON acn.artist_credit = ac.id JOIN artist_name an ON acn.name = an.id WHERE ac.id = %s ORDER BY position''', ac))

def db_connect():
    engine = sqlalchemy.create_engine(cfg.MB_DB)
    db = engine.connect()
    db.execute('SET search_path TO musicbrainz, %s' % cfg.BOT_SCHEMA_DB)
    return db

def main(verbose=False):
    download_if_modified(bbc_sitemap_url, bbc_sitemap)

    db = db_connect()

    release_redirects = dict(get_release_redirects(db))
    release_groups = dict(get_release_groups(db))
    releases = dict(get_releases(db))
    bbc_reviews_set = set((gid, url) for gid, url in db.execute('''SELECT gid, url FROM bot_bbc_reviews_set'''))

    review_urls = defaultdict(set)
    for rg, url in get_review_urls(db):
        review_urls[rg].add(url)

    cleanup_review_urls = set()
    for cleanup_url in cleanup_urls:
        f = urllib.urlopen(cleanup_url)
        cleanup_review_urls |= set(re.findall(ur'http://www.bbc.co.uk/music/reviews/[0-9a-z]+', f.read()))

    editor_id = db.execute('''SELECT id FROM editor WHERE name = %s''', cfg.MB_USERNAME).first()[0]
    mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE, editor_id=editor_id)

    normal_edits_left, edits_left = mb.edits_left()

    bbc_reviews = list(load_bbc_reviews(bbc_sitemap))
    count = len(bbc_reviews)
    for i, (review_url, release_url, title) in enumerate(bbc_reviews):
        if normal_edits_left <= 0:
            break
        if verbose:
            out(u'%d/%d - %.2f%%' % (i+1, count, (i+1) * 100.0 / count))
            out(u'%s %s' % (title, review_url))
            out(release_url)
        if review_url in cleanup_review_urls:
            continue
        release_gid = utils.extract_mbid(release_url, 'release')
        row = release_redirects.get(release_gid)
        if not row:
            row = releases.get(release_gid)
        if not row:
            if verbose:
                out('  non-existant release in review %s' % review_url)
            continue
        rg, ac, release_name = row
        gid, name = release_groups[rg]
        if review_url in review_urls[rg]:
            continue
        if (gid, review_url) in bbc_reviews_set:
            if verbose:
                out(u'  already linked earlier (probably got removed by some editor!')
            continue
        mb_title = '%s - %s' % (artist_credit(db, ac), release_name)
        if not are_similar(title, mb_title):
            if verbose:
                out(u'  similarity too small: %s <-> %s' % (title, mb_title))
                # out(u'|-\n| [%s %s]\n| [[ReleaseGroup:%s|%s]]\n| [[Release:%s|%s]]' % (review_url, bbc_name, gid, name, release_gid, release_name))
            continue
        text = u'Review is in BBC mapping [1], and review name “%s” is'\
                ' similar to the release name. If this is wrong,'\
                ' please note it here and put the correct mapping in'\
                ' the wiki [2].\n\n[1] %s\n[2] %s' % (title, bbc_sitemap_url, cleanup_urls[0])
        text += '\n\n%s' % program_string(__file__)
        try:
            out(u'http://musicbrainz.org/release-group/%s  ->  %s' % (gid, review_url))
            mb.add_url('release_group', gid, 94, review_url, text, auto=False)
            db.execute("INSERT INTO bot_bbc_reviews_set (gid,url) VALUES (%s,%s)", (gid, review_url))
            bbc_reviews_set.add((gid, review_url))
            normal_edits_left -= 1
        except (urllib2.HTTPError, urllib2.URLError, socket.timeout) as e:
            out(e)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-v', '--verbose', action='store_true', default=False,
            help='be more verbose')
    (options, args) = parser.parse_args()
    with PIDFile('/tmp/mbbot_bbc_reviews.pid'):
        main(options.verbose)
