#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
from collections import defaultdict
import urllib
from editing import MusicBrainzClient
from htmlentitydefs import name2codepoint
from StringIO import StringIO
from gzip import GzipFile
import xml.dom.minidom

import sqlalchemy
import Levenshtein

import config as cfg
from utils import out, program_string, asciipunct

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute('SET search_path TO musicbrainz, %s' % cfg.BOT_SCHEMA_DB)

bbc_mapping_url = 'https://raw.github.com/gist/1704822/5c8291f273c80e4e6ffa7ea521be694eb7bceb79/gistfile1.txt'
cleanup_urls = ['http://wiki.musicbrainz.org/Community_Project/BBC_Review_Cleanup', 'http://wiki.musicbrainz.org/Community_Project/BBC_Review_Cleanup/Old']

def load_bbc_reviews():
    global bbc_reviews
    data = GzipFile(fileobj=StringIO(urllib.urlopen('http://www.bbc.co.uk/music/sitemap-extended.xml.gz').read())).read()
    data = re.sub(r'(<urlset .*?)>', r'\1 xmlns:og="http://ogp.me/ns#">', data)
    x = xml.dom.minidom.parseString(data)
    bbc_reviews = {}
    for url in x.getElementsByTagName('url'):
        loc = url.getElementsByTagName('loc')[0].firstChild.data
        if re.match(ur'http://www\.bbc\.co\.uk/music/reviews/', loc):
            d = {}
            for tag in ['loc', 'lastmod', 'og:title', 'og:image', 'og:type']:
                el = url.getElementsByTagName(tag)
                if el:
                    d[tag] = el[0].firstChild.data
            bbc_reviews[loc] = d
#load_bbc_reviews()

def are_similar(name1, name2):
    name1, name2 = (asciipunct(s.strip().lower()) for s in (name1, name2))
    ratio = Levenshtein.jaro_winkler(name1, name2)
    return ratio >= 0.8 or name1 in name2 or name2 in name1

def html_unescape(s):
    return re.sub(r'&(%s);' % '|'.join(name2codepoint), lambda m: unichr(name2codepoint[m.group(1)]), s)

def get_bbc_review_album_name(bbc_url):
    #return bbc_reviews[bbc_url]['og:title']
    try:
        f = urllib.urlopen(bbc_url)
        data = f.read()
    except IOError, e:
        out(e)
        return None
    m = re.search(ur'<title>BBC - Music - Review of (.+?)</title>', data)
    #m = re.search(ur'<em>(.+?)</em> *<span>Review</span> *</h1>', data)
    return html_unescape(unicode(m.group(1), 'utf-8')) if m else None

def parse_sql_table_dump(text):
    lines = re.findall(ur'^\| +(.+?) +\| +(.+?) +\| *$', text, re.M)
    return lines[1:]

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

query_release_redirects = '''
SELECT redirect.gid, r.release_group, rn.name
FROM release_gid_redirect redirect
JOIN release r ON r.id = redirect.new_id
JOIN release_name rn ON r.name = rn.id
'''
release_redirects = dict((gid, (rg, name)) for gid, rg, name in db.execute(query_release_redirects))

query_rgs = '''
SELECT rg.id, rg.gid, rn.name, rgt.name
FROM release_group rg
JOIN release_name rn ON rg.name = rn.id
LEFT JOIN release_group_type rgt ON rgt.id = rg.type
'''
release_groups = dict((rg, (gid, name, rgtype)) for rg, gid, name, rgtype in db.execute(query_rgs))

query_releases = '''
SELECT r.gid, r.release_group, rn.name
FROM release r
JOIN release_name rn ON r.name = rn.id
'''
releases = dict((gid, (rg, name)) for gid, rg, name in db.execute(query_releases))

query_rg_reviews = '''
SELECT l_rgu.entity0, url.url
FROM l_release_group_url l_rgu
JOIN link l ON l_rgu.link = l.id
JOIN url ON url.id = l_rgu.entity1
WHERE l.link_type = 94
'''
review_urls = defaultdict(set)
for rg, url in db.execute(query_rg_reviews):
    review_urls[rg].add(url)

cleanup_review_urls = set()
for cleanup_url in cleanup_urls:
    f = urllib.urlopen(cleanup_url)
    cleanup_review_urls |= set(re.findall(ur'http://www.bbc.co.uk/music/reviews/[0-9a-z]+', f.read()))

f = urllib.urlopen(bbc_mapping_url)
rows = parse_sql_table_dump(f.read())
count = len(rows)
for i, (bbc_url, release_gid) in enumerate(rows):
    if release_gid == 'NULL':
        #bbc_name = get_bbc_review_album_name(bbc_url)
        #out(u'|-\n| [%s %s]\n|\n|' % (bbc_url, bbc_name))
        continue
    if bbc_url in cleanup_review_urls:
        continue
    row = release_redirects.get(release_gid)
    if not row:
        row = releases.get(release_gid)
    if not row:
        out('non-existant release in review %s' % bbc_url)
        continue
    rg, release_name = row
    gid, name, rgtype = release_groups[rg]
    #if rgtype == 'Single':
    #    out('%s - http://musicbrainz.org/release-group/%s - %s' % (name, gid, bbc_url))
    #continue
    if bbc_url in review_urls[rg]:
        continue
    bbc_name = get_bbc_review_album_name(bbc_url)
    if not bbc_name:
        out('could not get BBC album name of %s, aborting' % bbc_url)
        continue
    out('%d/%d - %.2f%%' % (i+1, count, (i+1) * 100.0 / count))
    out('http://musicbrainz.org/release-group/%s - %s' % (gid, bbc_url))
    if not are_similar(name, bbc_name):
        out(u'  similarity too small: %s <-> %s' % (name, bbc_name))
        #out(u'|-\n| [%s %s]\n| [[ReleaseGroup:%s|%s]]\n| [[Release:%s|%s]]' % (bbc_url, bbc_name, gid, name, release_gid, release_name))
        continue
    text = u'Review is in BBC mapping [1], and review name “%s” is similar to the release group name. If this is wrong, please note it here and put the correct mapping in the wiki [2].\n\n[1] %s\n[2] http://wiki.musicbrainz.org/Community_Project/BBC_Review_Cleanup' % (bbc_name, bbc_mapping_url)
    text += '\n\n%s' % program_string(__file__)
    mb.add_url('release_group', gid, 94, bbc_url, text, auto=False)
