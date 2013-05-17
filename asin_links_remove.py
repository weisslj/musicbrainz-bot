#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import urllib2
import socket
import datetime
from collections import defaultdict
from optparse import OptionParser

import sqlalchemy
import Levenshtein
import amazonproduct

from editing import MusicBrainzClient
import config as cfg
from utils import out, program_string, asciipunct
from mbbot.utils.pidfile import PIDFile

'''
CREATE TABLE bot_asin_removed (
    gid uuid NOT NULL,
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_asin_removed_pkey PRIMARY KEY (gid,url)
);
CREATE TABLE bot_asin_remove_problematic (
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_asin_remove_problematic_pkey PRIMARY KEY (url)
);
CREATE TABLE bot_asin_remove_missing (
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_asin_remove_missing_pkey PRIMARY KEY (url)
);
CREATE TABLE bot_asin_remove_no_barcode (
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_asin_remove_no_barcode_pkey PRIMARY KEY (url)
);
'''

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute('SET search_path TO musicbrainz, %s' % cfg.BOT_SCHEMA_DB)

editor_id = db.execute('''SELECT id FROM editor WHERE name = %s''', cfg.MB_USERNAME).first()[0]
mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE, editor_id=editor_id)

store_map = [
    # http://www.amazon.com/gp/help/customer/display.html/ref=hp_left_cn?nodeId=527692
    ('us', ['US', 'AU']),
    # http://www.amazon.co.uk/gp/help/customer/display.html/ref=ssd?nodeId=1204872
    ('uk', ['GB', 'XE']),
    # http://www.amazon.de/gp/help/customer/display.html/ref=hp_left_sib?nodeId=13464781
    ('de', ['DE', 'AT', 'BE', 'LI', 'LU', 'NL', 'CH', 'XE']),
    # http://www.amazon.fr/gp/help/customer/display.html?nodeId=897502
    ('fr', ['FR', 'MC', 'BE', 'LU', 'CH', 'XE']),
    # http://www.amazon.co.jp/gp/help/customer/display.html/ref=hp_rel_topic?nodeId=1039606
    ('jp', ['JP']),
    # http://www.amazon.ca/gp/help/customer/display.html?nodeId=918742
    ('ca', ['CA']),
    # http://www.amazon.es/gp/help/customer/display.html?nodeId=200533920
    # ('es', ['ES']),
    # http://www.amazon.it/gp/help/customer/display.html?nodeId=200533920
    ('it', ['IT', 'SM', 'VA']),
    # http://www.amazon.cn/gp/help/customer/display.html?nodeId=200485640
    ('cn', ['CN']),
]
store_map_rev = defaultdict(list)
for loc, country_list in store_map:
    for country in country_list:
        store_map_rev[country].append(loc)
        
amazon_api = {}

query_releases_with_duplicate_asin = '''
SELECT q.url, lru.id, r.id, r.gid, r.barcode, rn.name, r.artist_credit
FROM
    (
        SELECT
            url.id, url.gid, url, COUNT(*) AS count
        FROM
            url JOIN l_release_url lru ON lru.entity1 = url.id
        WHERE
            url ~ '^http://www.amazon.(com|ca|co.uk|fr|de|it|es|co.jp|cn)'
        GROUP BY
            url.id, url.gid, url HAVING COUNT(url) > 1
    ) AS q
    JOIN l_release_url lru ON lru.entity1 = q.id
    JOIN release r ON r.id = lru.entity0
    JOIN release_status AS rs ON r.status = rs.id
    JOIN release_name rn ON rn.id = r.name
    JOIN artist_credit ac ON r.artist_credit = ac.id
    JOIN artist_name an ON ac.name = an.id
WHERE r.edits_pending = 0 AND lru.edits_pending = 0 AND rs.name != 'Pseudo-Release' AND r.barcode IS NOT NULL AND r.barcode != ''
GROUP BY q.url, lru.id, r.id, r.gid, r.barcode, rn.name, r.artist_credit
ORDER BY r.artist_credit
'''

query_release_asins = '''
SELECT
    url.id, url.gid, url
FROM
    url JOIN l_release_url lru ON lru.entity1 = url.id
WHERE
    url ~ '^http://www.amazon.(com|ca|co.uk|fr|de|it|es|co.jp|cn)' AND lru.entity0 = %s
'''

# from https://github.com/metabrainz/musicbrainz-server/blob/master/root/static/scripts/edit/MB/Control/URLCleanup.js
def amazon_url_asin(url):
    m = re.search(r'(?:/|\ba=)([A-Z0-9]{10})(?:[/?&%#]|$)', url)
    return m.group(1) if m else None

asin_removed = set((gid, url) for gid, url in db.execute('''SELECT gid, url FROM bot_asin_removed'''))
asin_problematic = set(url for url, in db.execute('''SELECT url FROM bot_asin_remove_problematic'''))
asin_missing = set(url for url, in db.execute('''SELECT url FROM bot_asin_remove_missing'''))
asin_no_barcode = set(url for url, in db.execute('''SELECT url FROM bot_asin_remove_no_barcode'''))

def are_similar(name1, name2):
    name1, name2 = (asciipunct(s.strip().lower()) for s in (name1, name2))
    ratio = Levenshtein.jaro_winkler(name1, name2)
    return ratio >= 0.8 or name1 in name2 or name2 in name1

def barcode_type(s):
    if len(s) == 8 or len(s) == 13:
        return 'EAN'
    elif len(s) == 12:
        return 'UPC'
    return 'barcode'

def amazon_url_tld(url):
    m = re.search(r'amazon\.([a-z\.]+)/', url)
    if m:
        tld = m.group(1)
        if tld == 'jp':
            tld = 'co.jp'
        if tld == 'at':
            tld = 'de'
        return tld
    return None

def amazon_url_loc(url):
    m = re.search(r'amazon\.([a-z\.]+)/', url)
    if m:
        tld = m.group(1)
        if tld == 'co.jp':
            tld = 'jp'
        if tld == 'at':
            tld = 'de'
        if tld == 'co.uk':
            tld = 'uk'
        if tld == 'com':
            tld = 'us'
        return tld
    return None

def amazon_url_cleanup(url, asin):
    tld = amazon_url_tld(url)
    if tld:
        return 'http://www.amazon.%s/gp/product/%s' % (tld, asin)
    return None

def amazon_lookup_asin(url):
    params = {
        'ResponseGroup' : 'ItemAttributes,Medium,Images',
        'IdType' : 'ASIN',
    }
    loc = amazon_url_loc(url)
    asin = amazon_url_asin(url)
    if loc not in amazon_api:
        amazon_api[loc] = amazonproduct.API(locale=loc)
    try:
        root = amazon_api[loc].item_lookup(asin, **params)
    except amazonproduct.errors.InvalidParameterValue, e:
        out(e)
        return None
    except amazonproduct.errors.AWSError, e:
        out(e)
        return None
    return root.Items.Item

def release_format(r):
    hist = defaultdict(int)
    text = []
    last = None
    for m, in db.execute('''SELECT mf.name FROM medium m LEFT JOIN medium_format mf ON mf.id = m.format WHERE m.release = %s ORDER BY m.position''', r):
        hist[m] += 1
        if last and last != m:
            text.append(u'%d × %s' % (hist[last], last))
            hist[last] = 0
        last = m
    text.append(u'%d × %s' % (hist[last], last))
    return u', '.join(text)

def artist_credit(ac):
    return u''.join(u'%s%s' % (name, join_phrase if join_phrase else u'') for name, join_phrase in db.execute('''SELECT an.name,acn.join_phrase from artist_credit ac JOIN artist_credit_name acn ON acn.artist_credit = ac.id JOIN artist_name an ON acn.name = an.id WHERE ac.id = %s ORDER BY position''', ac))

def format_release(release):
    rel_id, r, gid, barcode, name, ac = release
    return u'“%s” by “%s”, %s, %s, %s' % (name, artist_credit(ac), release_format(r), barcode_type(barcode), barcode)

def format_release2(release):
    rel_id, r, gid, barcode, name, ac = release
    return u'  http://musicbrainz.org/release/%s “%s” by “%s”, %s, %s, %s' % (gid, name, artist_credit(ac), release_format(r), barcode_type(barcode), barcode)

def main(verbose=False):
    normal_edits_left, edits_left = mb.edits_left()
    releases_by_url = defaultdict(list)
    for url, rel_id, r, gid, barcode, name, ac in db.execute(query_releases_with_duplicate_asin):
        releases_by_url[url] += [(rel_id, r, gid, barcode, name, ac)]
    count = len(releases_by_url)
    for i, (url, releases) in enumerate(releases_by_url.iteritems()):
        if normal_edits_left <= 0:
            break
        if url in asin_problematic or url in asin_missing or url in asin_no_barcode:
            continue
        if verbose:
            out(u'%d/%d - %.2f%%' % (i+1, count, (i+1) * 100.0 / count))
            out(u'%s' % url)
        try:
            item = amazon_lookup_asin(url)
        except (urllib2.HTTPError, urllib2.URLError, socket.timeout) as e:
            out(e)
            continue
        if item is None:
            if verbose:
                out('  not found, continue')
            db.execute("INSERT INTO bot_asin_remove_missing (url) VALUES (%s)", url)
            continue
        attrs = item.ItemAttributes
        asin_barcode = None
        if 'EAN' in attrs.__dict__:
            asin_barcode = unicode(attrs.EAN)
        elif 'UPC' in attrs.__dict__:
            asin_barcode = unicode(attrs.UPC)
        if not asin_barcode:
            if verbose:
                out(u'  no barcode, continue')
            db.execute("INSERT INTO bot_asin_remove_no_barcode (url) VALUES (%s)", url)
            continue
        matched = []
        not_matched = []
        for release in releases:
            rel_id, r, gid, barcode, name, ac = release
            if verbose:
                out(u'  %s http://musicbrainz.org/release/%s %s' % (name, gid, barcode))
            if barcode.lstrip('0') == asin_barcode.lstrip('0'):
                if verbose:
                    out(u'    matched')
                matched += [release]
            else:
                if verbose:
                    out(u'    NOT matched')
                if (gid, url) not in asin_removed:
                    not_matched += [release]
        if not (matched and not_matched):
            if verbose:
                out(u'  skip, not matched and not_matched')
            db.execute("INSERT INTO bot_asin_remove_problematic (url) VALUES (%s)", url)
            continue
        for not_matched_release in not_matched:
            text = u'Barcode mismatch!\nThis release: %s\n' % format_release(not_matched_release)
            text += u'Amazon.com: “%s”' % attrs.Title
            if 'Artist' in attrs.__dict__:
                text += u' by “%s”' % attrs.Artist
            if 'Binding' in attrs.__dict__:
                text += u', '
                if 'NumberOfDiscs' in attrs.__dict__:
                    text += u'%s × ' % attrs.NumberOfDiscs
                text += u'%s' % attrs.Binding
            if 'ReleaseDate' in attrs.__dict__:
                text += u', %s' % attrs.ReleaseDate
            text += u', %s %s' % (barcode_type(asin_barcode), asin_barcode)
            text += u'\n\nASIN is already (with correct barcode) attached to:\n'
            for matched_release in matched:
                text += u'%s\n' % format_release2(matched_release)
            text += '\n\n%s' % program_string(__file__)
            rel_id, r, gid, barcode, name, ac = not_matched_release
            try:
                out(u'http://musicbrainz.org/release/%s  remove  %s' % (gid,url))
                mb.remove_relationship(rel_id, 'release', 'url', text)
                db.execute("INSERT INTO bot_asin_removed (gid,url) VALUES (%s,%s)", (gid,url))
                normal_edits_left -= 1
            except (urllib2.HTTPError, urllib2.URLError, socket.timeout) as e:
                out(e)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-v', '--verbose', action='store_true', default=False,
            help='be more verbose')
    (options, args) = parser.parse_args()
    with PIDFile('/tmp/mbbot_asin_links_remove.pid'):
        main(options.verbose)
