# -*- coding: utf-8 -*-
import re
import urllib2
import socket
from collections import defaultdict
import sqlalchemy
import Levenshtein
import amazonproduct
from editing import MusicBrainzClient
import config as cfg
from utils import out
from mbbot.utils.pidfile import PIDFile

'''
CREATE TABLE bot_asin_set (
    gid uuid NOT NULL,
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_asin_set_pkey PRIMARY KEY (gid,url)
);
CREATE TABLE bot_asin_missing (
    gid uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_asin_missing_pkey PRIMARY KEY (gid)
);
CREATE TABLE bot_asin_nocover (
    gid uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_asin_nocover_pkey PRIMARY KEY (gid)
);
CREATE TABLE bot_asin_problematic (
    gid uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_asin_problematic_pkey PRIMARY KEY (gid)
);
CREATE TABLE bot_asin_catmismatch (
    gid uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_asin_catmismatch_pkey PRIMARY KEY (gid)
);
'''

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute('SET search_path TO musicbrainz')

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

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

query_releases_without_asin = '''
SELECT r.id, r.gid, r.barcode, release_name.name, r.artist_credit, c.iso_code, date_year, date_month, date_day
FROM release r
JOIN release_name ON r.name = release_name.id
JOIN country c ON r.country = c.id
JOIN artist_credit ac ON r.artist_credit = ac.id
JOIN artist_name an ON ac.name = an.id
JOIN artist_credit_name AS acn ON acn.artist_credit = r.artist_credit
JOIN artist AS artist ON artist.id = acn.artist
JOIN release_status AS rs ON r.status = rs.id
WHERE r.edits_pending = 0 AND rs.name != 'Pseudo-Release' AND (r.comment IS NULL OR r.comment !~* 'mispress') AND r.id IN (
    SELECT r.id
    FROM release r

    EXCEPT

    SELECT DISTINCT r.id
    FROM release r
    JOIN l_release_url l_ru ON r.id = l_ru.entity0
    JOIN link l ON l_ru.link = l.id
    JOIN link_type lt ON l.link_type = lt.id
    WHERE lt.name = 'amazon asin'
) AND r.barcode IN (
    SELECT r.barcode
    FROM release r
    WHERE r.barcode IS NOT NULL AND r.barcode != ''
    GROUP BY r.barcode
    HAVING COUNT(r.barcode) = 1
)
GROUP BY r.id, r.gid, r.barcode, release_name.name, c.iso_code, r.artist_credit
ORDER BY r.artist_credit
'''

def amazon_url_asin(url):
    m = re.search(r'(?:/|\ba=)([A-Z0-9]{10})(?:[/?&%#]|$)', url)
    return m.group(1) if m else None

asin_set = set((gid, url) for gid, url in db.execute('''SELECT gid, url FROM bot_asin_set'''))
asin_missing = set(gid for gid, in db.execute('''SELECT gid FROM bot_asin_missing'''))
asin_nocover = set(gid for gid, in db.execute('''SELECT gid FROM bot_asin_nocover'''))
asin_problematic = set(gid for gid, in db.execute('''SELECT gid FROM bot_asin_problematic'''))
asin_catmismatch = set(gid for gid, in db.execute('''SELECT gid FROM bot_asin_catmismatch'''))
asins = set(amazon_url_asin(url) for url, in db.execute("""SELECT url.url FROM url WHERE url.url ~ '^http://www\.amazon\.'"""))
barcodes_hist = defaultdict(int)
for barcode, in db.execute("""SELECT DISTINCT barcode FROM release WHERE barcode IS NOT NULL AND barcode != ''"""):
    barcodes_hist[barcode.lstrip('0')] += 1

def asciipunct(s):
    mapping = {
        u"…": u"...",
        u"‘": u"'",
        u"’": u"'",
        u"‚": u"'",
        u"“": u"\"",
        u"”": u"\"",
        u"„": u"\"",
        u"′": u"'",
        u"″": u"\"",
        u"‹": u"<",
        u"›": u">",
        u"‐": u"-",
        u"‒": u"-",
        u"–": u"-",
        u"−": u"-",
        u"—": u"-",
        u"―": u"-",
    }
    for orig, repl in mapping.iteritems():
        s = s.replace(orig, repl)
    return s

def are_similar(name1, name2):
    name1, name2 = (asciipunct(s.strip().lower()) for s in (name1, name2))
    ratio = Levenshtein.jaro_winkler(name1, name2)
    return ratio >= 0.8 or name1 in name2 or name2 in name1

def barcode_type(s):
    if len(s) == 8 or len(s) == 13:
        return 'EAN'
    elif len(s) == 12:
        return 'UPC'
    return None

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

def amazon_url_cleanup(url):
    tld = amazon_url_tld(url)
    asin = amazon_url_asin(url)
    if tld and asin:
        return 'http://www.amazon.%s/gp/product/%s' % (tld, asin)
    return None

def amazon_get_asin(barcode, country):
    params = {
        'ResponseGroup' : 'Medium,Images',
        'SearchIndex' : 'Music',
        'IdType' : barcode_type(barcode),
    }
    item = None
    for loc in store_map_rev[country]:
        if loc not in amazon_api:
            amazon_api[loc] = amazonproduct.API(locale=loc)
        try:
            root = amazon_api[loc].item_lookup(barcode, **params)
        except amazonproduct.errors.InvalidParameterValue, e:
            continue
        item = root.Items.Item
        if not 'LargeImage' in item.__dict__:
            continue
        attrs = item.ItemAttributes
        if 'Format' in attrs.__dict__ and 'Import' in [f for f in attrs.Format]:
            continue
        break
    return item

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

def date_format(year, month, day):
    if day:
        return u'%04d-%02d-%02d' % (year, month, day)
    if month:
        return u'%04d-%02d' % (year, month)
    return u'%04d' % year

def release_labels(r):
    return [name for name, in db.execute('''SELECT ln.name FROM release_label rl JOIN label l ON rl.label = l.id JOIN label_name ln ON l.name = ln.id WHERE rl.release = %s''', r)]

def release_catnrs(r):
    return [cat for cat, in db.execute('''SELECT catalog_number FROM release_label WHERE release = %s''', r) if cat]

def artist_countries(r):
    return [country for country, in db.execute('''SELECT DISTINCT c.iso_code FROM release r JOIN artist_credit_name AS acn ON acn.artist_credit = r.artist_credit JOIN artist AS artist ON artist.id = acn.artist JOIN country c ON c.id = artist.country WHERE r.id = %s''', r) if country]

def cat_normalize(cat, country):
    if country == 'JP':
        m = re.match(ur'^([0-9a-zA-Z]+)[ .-]*([0-9]+)(?:[^0-9]|$)', cat)
        return (u'%s%s' % m.groups()).upper() if m else None
    else:
        return re.sub(r'[ -]+', r'', cat).upper()

def cat_compare(a, b, country):
    a = cat_normalize(a, country)
    b = cat_normalize(b, country)
    return a and b and a == b

def main():
    releases = [(r, gid, barcode, name, ac, country, year, month, day) for r, gid, barcode, name, ac, country, year, month, day in db.execute(query_releases_without_asin)]
    count = len(releases)
    for i, (r, gid, barcode, name, ac, country, year, month, day) in enumerate(releases):
        if gid in asin_missing or gid in asin_problematic or gid in asin_nocover or gid in asin_catmismatch:
            continue
        if not barcode_type(barcode):
            db.execute("INSERT INTO bot_asin_problematic (gid) VALUES (%s)", gid)
            continue
        if country not in store_map_rev:
            continue
        if barcode.lstrip('0') in barcodes_hist and barcodes_hist[barcode.lstrip('0')] > 1:
            out('  two releases with same barcode, skip for now')
            db.execute("INSERT INTO bot_asin_problematic (gid) VALUES (%s)", gid)
            continue
        out(u'%d/%d - %.2f%%' % (i+1, count, (i+1) * 100.0 / count))
        out(u'%s http://musicbrainz.org/release/%s %s %s' % (name, gid, barcode, country))
        try:
            item = amazon_get_asin(barcode, country)
        except (urllib2.HTTPError, urllib2.URLError, socket.timeout) as e:
            out(e)
            continue
        if item is None:
            out('  not found, continue')
            db.execute("INSERT INTO bot_asin_missing (gid) VALUES (%s)", gid)
            continue
        url = amazon_url_cleanup(str(item.DetailPageURL))
        out(url)
        if item.ASIN in asins:
            out('  skip, ASIN already in DB')
            db.execute("INSERT INTO bot_asin_problematic (gid) VALUES (%s)", gid)
            continue
        if not 'LargeImage' in item.__dict__:
            out('  skip, has no image')
            db.execute("INSERT INTO bot_asin_nocover (gid) VALUES (%s)", gid)
            continue
        attrs = item.ItemAttributes
        if 'Format' in attrs.__dict__ and 'Import' in [f for f in attrs.Format]:
            out('  skip, is marked as Import')
            db.execute("INSERT INTO bot_asin_problematic (gid) VALUES (%s)", gid)
            continue
        amazon_name = unicode(attrs.Title)
        catnr = None
        if 'SeikodoProductCode' in attrs.__dict__:
            catnr = unicode(attrs.SeikodoProductCode)
        elif 'MPN' in attrs.__dict__:
            catnr = unicode(attrs.MPN)
        matched = False
        if catnr:
            for mb_catnr in release_catnrs(r):
                if cat_compare(mb_catnr, catnr, country):
                    matched = True
                    break
            if not matched and country == 'JP':
                out(u'  CAT NR MISMATCH, ARGH!')
                db.execute("INSERT INTO bot_asin_catmismatch (gid) VALUES (%s)", gid)
                continue
        if not matched:
            catnr = None
            if not are_similar(name, amazon_name):
                out(u'  Similarity too small: %s <-> %s' % (name, amazon_name))
                db.execute("INSERT INTO bot_asin_problematic (gid) VALUES (%s)", gid)
                continue
        if (gid, url) in asin_set:
            out(u'  already linked earlier (probably got removed by some editor!')
            continue
        text = u'%s lookup for “%s” (country: %s), ' % (barcode_type(barcode), barcode, country)
        if catnr:
            text += u'matching catalog numer “%s”, release name is “%s”' % (catnr, attrs.Title)
        else:
            text += u'has similar name “%s”' % attrs.Title
        if 'Artist' in attrs.__dict__:
            text += u' by “%s”' % attrs.Artist
        text += u'.\nAmazon.com: '
        if 'Binding' in attrs.__dict__:
            if 'NumberOfDiscs' in attrs.__dict__:
                text += u'%s × ' % attrs.NumberOfDiscs
            text += u'%s' % attrs.Binding
        if not catnr and 'Label' in attrs.__dict__:
            text += u', %s' % attrs.Label
        if 'ReleaseDate' in attrs.__dict__:
            text += u', %s' % attrs.ReleaseDate
        text += u'\nMusicBrainz: '
        text += u'%s' % release_format(r)
        if not catnr:
            labels = release_labels(r)
            if labels:
                text += u', %s' % u' / '.join(labels)
        if year:
            text += u', %s' % date_format(year, month, day)
        if catnr and country == 'JP':
            text += u'\nhttp://amazon.jp/s?field-keywords=%s\nhttp://amazon.jp/s?field-keywords=%s' % (catnr, barcode)
        else:
            text += u'\nhttp://amazon.%s/s?field-keywords=%s' % (amazon_url_tld(url), barcode)
        try:
            mb.add_url('release', gid, 77, url, text)
            db.execute("INSERT INTO bot_asin_set (gid,url) VALUES (%s,%s)", (gid,url))
        except (urllib2.HTTPError, urllib2.URLError, socket.timeout) as e:
            out(e)

if __name__ == '__main__':
    with PIDFile('/tmp/mbbot_asin_links.pid'):
        main()