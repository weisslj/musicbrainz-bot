#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
from optparse import OptionParser
from collections import defaultdict

import sqlalchemy
import mechanize

import editing
from editing import MusicBrainzClient
from utils import out
from mbbot.utils.pidfile import PIDFile
import config as cfg

'''
CREATE TABLE bot_cc_removed (
    gid uuid NOT NULL,
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_cc_removed_pkey PRIMARY KEY (gid,url)
);
'''

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute('SET search_path TO musicbrainz')

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

query_releases_with_cc = '''
SELECT r.id, r.gid, r.artist_credit, release_name.name, url.url, l_ru.id
FROM release r
JOIN release_name ON r.name = release_name.id
JOIN l_release_url l_ru ON r.id = l_ru.entity0
JOIN link l ON l_ru.link = l.id
JOIN url ON url.id = l_ru.entity1
WHERE l.link_type = 84 AND l_ru.edits_pending = 0
GROUP BY r.id, r.gid, r.artist_credit, release_name.name, url.url, l_ru.id
ORDER BY r.artist_credit
'''

browser = mechanize.Browser()
browser.set_handle_robots(False)
browser.set_debug_redirects(False)
browser.set_debug_http(False)

html_escape_table = {
    u'&': u'&amp;',
    u''': u'&quot;',
    u''': u'&apos;',
    u'>': u'&gt;',
    u'<': u'&lt;',
}

def html_escape(text):
    return u''.join(html_escape_table.get(c,c) for c in text)

cc_removed = set((gid, url) for gid, url in db.execute('''SELECT gid, url FROM bot_cc_removed'''))

def main(verbose=False):
    releases = [(r, gid, ac, name, url, rel_id) for r, gid, ac, name, url, rel_id in db.execute(query_releases_with_cc)]
    count = len(releases)
    for i, (r, gid, ac, name, url, rel_id) in enumerate(releases):
        original_url = url
        #if not re.match(r'http://([^/]+\.)?(bandcamp\.com|archive\.org|magnatune\.com)/', url):
        #    continue
        if verbose:
            out(u'%d/%d - %.2f%%' % (i, count, i * 100.0 / count))
            out(u'%s - http://musicbrainz.org/release/%s - %s' % (name, gid, url))
        if re.match(r'http://([^/]+\.)?magnatune\.com/', url):
            license_urls = set([u'http://creativecommons.org/licenses/by-nc-sa/1.0/'])
        else:
            try:
                browser.open(url.encode('utf-8'))
            except:
                continue
            if not browser.response().info()['Content-type'].startswith('text'):
                if verbose:
                    out(u'not a text document, aborting!')
                continue
            page = browser.response().read()
            license_urls = set(re.findall(r'(http://creativecommons.org/licenses/[0-9A-Za-z/+.-]+)', page))
            if len(license_urls) == 0:
                url = u'http://web.archive.org/' + url
                if verbose:
                    out(u'no license url found, trying archive.org!')
                if u'jamendo.com' not in url:
                    continue
                try:
                    browser.open(url.encode('utf-8'))
                except:
                    continue
                page = browser.response().read()
                license_urls = set(re.findall(r'(http://creativecommons.org/licenses/[0-9A-Za-z/+.-]+)', page))
                if len (license_urls) == 0 and '<p class="impatient"><a href="http://web.archive.org' in page:
                    if verbose:
                        out(u'no license url found, trying archive.org AGAIN!')
                    m = re.search(r'<p class="impatient"><a href="([^"]+)">Impatient\?</a></p>', page)
                    if m and m.group(1):
                        url = m.group(1)
                        try:
                            browser.open(url.encode('utf-8'))
                        except:
                            continue
                        page = browser.response().read()
                        license_urls = set(re.findall(r'(http://creativecommons.org/licenses/[0-9A-Za-z/+.-]+)', page))
            if len(license_urls) > 1:
                if verbose:
                    out(u'more than one license url found, aborting!')
                continue
            if len(license_urls) == 0:
                if verbose:
                    out(u'no license url found, aborting!')
                continue
            if name.lower().encode('utf-8') not in page.lower() and html_escape(name.lower()).encode('utf-8') not in page.lower() and re.sub(r'( +e\.p\.| +ep|, volume [0-9]+)', u'', name.lower()).encode('utf-8') not in page.lower():
                if verbose:
                    out(u'album name not found in page, aborting!')
                continue
        license_url_raw = list(license_urls)[0]
        license_url = re.sub(r'((legalcode|deed)((\.|-)[a-z]+)?)$', u'', license_url_raw)
        if verbose:
            out(u'%s' % license_url)
        link_id = 75
        text = u'The [Creative_Commons_Licensed_Download_Relationship_Type] is obsoleted by a new [License_Relationship_Type].'
        text += u' All CC links will be replaced by a Free/Paid Download Relationship and a License URL.\n'
        if re.match(r'http://([^/]+\.)?magnatune\.com/', original_url):
            text += u'“All music files available to Magnatune members are licensed under a Creative Commons by-nc-sa v1.0 license.”\n'
            text += u'http://magnatune.com/info/cc_licensed'
            link_id = 74
        #elif re.match(r'http://([^/]+\.)?hhgroups\.com/', original_url):
        else:
            if re.match(r'http://([^/]+\.)?bandcamp\.com/', original_url):
                if not re.match(r'http://([^/]+\.)?bandcamp\.com/album/', original_url) and ('>%s</h2>' % name.lower().encode('utf-8')) not in page.lower():
                    if verbose:
                        out(u'not the bandcamp page for this album, aborting!')
                    continue
                if '>Free Download</a>' not in page:
                    if '>Buy Now</a>' in page:
                        link_id = 74
                    else:
                        if verbose:
                            out(u'could not determine kind of download (free/paid), aborting!')
                        continue
            text += u'I’m converting this relationship because I’ve found a link to %s in the linked page %s.' % (license_url_raw, url)
        mb.add_url('release', gid, 301, license_url, text, auto=False)
        if not mb.edit_relationship(rel_id, 'release', 'url', 84, link_id, {'license.0': []}, {}, {}, text, auto=False):
            if (gid, original_url) not in cc_removed:
                text = u'Download and License relationship are already set, so this relationship is not necessary anymore.'
                mb.remove_relationship(rel_id, 'release', 'url', text)
                db.execute("INSERT INTO bot_cc_removed (gid,url) VALUES (%s,%s)", (gid,original_url))
                cc_removed.add((gid, original_url))

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-v', '--verbose', action='store_true', default=False,
            help='be more verbose')
    (options, args) = parser.parse_args()
    with PIDFile('/tmp/mbbot_convert_cc_links.pid'):
        main(options.verbose)
