#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import urllib2
import socket
from optparse import OptionParser

import sqlalchemy
import Levenshtein
import discogs_client as discogs

from editing import MusicBrainzClient
import config as cfg
from utils import out, program_string, asciipunct
from mbbot.utils.pidfile import PIDFile
import blacklist

'''
CREATE TABLE bot_discogs_release_group_set (
    gid uuid NOT NULL,
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_discogs_release_group_set_pkey PRIMARY KEY (gid,url)
);
CREATE TABLE bot_discogs_release_group_missing (
    gid uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_discogs_release_group_missing_pkey PRIMARY KEY (gid)
);
CREATE TABLE bot_discogs_release_group_problematic (
    gid uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_discogs_release_group_problematic_pkey PRIMARY KEY (gid)
);
'''

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute('SET search_path TO musicbrainz, %s' % cfg.BOT_SCHEMA_DB)

editor_id = db.execute('''SELECT id FROM editor WHERE name = %s''', cfg.MB_USERNAME).first()[0]
mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE, editor_id=editor_id)

discogs.user_agent = 'MusicBrainzDiscogsReleaseGroupsBot/0.1 +https://github.com/weisslj/musicbrainz-bot'

query_rg_without_master = '''
SELECT rg.id, rg.gid, release_name.name
FROM release_group rg
JOIN release_name ON rg.name = release_name.id
WHERE rg.id IN (
    SELECT DISTINCT rg.id
    FROM release_group rg
    JOIN release ON rg.id = release.release_group
    JOIN l_release_url l_ru ON release.id = l_ru.entity0
    JOIN link l ON l_ru.link = l.id
    WHERE l.link_type = 76 AND rg.edits_pending = 0 AND release.edits_pending = 0
        AND l_ru.edits_pending = 0
    
    EXCEPT
    
    SELECT rg.id
    FROM release_group rg
    JOIN l_release_group_url l_rgu ON rg.id = l_rgu.entity0
    JOIN link l ON l_rgu.link = l.id
    WHERE l.link_type = 90
)
ORDER BY rg.artist_credit
'''

query_rg_release_discogs = '''
SELECT url.url
FROM l_release_url l_ru
JOIN link l ON l_ru.link = l.id
JOIN release ON release.id = l_ru.entity0
JOIN release_group rg ON rg.id = release.release_group
JOIN release_name ON release.name = release_name.id
JOIN url ON url.id = l_ru.entity1
WHERE release.release_group = %s AND l.link_type = 76
'''

bot_blacklist = blacklist.discogs_links('release-group')
bot_blacklist_new = set()
discogs_release_group_set = set((gid, url) for gid, url in db.execute('''SELECT gid, url FROM bot_discogs_release_group_set'''))
discogs_release_group_set |= bot_blacklist
discogs_release_group_missing = set(gid for gid, in db.execute('''SELECT gid FROM bot_discogs_release_group_missing'''))
discogs_release_group_problematic = set(gid for gid, in db.execute('''SELECT gid FROM bot_discogs_release_group_problematic'''))

def are_similar(name1, name2):
    name1, name2 = (asciipunct(s.strip().lower()) for s in (name1, name2))
    ratio = Levenshtein.jaro_winkler(name1, name2)
    return ratio >= 0.8 or name1 in name2 or name2 in name1

def discogs_artists_str(artists):
    if len(artists) > 1:
        return ' and '.join([', '.join([a.name for a in artists[:-1]]), artists[-1].name])
    else:
        return artists[0].name

def discogs_get_master(release_urls):
    for release_url in release_urls:
        m = re.match(r'http://www.discogs.com/release/([0-9]+)', release_url)
        if m:
            release_id = int(m.group(1))
            release = discogs.Release(release_id)
            master = release.master
            if master:
                yield (master.title, master._id, discogs_artists_str(master.artists))

def main(verbose=False):
    normal_edits_left, edits_left = mb.edits_left()
    rgs = [(rg, gid, name) for rg, gid, name in db.execute(query_rg_without_master)]
    count = len(rgs)
    for i, (rg, gid, name) in enumerate(rgs):
        if edits_left <= 0:
            break
        if gid in discogs_release_group_missing or gid in discogs_release_group_problematic:
            continue
        urls = set(url for url, in db.execute(query_rg_release_discogs, rg))
        if verbose:
            out(u'%d/%d - %.2f%%' % (i+1, count, (i+1) * 100.0 / count))
            out(u'%s http://musicbrainz.org/release-group/%s' % (name, gid))
        try:
            masters = list(discogs_get_master(urls))
        except (discogs.HTTPError, discogs.requests.ConnectionError) as e:
            out('  ERROR: discogs_get_master(%s): %s' % (list(urls), e))
            continue
        if len(masters) == 0:
            if verbose:
                out(u'  aborting, no Discogs master!')
            db.execute("INSERT INTO bot_discogs_release_group_missing (gid) VALUES (%s)", gid)
            continue
        if len(set(masters)) > 1:
            if verbose:
                out(u'  aborting, releases with different Discogs master in one group!')
            db.execute("INSERT INTO bot_discogs_release_group_problematic (gid) VALUES (%s)", gid)
            continue
        if len(masters) != len(urls):
            if verbose:
                out(u'  aborting, releases without Discogs master in group!')
            db.execute("INSERT INTO bot_discogs_release_group_problematic (gid) VALUES (%s)", gid)
            continue
        master_name, master_id, master_artists = masters[0]
        if not are_similar(master_name, name):
            if verbose:
                out(u'  Similarity too small: %s <-> %s' % (name, master_name))
            db.execute("INSERT INTO bot_discogs_release_group_problematic (gid) VALUES (%s)", gid)
            continue
        master_url = 'http://www.discogs.com/master/%d' % master_id
        if (gid, master_url) in discogs_release_group_set:
            if verbose:
                out(u'  already linked earlier (probably got removed by some editor!')
            if (gid, master_url) not in bot_blacklist:
                bot_blacklist_new.add((gid, master_url))
            continue
        if len(urls) >= 2:
            text = u'There are %d distinct Discogs links in this release group, and all point to this master URL.\n' % len(urls)
        else:
            text = u'There is one Discogs link in this release group, and it points to this master URL.\n%s\n' % list(urls)[0]
        text += u'Also, the name of the Discogs master “%s” (by %s) is similar to the release group name.' % (master_name, master_artists)
        text += '\n\n%s' % program_string(__file__)
        try:
            out(u'http://musicbrainz.org/release-group/%s  ->  %s' % (gid,master_url))
            mb.add_url('release_group', gid, 90, master_url, text, auto=True)
            db.execute("INSERT INTO bot_discogs_release_group_set (gid,url) VALUES (%s,%s)", (gid,master_url))
            edits_left -= 1
        except (urllib2.HTTPError, urllib2.URLError, socket.timeout) as e:
            out('  ERROR: mb.add_url(%s, %s): %s' % (gid, master_url, e))
            out(e)
    if bot_blacklist_new:
        out(blacklist.wiki_markup(bot_blacklist_new, 'release-group', db))

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-v', '--verbose', action='store_true', default=False,
            help='be more verbose')
    (options, args) = parser.parse_args()
    with PIDFile('/tmp/mbbot_discogs_links_release_groups.pid'):
        main(options.verbose)
