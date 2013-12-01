#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
from collections import defaultdict
import urllib
import urllib2
import socket
from optparse import OptionParser

import sqlalchemy
import Levenshtein
import discogs_client as discogs

from editing import MusicBrainzClient
from utils import out, program_string, asciipunct
import config as cfg
from mbbot.utils.pidfile import PIDFile
import blacklist

'''
CREATE TABLE bot_discogs_artist_set (
    gid uuid NOT NULL,
    url text NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_discogs_artist_set_pkey PRIMARY KEY (gid,url)
);
CREATE TABLE bot_discogs_artist_problematic (
    gid uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_discogs_artist_problematic_pkey PRIMARY KEY (gid)
);
'''

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute('SET search_path TO musicbrainz, %s' % cfg.BOT_SCHEMA_DB)

editor_id = db.execute('''SELECT id FROM editor WHERE name = %s''', cfg.MB_USERNAME).first()[0]
mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE, editor_id=editor_id)

discogs.user_agent = 'MusicBrainzDiscogsReleaseGroupsBot/0.1 +https://github.com/weisslj/musicbrainz-bot'

query_missing = '''
SELECT r.id, r.gid, t.name, t.position, m.position, url.url, a.id, a.gid, ac.id
FROM release r
JOIN medium m ON m.release = r.id
JOIN track t ON t.medium = m.id
JOIN artist_credit ac ON ac.id = t.artist_credit
JOIN artist_credit_name acn ON acn.artist_credit = ac.id
JOIN artist a ON acn.artist = a.id
JOIN l_release_url l_ru ON r.id = l_ru.entity0
JOIN url ON url.id = l_ru.entity1
WHERE url.url IN (
    SELECT url.url
    FROM release r
    JOIN l_release_url l_ru ON r.id = l_ru.entity0
    JOIN link l ON l_ru.link = l.id
    JOIN url ON url.id = l_ru.entity1
    WHERE l.link_type = 76
    GROUP BY url.url
    HAVING COUNT(url.url) = 1
) AND ac.artist_count = 1 AND r.edits_pending = 0 AND a.edits_pending = 0 AND l_ru.edits_pending = 0 AND a.id IN (
    SELECT a.id
    FROM artist a

    EXCEPT

    SELECT a.id
    FROM artist a
    JOIN l_artist_url l_au ON a.id = l_au.entity0
    JOIN link l ON l_au.link = l.id
    WHERE l.link_type = 180
)
'''

def are_tracks_similar(name1, name2):
    name1, name2 = (asciipunct(s.strip().lower()) for s in (name1, name2))
    ratio = Levenshtein.jaro_winkler(name1, name2)
    return ratio >= 0.8 or name1 in name2 or name2 in name1

# is stricter, because we don't want to match e.g. "A" with "A feat. B"
def are_artists_similar(name1, name2):
    name1, name2 = (asciipunct(s.strip().lower()) for s in (name1, name2))
    ratio = Levenshtein.jaro_winkler(name1, name2, 0.0) # no common prefix length
    return ratio >= 0.8

MB_ENC_ALWAYS = '"<>\\^`{|} '
MB_UNENCODE = "!'()*~"
MB_ENC_NEVER  = '#$%&+,/:;=?@[]'

_hexdig = '0123456789ABCDEFabcdef'
_hextochr = dict((a + b, chr(int(a + b, 16)))
                 for a in _hexdig for b in _hexdig)

def unquote(s, safe=''):
    """unquote('abc%20def') -> 'abc def'."""
    res = s.split('%')
    # fastpath
    if len(res) == 1:
        return s
    s = res[0]
    for item in res[1:]:
        try:
            c = _hextochr[item[:2]]
            if c not in safe:
                s += c + item[2:]
            else:
                s += '%' + item
        except KeyError:
            s += '%' + item
        except UnicodeDecodeError:
            s += unichr(int(item[:2], 16)) + item[2:]
    return s

def musicbrainz_quote(s):
    return unicode(urllib.quote(unquote(s.encode('utf-8'), MB_ENC_NEVER), MB_UNENCODE+MB_ENC_NEVER), 'utf-8')

def discogs_quote(name):
    return unicode(urllib.quote_plus(name.encode('utf-8')), 'utf-8')

def combine_names(names):
    if len(names) > 1:
        return u' and '.join([', '.join([u'“'+n+u'”' for n in names[:-1]]), u'“'+names[-1]+u'”'])
    else:
        return u'“'+names[0]+u'”'

def artist_credit(ac):
    return u''.join(u'%s%s' % (name, join_phrase if join_phrase else u'') for name, join_phrase in db.execute('''SELECT acn.name,acn.join_phrase from artist_credit ac JOIN artist_credit_name acn ON acn.artist_credit = ac.id WHERE ac.id = %s ORDER BY position''', ac))

def discogs_artist_url(name):
    return u'http://www.discogs.com/artist/%s' % musicbrainz_quote(discogs_quote(name))

bot_blacklist = blacklist.discogs_links('artist')
bot_blacklist_new = set()
discogs_artist_set = set((gid, url) for gid, url in db.execute('''SELECT gid, url FROM bot_discogs_artist_set'''))
discogs_artist_set |= bot_blacklist
discogs_artist_problematic = set(gid for gid, in db.execute('''SELECT gid FROM bot_discogs_artist_problematic'''))

def main(verbose=False):
    normal_edits_left, edits_left = mb.edits_left()
    d = defaultdict(dict)

    for r, r_gid, t_name, t_pos, m_pos, url, a, a_gid, ac in db.execute(query_missing):
        if a_gid in discogs_artist_problematic:
            continue
        d[a][r] = (r, r_gid, t_name, t_pos, m_pos, url, a, a_gid, ac)

    count = len(d)
    for i, k in enumerate(d):
        if normal_edits_left <= 0:
            break
        if len(d[k]) != 1:
            continue
        r1 = list(d[k])[0]
        r, r_gid, t_name, t_pos, m_pos, url, a, a_gid, ac = d[k][r1]
        if m_pos > 1:
            db.execute("INSERT INTO bot_discogs_artist_problematic (gid) VALUES (%s)", a_gid)
            continue
        artist_releases = set([r for r, in db.execute('''SELECT DISTINCT r.id FROM release r JOIN medium m ON m.release = r.id JOIN track t ON t.medium = m.id WHERE t.artist_credit = %s''', ac)])
        if len(artist_releases) > 1:
            db.execute("INSERT INTO bot_discogs_artist_problematic (gid) VALUES (%s)", a_gid)
            continue
        if verbose:
            out(u'%d/%d - %.2f%%' % (i+1, count, (i+1) * 100.0 / count))
            out('http://musicbrainz.org/release/%s (%d-%d)' % (r_gid, m_pos, t_pos))
            out('%s' % url)
            out('http://musicbrainz.org/artist/%s' % a_gid)
        m = re.match(r'^http://www\.discogs\.com/release/([0-9]+)', url)
        if not m:
            if verbose:
                out('skip, is no valid Discogs release URL')
            continue
        discogs_release_id = int(m.group(1))
        discogs_release = discogs.Release(discogs_release_id)
        if discogs_release.data['status'] in ['Draft', 'Rejected']:
            if verbose:
                out('skip, release is not draft/rejected')
            db.execute("INSERT INTO bot_discogs_artist_problematic (gid) VALUES (%s)", a_gid)
            continue
        t_index = 0
        discogs_track = None
        for t in discogs_release.tracklist:
            if t['type'] == 'Track':
                t_index += 1
            if t_index == t_pos:
                discogs_track = t
                break
        if discogs_track is None:
            if verbose:
                out('track not found')
            db.execute("INSERT INTO bot_discogs_artist_problematic (gid) VALUES (%s)", a_gid)
            continue
        discogs_artists = discogs_track['artists']
        if len(discogs_artists) == 0:
            discogs_artists = discogs_release.artists
        if len(discogs_artists) != 1:
            if verbose:
                out('skip, %d track artists' % len(discogs_artists))
            db.execute("INSERT INTO bot_discogs_artist_problematic (gid) VALUES (%s)", a_gid)
            continue
        if not are_tracks_similar(discogs_track['title'], t_name):
            if verbose:
                out(u'not similar: %s <-> %s' % (discogs_track['title'], t_name))
            db.execute("INSERT INTO bot_discogs_artist_problematic (gid) VALUES (%s)", a_gid)
            continue
        discogs_artist = discogs_artists[0]
        if discogs_artist.name in [u'Various', u'Unknown Artist']:
            if verbose:
                out(u'not linking to Various or Unknown Artist')
            db.execute("INSERT INTO bot_discogs_artist_problematic (gid) VALUES (%s)", a_gid)
            continue
        ac_name = artist_credit(ac)
        norm_name = discogs_artist.name
        m = re.match(r'(.*?) \([0-9]+\)', norm_name)
        if m:
            norm_name = m.group(1)
        m = re.match(r'(.*?), (The)', norm_name)
        if m:
            norm_name = '%s %s' % (m.group(2), m.group(1))
        if not are_artists_similar(norm_name, ac_name):
            if verbose:
                out(u'not similar: %s [%s] <-> %s' % (norm_name, discogs_artist.name, ac_name))
            db.execute("INSERT INTO bot_discogs_artist_problematic (gid) VALUES (%s)", a_gid)
            continue
        discogs_url = discogs_artist_url(discogs_artist.name)
        if (a_gid, discogs_url) in discogs_artist_set:
            if verbose:
                out(u'  already linked earlier (probably got removed by some editor!')
            if (a_gid, discogs_url) not in bot_blacklist:
                bot_blacklist_new.add((a_gid, discogs_url))
            continue
        text = u'Artist appears on only one release [1] (e.g. medium %d, track %d), which is linked to discogs release [2]. Also, the track names are similar:\n' % (m_pos, t_pos)
        text += u'Discogs: “%s” by %s\n' % (discogs_track['title'], combine_names([x.name for x in discogs_artists]))
        text += u'MBrainz: “%s” by “%s”\n\n' % (t_name, ac_name)
        text += u'[1] http://musicbrainz.org/release/%s\n[2] %s' % (r_gid, url)
        text += '\n\n%s' % program_string(__file__)
        try:
            out(u'http://musicbrainz.org/artist/%s  ->  %s' % (a_gid,discogs_url))
            mb.add_url('artist', a_gid, 180, discogs_url.encode('utf-8'), text)
            db.execute("INSERT INTO bot_discogs_artist_set (gid,url) VALUES (%s,%s)", (a_gid, discogs_url))
            normal_edits_left -= 1
        except (urllib2.HTTPError, urllib2.URLError, socket.timeout) as e:
            out(e)
    if bot_blacklist_new:
        out(blacklist.wiki_markup(bot_blacklist_new, 'artist', db))

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-v', '--verbose', action='store_true', default=False,
            help='be more verbose')
    (options, args) = parser.parse_args()
    with PIDFile('/tmp/mbbot_discogs_links_track_artists.pid'):
        main(options.verbose)
