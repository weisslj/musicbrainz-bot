#!/usr/bin/env python

import sys
import re
import urllib2
import urllib
import psycopg2
from psycopg2.extras import NamedTupleCursor

import config

db = psycopg2.connect(config.dbconn)
cur = db.cursor(cursor_factory=NamedTupleCursor)
cur2 = db.cursor(cursor_factory=NamedTupleCursor)

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

# Progress file
statefile = open('split_artists.db', 'r+')
state = set(x.strip() for x in statefile.readlines())

####

def done(gid):
    statefile.write("%s\n" % gid)
    statefile.flush()
    state.add(gid)

def encode_dict(d):
    l = []
    for k, v in sorted(d.items()):
        print "%s=%r" % (k, v)
        v = unicode(v).encode('utf8')
        l.append((k, v))
    print
    return urllib.urlencode(l)

USER_AGENT = 'brainybot (+https://musicbrainz.org/user/intgr_bot)'
def do_request(url, dic):
    print "POST", url

    rawdata = encode_dict(dic)
    req = urllib2.Request(config.url + url, data=rawdata, headers={'Cookie': config.cookie, 'User-Agent': USER_AGENT})
    resp = urllib2.urlopen(req)
    code = resp.getcode()
    data = resp.read()
    open('/tmp/%s.html' % url.replace('/', '_'), 'wb').write(data)
    assert code == 200
    assert "Thank you, your edit has been entered into the edit queue for peer review." in data

def split_artist(arts, joins, comment):
    assert len(arts) == len(joins)+1
    joins.append('')

    postdata = {}
    for i, (art, join) in enumerate(zip(arts, joins)):
        key = 'split-artist.artist_credit.names.%d.' % i
        postdata[key + 'name'] = ''
        postdata[key + 'artist.name'] = art.name
        postdata[key + 'artist.id'] = art.id
        postdata[key + 'join_phrase'] = join

    postdata['split-artist.edit_note'] = comment
    return postdata

def clean_link_phrase(phrase):
    return re.sub(r'\{[^}]+\}\s*', '', phrase).strip()

def get_score(src, dest):
    cur = db.cursor(cursor_factory=NamedTupleCursor)
    comment = u""
    score = 0

    cur.execute("""\
        SELECT short_link_phrase
        FROM l_artist_artist laa
        JOIN link l ON (laa.link=l.id)
        JOIN link_type lt ON (lt.id=link_type)
        WHERE entity0=%s AND entity1=%s""", [dest.id, src.id])
    for link in cur:
        score += 1
        comment += u"Relationship: %s %s %s\n" % (dest.name, clean_link_phrase(link.short_link_phrase), src.name)

    # Holy shitfuck!
    # artist -> artist_credit_name -> artist_credit -> track -> tracklist -> medium -> release

    return score, comment

def handle_artist(src):
    cur = db.cursor(cursor_factory=NamedTupleCursor)

    #print src
    match = re.split(split_re, src.name)
    names = match[0::2]
    joins = match[1::2]
    arts = []
    comment = u"Multiple artists. Only one artist credit.\n"

    if len(set(names)) != len(names):
        #print '  SKIP, dup names'
        return

    cur.execute("""\
        SELECT ac.id, ac.artist_count
        FROM artist_credit ac
        JOIN artist_credit_name acn ON (acn.artist_credit=ac.id)
        JOIN artist a ON (acn.artist=a.id)
        WHERE a.id=%s""", [src.id])
    if cur.rowcount != 1:
        #print '  SKIP %d credits' % cur.rowcount
        return
    cred = cur.fetchone()
    if cred.artist_count != 1:
        #print '  SKIP credit has multiple artists'
        return

    print "%s %sartist/%s" % (src.name, config.url, src.gid)

    for name in names:
        cur.execute("SELECT id, gid, name FROM s_artist WHERE name=%s", [name])
        assert cur.rowcount == 1, "%d rows" % cur2.rowcount
        dest = cur.fetchone()
        arts.append(dest)

        score, c = get_score(src, dest)
        print '  ', score, dest
        if not score:
            return
        if c:
            print '    ', c.strip()
        comment += c

    #print '  ', joins
    url = 'artist/%s/credit/%d/edit' % (src.gid, cred.id)
    postdata = split_artist(arts, joins, comment.strip())
    print '  SUBMITTING!', url
    do_request(url, postdata)
    done(src.gid)
    sys.exit()

split_re = '(, | & )'
query = """\
SELECT id, gid, name
FROM s_artist a
WHERE edits_pending=0 AND name ilike '%%&%%' AND true = ALL(
  -- SELECT exists(SELECT * FROM s_artist b WHERE name=c_name)
    SELECT (SELECT count(*)=1 FROM s_artist b WHERE name=c_name)
      FROM regexp_split_to_table(a.name, %(re)s) c_name
      ) AND array_length(regexp_split_to_array(a.name, %(re)s), 1) > 1
  --AND gid='a07fead8-b3a8-4ac9-9f4d-f59fb1a1d585'
--ORDER BY length(name) DESC
--LIMIT 1000
"""

cur.execute(query, {'re': split_re})
for art in cur:
    if art.gid in state:
        print "Skipping", art.gid
    else:
        handle_artist(art)
