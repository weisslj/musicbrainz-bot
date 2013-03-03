#!/usr/bin/env python
# encoding=utf-8

import re
import urllib2
import urllib
import psycopg2
from psycopg2.extras import NamedTupleCursor

import config

db = psycopg2.connect(config.dbconn)

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY)

# Progress file
statefile = open('split_artists.db', 'r+')
state = set(x.strip() for x in statefile.readlines())

####

def done(gid):
    if not config.dry_run:
        statefile.write("%s\n" % gid)
        statefile.flush()
    state.add(gid)

def encode_dict(d):
    l = []
    for k, v in sorted(d.items()):
        print "  %s=%r" % (k, v)
        v = unicode(v).encode('utf8')
        l.append((k, v))
    print
    return urllib.urlencode(l)

USER_AGENT = 'brainybot (+https://musicbrainz.org/user/intgr_bot)'
def do_request(url, dic):
    print "POST", url

    rawdata = encode_dict(dic)
    if config.dry_run:
        return
    req = urllib2.Request(config.url + url, data=rawdata, headers={'Cookie': config.cookie, 'User-Agent': USER_AGENT})
    resp = urllib2.urlopen(req)
    code = resp.getcode()
    data = resp.read()
    open('/tmp/%s.html' % url.replace('/', '_'), 'wb').write(data)
    assert code == 200
    assert "Thank you, your edit has been entered into the edit queue for peer review." in data

def do_del_relationship(rel_id, comment):
    postdata = {'confirm.edit_note': comment}
    do_request('edit/relationship/delete?type1=artist&type0=artist&id=%d' % rel_id, postdata)

def construct_post(arts, names, joins, comment):
    assert len(arts) == len(names) == len(joins)+1
    joins.append('')

    postdata = {}
    for i, (art, name, join) in enumerate(zip(arts, names, joins)):
        key = 'split-artist.artist_credit.names.%d.' % i
        postdata[key + 'name'] = name if art.name!=name else ""
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
    rels = []

    cur.execute("""\
        SELECT laa.id, short_link_phrase, link_type
        FROM l_artist_artist laa
        JOIN link l ON (laa.link=l.id)
        JOIN link_type lt ON (lt.id=link_type)
        WHERE entity0=%s AND entity1=%s""", [dest.id, src.id])
    for link in cur:
        if link.link_type != 102: # "collaborated on"
            # Wrong relationship type, can't handle that
            return -1, rels, comment
        score += 1
        comment += u"Relationship: %s %s %s\n" % (dest.name, clean_link_phrase(link.short_link_phrase), src.name)
        rels.append(link)

    # Holy shitfuck!
    # artist <- artist_credit_name <- artist_credit -> track -> tracklist <- medium -> release -> release_name
    cur.execute("""\
        SELECT r.id, r.gid, rn.name, r.release_group,
            string_agg(distinct t1.number, ', ' order by t1.number) as src_tracks,
            string_agg(distinct t2.number, ', ' order by t2.number) as dest_tracks
        FROM release r
        JOIN release_name rn ON (r.name=rn.id)
            /* FROM artist_credit_name acn1
            JOIN artist_credit ac1 ON (ac1.name=acn1.id)
            JOIN track t1 ON (t1.artist_credit=ac1.id)
            JOIN tracklist tl1 ON (t1.tracklist=tl1.id)
            JOIN medium m1 ON (m1.tracklist=tl1.id)*/
        JOIN medium m1 ON (m1.release=r.id)
            JOIN tracklist tl1 ON (m1.tracklist=tl1.id)
            JOIN track t1 ON (t1.tracklist=tl1.id)
            JOIN artist_credit ac1 ON (t1.artist_credit=ac1.id)
            JOIN artist_credit_name acn1 ON (ac1.id=acn1.artist_credit)
        JOIN medium m2 ON (m2.release=r.id)
            JOIN tracklist tl2 ON (m2.tracklist=tl2.id)
            JOIN track t2 ON (t2.tracklist=tl2.id)
            JOIN artist_credit ac2 ON (t2.artist_credit=ac2.id)
            JOIN artist_credit_name acn2 ON (ac2.id=acn2.artist_credit)
        WHERE ac1.artist_count=1
          AND acn1.artist=%s
          AND acn2.artist=%s
        GROUP BY 1,2,3
        ORDER BY count(distinct t1.position)+count(distinct t2.position) DESC, rn.name
        """, [src.id, dest.id])

    rgs = set()
    for rel in cur:
        # Don't report same release group multiple times. ORDER takes care of finding the best-matching one
        if rel.release_group not in rgs:
            rgs.add(rel.release_group)
            score += 1
            comment += u"\"%s\" has tracks from %s (%s) and collaboration (%s): %srelease/%s\n" % (rel.name, dest.name, rel.dest_tracks, rel.src_tracks, config.url, rel.gid)

    return score, rels, comment

def find_best_artist(src, name):
    cur = db.cursor(cursor_factory=NamedTupleCursor)
    cur.execute("SELECT id, gid, name FROM s_artist WHERE lower(name)=lower(%s)", [name])
    matches = []

    # Find the best-matching artist. Currently we only accept 1 positive-score artist, otherwise it's considered ambiguous
    for art in cur:
        score, rels, c = get_score(src, art)
        print "  %d %s: %sartist/%s" % (score, art.name, config.url, art.gid)
        if score <= 0:
            continue

        matches.append((art, rels, c))
        if c:
            print '    ', c.strip().replace('\n', '\n     ')

    if len(matches) == 1:
        return matches[0]
    else:
        # Too many/too few matches
        print "  SKIP, found %d positive matches for %s" % (len(matches), name)
        return None, None, None

def prompt(question):
    answer = None
    while answer not in ['y', 'n']:
        print question,
        answer = raw_input().strip()

    return answer == 'y'

def handle_credit(src, cred, comment):
    del_rels = []
    match = re.split(split_re, src.name)
    names = match[0::2]
    joins = match[1::2]
    arts = []
    if len(set(names)) != len(names):
        #print '  SKIP, dup names'
        return None, None

    assert len(names) > 1

    for name in names:
        art, rels, c = find_best_artist(src, name)
        if not art:
            return None, None
        arts.append(art)
        del_rels.extend(rels)
        comment += c

    # Will call do_request with these values
    url = 'artist/%s/credit/%d/edit' % (src.gid, cred.id)
    postdata = construct_post(arts, names, joins, comment.strip())
    cred_tx = (url, postdata)

    return cred_tx, del_rels

def handle_artist(src):
    cur = db.cursor(cursor_factory=NamedTupleCursor)

    #print src
    print "%s (%d refs, %d rec): %sartist/%s" % (src.name, src.ref_count, src.r_count, config.url, src.gid)

    cur.execute("""\
        SELECT ac.id, ac.artist_count, ac_an.name, ac.ref_count
        FROM artist_credit ac
        JOIN artist_credit_name acn ON (acn.artist_credit=ac.id)
        JOIN artist a ON (acn.artist=a.id)
        JOIN artist_name ac_an ON (ac.name=ac_an.id)
        WHERE a.id=%s""", [src.id])
    cred_count = cur.rowcount
    #if cred_count < 2:
    #    return
    #if cur.rowcount != 1:
    #    print '  SKIP %d credits' % cur.rowcount
    #    return
    #cred = cur.fetchone()
    #if cred.artist_count != 1:
    #    print "  SKIP credit has multiple artists"
    #    return
    del_rels = None
    cred_txs = []
    for cred in cur:
        print "  ----"
        if cred.name != src.name:
            # Issue #1
            # Artist name "Giraut de Bornelh & Peire Cardenal"
            # Credited as "Giraut de Bornelh - Peire Cardenal"
            print "  SKIP artist credit \"%s\" has different name" % cred.name
            continue

        comment = u"Multiple artists. %d attached artist credits. No [other] relationships. Credit used %d times (%d recordings)." % (cred_count, cred.ref_count, src.r_count)
        if config.confirm:
            comment += " Edit confirmed by human."
        comment += "\n"

        last_rels = del_rels
        cred_tx, del_rels = handle_credit(src, cred, comment)
        if cred_tx is None:
            continue
        cred_txs.append(cred_tx)

        # Sanity check, every credit must find the same rels to remove
        if last_rels is not None:
            assert last_rels == del_rels

    if not cred_txs:
        return

    if config.confirm:
        if not prompt("Submit? [y/n]"):
            return

    # Complete all transactions
    for tx in cred_txs:
        do_request(*tx)

    # Only delete relationships if all credits were fixed
    if len(cred_txs) == cred_count:
        for rel in del_rels:
            # Will call do_del_relationship with these values
            do_del_relationship(rel.id, "Deleting relationship, so empty collaboration artist can be removed.\nSee: %sartist/%s/open_edits" % (config.url, src.gid))

    # Only delete relationships if all credits were renamed
    done(src.gid)

split_re = ur'((?:\s*[*&+,/]\s*|(?:\s*,)?\s+(?:&|and|feat\.?|vs\.?|presents|with|-|und|ja|og|och|et|и)\s+))'
query = """\
SELECT a.id, a.gid, an.name, ac.ref_count,
    (SELECT count(*)
     FROM recording r
     JOIN artist_credit ac ON (r.artist_credit=ac.id)
     JOIN artist_credit_name acn ON (acn.artist_credit=ac.id)
     WHERE acn.artist=a.id) AS r_count
FROM artist a
    JOIN artist_name an ON (a.name=an.id)
JOIN artist_credit_name acn ON (a.id=acn.artist)
    JOIN artist_credit ac ON (acn.artist_credit=ac.id)
    --JOIN artist_name ac_an ON (ac.name=ac_an.id)

WHERE edits_pending=0
  AND a.name = ac.name -- must have same name
  AND an.name ~ %(re)s
  AND true = ALL(
    SELECT exists(SELECT * FROM s_artist b WHERE lower(name)=c_name)
      FROM regexp_split_to_table(lower(an.name), %(re)s) c_name
      ) AND array_length(regexp_split_to_array(an.name, %(re)s), 1) > 1

  -- l_artist_label is handled differently in Python code
  AND not exists(SELECT * FROM l_artist_label         WHERE entity0=a.id)
  AND not exists(SELECT * FROM l_artist_recording     WHERE entity0=a.id)
  AND not exists(SELECT * FROM l_artist_release       WHERE entity0=a.id)
  AND not exists(SELECT * FROM l_artist_release_group WHERE entity0=a.id)
  AND not exists(SELECT * FROM l_artist_url           WHERE entity0=a.id)
  AND not exists(SELECT * FROM l_artist_work          WHERE entity0=a.id)
ORDER BY ac.ref_count, r_count
"""

def run_bot():
    cur = db.cursor(cursor_factory=NamedTupleCursor)
    args = {'re': split_re}
    print cur.mogrify(query, args)
    cur.execute(query, args)
    print "TOTAL found", cur.rowcount
    for art in cur:
        if art.gid in state:
            print "Skipping", art.gid
        else:
            handle_artist(art)

if __name__=='__main__':
    run_bot()
