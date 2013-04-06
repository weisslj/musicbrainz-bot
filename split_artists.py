#!/usr/bin/env python
# encoding=utf-8
#### Local config

CONFIRM = True
DRY_RUN = False
REFRESH_INTERVAL = '1 week'

"""
-- Database schema
CREATE SCHEMA mbbot;

CREATE TABLE mbbot.split_artists_history(
    artist       int            not null,
    credit       int            ,
    changed      bool           not null,
    bot_version  smallint       not null,
    time         timestamptz    not null default now()
);
CREATE INDEX split_artists_history_credit_bot_version_time_idx
    ON mbbot.split_artists_history(credit, bot_version, time);
"""

#### Bot code

import re
import sys
import psycopg2
from psycopg2.extras import NamedTupleCursor

from editing import MusicBrainzClient
import config

####

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
        WHERE entity0=%s AND entity1=%s""", [dest.id, src.a_id])
    for link in cur:
        if link.link_type != 102: # "collaborated on"
            # Wrong relationship type, can't handle that
            return -1, rels, comment
        score += 1
        comment += u"Relationship: %s %s %s\n" % (dest.description, clean_link_phrase(link.short_link_phrase), src.name)
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
        """, [src.a_id, dest.id])

    rgs = set()
    for rel in cur:
        # Don't report same release group multiple times. ORDER takes care of finding the best-matching one
        if rel.release_group not in rgs:
            rgs.add(rel.release_group)
            score += 1
            comment += u"\"%s\" has tracks from %s (%s) and collaboration (%s): %s/release/%s\n" % (rel.name, dest.description, rel.dest_tracks, rel.src_tracks, config.MB_SITE, rel.gid)

    return score, rels, comment

def find_best_artist(src, name):
    cur = db.cursor(cursor_factory=NamedTupleCursor)
    cur.execute("""\
    -- Don't return same artist twice even if both alias and name match
    SELECT DISTINCT ON (id) * FROM (
        SELECT a.id, a.gid, an.name, an.name as description
            FROM artist a
            JOIN artist_name an ON (a.name=an.id)
            WHERE lower(an.name)=lower(%(name)s)
        UNION ALL
        SELECT a.id, a.gid, an.name, an.name ||' (alias '|| aa_n.name ||')' as description
            FROM artist a
            JOIN artist_name an ON (a.name=an.id)
            JOIN artist_alias aa ON (aa.artist=a.id)
            JOIN artist_name aa_n ON (aa.name=aa_n.id)
            WHERE lower(aa_n.name)=lower(%(name)s)
    ) subq ORDER BY id, description
        """, {'name': name})
    matches = []

    # Find the best-matching artist. Currently we only accept 1 positive-score artist, otherwise it's considered ambiguous
    for art in cur:
        score, rels, c = get_score(src, art)
        print "  %d %s: %s/artist/%s" % (score, art.description, config.MB_SITE, art.gid)
        if score <= 0:
            continue

        matches.append((art, rels, c))
        if c:
            print '    ', c.strip().replace('\n', '\n     ')

    if len(matches) == 1:
        return matches[0]
    else:
        # Too many/too few matches
        print "  SKIP, found %d positive matches for %s (%d total)" % (len(matches), name, cur.rowcount)
        return None, None, None

def prompt(question):
    answer = None
    while answer not in ['y', 'n']:
        print question,
        answer = raw_input().strip()

    return answer == 'y'

def find_credit_matches(cred, comment):
    del_rels = []
    match = split_rec.split(cred.name)
    names = match[0::2]
    joins = match[1::2]
    arts = []
    if len(set(names)) != len(names):
        #print '  SKIP, dup names'
        return None, None

    assert len(names) > 1

    for name in names:
        art, rels, c = find_best_artist(cred, name)
        if not art:
            return None, None
        arts.append(art)
        del_rels.extend(rels)
        comment += c

    if len(set(art.id for art in arts)) != len(names):
        print "  SKIP, artist has split personality disorder! (%s)" % " / ".join(art.description for art in arts)
        return None, None

    # Will call mb.edit_artist_credit with these values
    # edit_artist_credit(entity_id, credit_id, ids, names, join_phrases, edit_note)
    cred_tx = (cred.gid, cred.c_id, [a.id for a in arts], names, joins, comment.strip())

    return cred_tx, del_rels

def handle_credit(src):
    cur = db.cursor(cursor_factory=NamedTupleCursor)

    other_refs = src.ref_count-(src.r_count+src.t_count)
    print "%s (%d rec, %d tracks, %d other refs): %s/artist/%s/aliases" % (
            src.name, src.r_count, src.t_count, other_refs, config.MB_SITE, src.gid)

    del_rels = None
    comment = (u"Artist has no [other] relationships. "
               "Credit has %d recordings, %d tracks, %d other references.") % (
               src.r_count, src.t_count, other_refs)
    if CONFIRM:
        comment += " Edit confirmed by human."
    comment += "\n"

    cred_tx, del_rels = find_credit_matches(src, comment)
    if cred_tx is None:
        # Found no good matches
        return False

    # Make sure an edit wasn't already submitted.
    cur.execute("SELECT EXISTS(SELECT * FROM "+config.BOT_SCHEMA_DB+".split_artists_history"+
                " WHERE credit=%s AND changed=true)",
                [src.c_id])

    changed = cur.fetchone()[0]
    if changed:
        print "SKIP, artist has already been edited"
        return None

    if CONFIRM:
        if not prompt("Submit? [y/n]"):
            return None

    print "Editing artist credit..."
    mb.edit_artist_credit(*cred_tx)

    if del_rels:
        # Only delete artist relationships if all artist's credits were fixed
        cur.execute("""
            SELECT count(*)
            FROM artist_credit ac
                JOIN artist_credit_name acn ON (acn.artist_credit=ac.id)
                JOIN artist a ON (a.id=acn.artist)
            WHERE a.id=%s AND ac.id != %s -- Exclude credit currently being edited
              AND not exists(
                    -- Also exclude credits already edited before
                    SELECT * FROM """+config.BOT_SCHEMA_DB+""".split_artists_history sah
                    WHERE sah.credit=ac.id AND sah.changed=true)
            """, [src.a_id, src.c_id])

        count = cur.fetchone()[0]
        if count != 0:
            print "NOT deleting relationships, %d credits left" % count
        else:
            print "Deleting relationships..."
            for rel in del_rels:
                note = ("Deleting relationship, so empty collaboration artist can be removed.\n"
                        "See: %s/artist/%s/open_edits") % (config.MB_SITE, src.gid)
                mb.remove_relationship(rel.id, 'artist', 'artist', note)

    return True

split_re = ur"((?:(?:\s*[,;]\s*|\s+)(?:&|and|[Ff]eat\.?|vs\.?|[Pp]res(?:ents|\.)?|[Ss]tarring|[Mm]eets|avec|with|-|con|y|und|mit|ja|og|och|et|e|и)\s+|\s*(?:[*&+,;/・＆、とや])\s*))"
split_rec = re.compile(split_re)
query = """\
SELECT a.id as a_id, ac.id as c_id, a.gid, an.name, ac.ref_count,
    (SELECT count(*) FROM recording r WHERE r.artist_credit=ac.id) AS r_count,
    (SELECT count(*) FROM track t WHERE t.artist_credit=ac.id) AS t_count
FROM artist_credit ac
    JOIN artist_credit_name acn ON (acn.artist_credit=ac.id)
    JOIN artist_name an ON (ac.name=an.id)
    JOIN artist a ON (a.id=acn.artist)

WHERE TRUE
  AND a.edits_pending=0
  AND ac.artist_count=1
  AND (%(filter)s IS NULL OR an.name ~ %(filter)s) -- PostgreSQL will optimize out if filter is NULL
  AND an.name ~ %(re)s
  AND not exists(
      SELECT * FROM """+config.BOT_SCHEMA_DB+""".split_artists_history sah
      WHERE sah.credit=ac.id AND bot_version=%(ver)s
        AND sah.time > (now() - %(interval)s::interval))
  -- l_artist_artist is handled differently in Python code
  AND not exists(SELECT * FROM l_artist_label         WHERE entity0=a.id)
  AND not exists(SELECT * FROM l_artist_recording     WHERE entity0=a.id)
  AND not exists(SELECT * FROM l_artist_release       WHERE entity0=a.id)
  AND not exists(SELECT * FROM l_artist_release_group WHERE entity0=a.id)
  AND not exists(SELECT * FROM l_artist_url           WHERE entity0=a.id)
  AND not exists(SELECT * FROM l_artist_work          WHERE entity0=a.id)
ORDER BY ac.ref_count, r_count, t_count
"""

VERSION = 1

def bot_main(filter=None):
    init_db()

    cur = db.cursor(cursor_factory=NamedTupleCursor)
    cur2 = db.cursor()
    args = {'re': split_re, 'filter': filter, 'ver': VERSION, 'interval': REFRESH_INTERVAL}
    print cur.mogrify(query, args)
    cur.execute(query, args)

    print "TOTAL found", cur.rowcount
    if cur.rowcount == 0:
        return

    init_mb()
    for cred in cur:
        changed = handle_credit(cred)
        # None - user cancelled edit
        if changed is not None:
            cur2.execute("INSERT INTO "+config.BOT_SCHEMA_DB+".split_artists_history"+
                         " (artist, credit, changed, bot_version) VALUES (%s, %s, %s, %s)",
                        [cred.a_id, cred.c_id, changed, VERSION])
            db.commit()

def init_mb():
    global mb
    print "Logging in..."
    mb = MusicBrainzClient(config.MB_USERNAME, config.MB_PASSWORD, config.MB_SITE)

def init_db():
    global db
    db = psycopg2.connect(config.MB_DB)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, db)
    psycopg2.extensions.register_type(psycopg2.extensions.UNICODEARRAY, db)

    cur = db.cursor()
    # Don't need data durability
    cur.execute("SET synchronous_commit=off")

if __name__=='__main__':
    if len(sys.argv) > 1:
        filter = sys.argv[1].decode('utf8')
    else:
        filter = None
    bot_main(filter)
