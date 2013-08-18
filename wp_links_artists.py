#!/usr/bin/python

import sys
import re
import sqlalchemy
import solr
from simplemediawiki import MediaWiki
from editing import MusicBrainzClient
import pprint
import urllib
import time
from mbbot.wp.wikipage import WikiPage
from mbbot.wp.analysis import determine_country
from utils import mangle_name, join_names, out, colored_out, bcolors, escape_query, quote_page_title, wp_is_canonical_page
import config as cfg

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute("SET search_path TO musicbrainz, %s" % cfg.BOT_SCHEMA_DB)

wp_lang = sys.argv[1] if len(sys.argv) > 1 else 'en'

wp = MediaWiki('http://%s.wikipedia.org/w/api.php' % wp_lang)

suffix = '_' + wp_lang if wp_lang != 'en' else ''
wps = solr.SolrConnection('http://localhost:8983/solr/wikipedia'+suffix)

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

"""
CREATE TABLE bot_wp_artist_link (
    gid uuid NOT NULL,
    lang character varying(2),
    processed timestamp with time zone DEFAULT now()
    CONSTRAINT bot_wp_artist_link_pkey PRIMARY KEY (gid, lang)
);

CREATE TABLE bot_wp_artist_link_ignore (
    gid uuid NOT NULL,
    lang character varying(2),
    CONSTRAINT bot_wp_artist_link_ignore_pkey PRIMARY KEY (gid, lang)
);
"""

acceptable_countries_for_lang = {
    'fr': ['FR', 'MC']
}
#acceptable_countries_for_lang['en'] = acceptable_countries_for_lang['fr']

query_params = []
no_country_filter = (wp_lang == 'en') and ('en' not in acceptable_countries_for_lang or len(acceptable_countries_for_lang['en']) == 0)
if no_country_filter:
    # Hack to avoid having an SQL error with an empty IN clause ()
    in_country_clause = 'TRUE'
else:
    placeHolders = ','.join( ['%s'] * len(acceptable_countries_for_lang[wp_lang]) )
    in_country_clause = "%s IN (%s)" % ('iso.code', placeHolders)
    query_params.extend(acceptable_countries_for_lang[wp_lang])
query_params.extend((wp_lang, wp_lang))

query = """
WITH
    artists_wo_wikipedia AS (
        SELECT DISTINCT a.id, iso.code AS iso_code
        FROM artist a
        LEFT JOIN area ON area.id = a.area
        LEFT JOIN iso_3166_1 iso ON iso.area = area.id
        LEFT JOIN (SELECT l.entity0 AS id
            FROM l_artist_url l
            JOIN url u ON l.entity1 = u.id AND u.url LIKE 'http://"""+wp_lang+""".wikipedia.org/wiki/%%'
            WHERE l.link IN (SELECT id FROM link WHERE link_type = 179)
        ) wpl ON wpl.id = a.id
        WHERE a.id > 2 AND wpl.id IS NULL
            AND (iso.code IS NULL OR """ + in_country_clause + """)
    )
SELECT a.id, a.gid, a.name, ta.iso_code, b.processed
FROM artists_wo_wikipedia ta
JOIN s_artist a ON ta.id=a.id
LEFT JOIN bot_wp_artist_link b ON a.gid = b.gid AND b.lang = %s
LEFT JOIN bot_wp_artist_link_ignore i ON a.gid = i.gid AND i.lang = %s
WHERE i.gid IS NULL
ORDER BY b.processed NULLS FIRST, ta.iso_code NULLS LAST, a.id
LIMIT 10000
"""

query_artist_albums = """
SELECT rg.name
FROM s_release_group rg
JOIN artist_credit_name acn ON rg.artist_credit = acn.artist_credit
WHERE acn.artist = %s
UNION
SELECT r.name
FROM s_release r
JOIN artist_credit_name acn ON r.artist_credit = acn.artist_credit
WHERE acn.artist = %s
"""

query_artist_works = """
SELECT DISTINCT w.name
FROM s_work w
WHERE w.id IN (
    -- Select works that are related to recordings for this artist
    SELECT entity1 AS work
      FROM l_recording_work
      JOIN recording ON recording.id = entity0
      JOIN artist_credit_name acn
              ON acn.artist_credit = recording.artist_credit
     WHERE acn.artist = %s
    UNION
    -- Select works that this artist is related to
    SELECT entity1 AS work
      FROM l_artist_work ar
      JOIN link ON ar.link = link.id
      JOIN link_type lt ON lt.id = link.link_type
     WHERE entity0 = %s
)
"""

query_artist_urls = """
SELECT DISTINCT u.url
FROM url u
JOIN l_artist_url l ON l.entity1 = u.id
WHERE l.entity0 = %s AND
    u.url !~ 'wikipedia.org'
"""

query_related_artists = """
SELECT DISTINCT a.name
FROM s_artist a
WHERE a.id IN (
    -- Select artists that this artist is directly related to
    SELECT CASE WHEN entity1 = %s THEN entity0 ELSE entity1 END AS artist
      FROM l_artist_artist ar
      JOIN link ON ar.link = link.id
      JOIN link_type lt ON lt.id = link.link_type
     WHERE entity0 = %s OR entity1 = %s
    UNION
    -- Select artists that are involved with works for this artist (i.e. writers of works this artist performs)
    SELECT law.entity0 AS artist
      FROM artist_credit_name acn
      JOIN recording ON acn.artist_credit = recording.artist_credit
      JOIN l_recording_work lrw ON recording.id = lrw.entity0
      JOIN l_artist_work law ON lrw.entity1 = law.entity1
     WHERE acn.artist = %s
    UNION
    -- Select artists of recordings of works for this artist (i.e. performers of works this artist wrote)
    SELECT acn.artist AS artist
      FROM artist_credit_name acn
      JOIN recording ON acn.artist_credit = recording.artist_credit
      JOIN l_recording_work lrw ON recording.id = lrw.entity0
      JOIN l_artist_work law ON lrw.entity1 = law.entity1
     WHERE law.entity0 = %s
)
"""

for artist in db.execute(query, query_params):
    colored_out(bcolors.OKBLUE, 'Looking up artist "%s" http://musicbrainz.org/artist/%s' % (artist['name'], artist['gid']))
    matches = wps.query(escape_query(artist['name']), defType='dismax', qf='name', rows=50).results
    last_wp_request = time.time()
    for match in matches:
        title = match['name']
        if title.endswith('album)') or title.endswith('song)'):
            continue
        if mangle_name(re.sub(' \(.+\)$', '', title)) != mangle_name(artist['name']) and mangle_name(title) != mangle_name(artist['name']):
            continue
        delay = time.time() - last_wp_request
        if delay < 1.0:
            time.sleep(1.0 - delay)
        last_wp_request = time.time()
        wikipage = WikiPage.fetch('http://%s.wikipedia.org/wiki/%s' % (wp_lang, title))
        page_orig = wikipage.text
        if not page_orig:
            continue
        out(' * trying article "%s"' % (title,))
        page = mangle_name(page_orig)

        is_canonical, reason = wp_is_canonical_page(title, page_orig)
        if (not is_canonical):
            out(' * %s, skipping' % reason)
            continue
        if 'infoboxalbum' in page:
            out(' * album page, skipping')
            continue
        page_title = title

        reasons = []

        # Examine albums
        found_albums = []
        albums = set([r[0] for r in db.execute(query_artist_albums, (artist['id'],) * 2)])
        albums_to_ignore = set()
        for album in albums:
            if mangle_name(artist['name']) in mangle_name(album):
                albums_to_ignore.add(album)
        albums -= albums_to_ignore
        if not albums:
            continue
        for album in albums:
            mangled_album = mangle_name(album)
            if len(mangled_album) > 6 and mangled_album in page:
                found_albums.append(album)
        if (found_albums):
            reasons.append(join_names('album', found_albums))
            out(' * has albums: %s, found albums: %s' % (len(albums), len(found_albums)))

        # Examine works
        found_works = []
        page = mangle_name(page_orig)
        works = set([r[0] for r in db.execute(query_artist_works, (artist['id'],) * 2)])
        for work in works:
            mangled_work = mangle_name(work)
            if mangled_work in page:
                found_works.append(work)
        if (found_works):
            reasons.append(join_names('work', found_works))
            out(' * has works: %s, found works: %s' % (len(works), len(found_works)))

        # Examine urls
        found_urls = []
        page = mangle_name(page_orig)
        urls = set([r[0] for r in db.execute(query_artist_urls, (artist['id'],))])
        for url in urls:
            mangled_url = mangle_name(url)
            if mangled_url in page:
                found_urls.append(url)
        if (found_urls):
            reasons.append(join_names('url', found_urls))
            out(' * has urls: %s, found urls: %s' % (len(urls), len(found_urls)))

        # Examine related artists
        found_artists = []
        page = mangle_name(page_orig)
        artists = set([r[0] for r in db.execute(query_related_artists, (artist['id'],) * 5)])
        artists_to_ignore = set()
        for rel_artist in artists:
            if mangle_name(artist['name']) in mangle_name(rel_artist):
                artists_to_ignore.add(rel_artist)
        artists -= artists_to_ignore
        for rel_artist in artists:
            mangled_rel_artist = mangle_name(rel_artist)
            if mangled_rel_artist in page:
                found_artists.append(rel_artist)
        if (found_artists):
            reasons.append(join_names('related artist', found_artists))
            out(' * has related artists: %s, found related artists: %s' % (len(artists), len(found_artists)))

        # Determine if artist matches
        if not found_albums and not found_works and not found_artists and not found_urls:
            continue

        # Check if wikipedia lang is compatible with artist country
        if wp_lang != 'en' or wp_lang in acceptable_countries_for_lang:
            if wp_lang not in acceptable_countries_for_lang:
                continue
            country, country_reasons = determine_country(wikipage)
            if (country not in acceptable_countries_for_lang[wp_lang]):
                colored_out(bcolors.HEADER, ' * artist country (%s) not compatible with wiki language (%s)' % (country, wp_lang))
                continue

        url = 'http://%s.wikipedia.org/wiki/%s' % (wp_lang, quote_page_title(page_title),)
        text = 'Matched based on the name. The page mentions %s.' % (', '.join(reasons),)
        colored_out(bcolors.OKGREEN, ' * linking to %s' % (url,))
        out(' * edit note: %s' % (text,))
        time.sleep(60)
        mb.add_url("artist", artist['gid'], 179, url, text)
        break

    if artist['processed'] is None:
        db.execute("INSERT INTO bot_wp_artist_link (gid, lang) VALUES (%s, %s)", (artist['gid'], wp_lang))
    else:
        db.execute("UPDATE bot_wp_artist_link SET processed = now() WHERE (gid, lang) = (%s, %s)", (artist['gid'], wp_lang))
