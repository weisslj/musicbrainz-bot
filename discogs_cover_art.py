#!/usr/bin/python

import sys
import os
import re
import urllib2
import sqlalchemy
from PIL import Image
from cStringIO import StringIO
import time
from editing import MusicBrainzClient
import socket
from utils import out, colored_out, bcolors, monkeypatch_mechanize
import config as cfg
try:
    import config_caa as cfg_caa
except ImportError:
    cfg_caa = cfg

try:
    import discogs_client as discogs
except ImportError as err:
    colored_out(bcolors.FAIL,
                "Error: Cannot use Discogs: %s\n" % err +
                "Run 'pip install discogs-client' or get discogs_client.py from\n"
                "https://github.com/discogs/discogs_client")
    sys.exit(1)

# Optional modules
try:
    import amazonproduct
    from amazonproduct.contrib.retry import RetryAPI
except ImportError as err:
    colored_out(bcolors.WARNING, "Warning: Cannot use Amazon: %s" % err)
    amazonproduct = None

try:
    from mbbot.source.spotify import SpotifyWebService
    spotify = SpotifyWebService()
except ImportError as err:
    colored_out(bcolors.WARNING, "Warning: Cannot use Spotify: %s" % err)
    spotify = None

try:
    from mbbot.source.itunes import ItunesSearchAPI
    itunes = ItunesSearchAPI()
except ImportError as err:
    colored_out(bcolors.WARNING, "Warning: Cannot use iTunes: %s" % err)
    itunes = None

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute("SET search_path TO musicbrainz, %s, public" % cfg.BOT_SCHEMA_DB)

monkeypatch_mechanize()
mb = MusicBrainzClient(cfg_caa.MB_USERNAME, cfg_caa.MB_PASSWORD, cfg_caa.MB_SITE)

discogs.user_agent = 'MusicBrainzBot/0.1 +https://github.com/murdos/musicbrainz-bot'


socket.setdefaulttimeout(300)

"""
CREATE TABLE bot_discogs_amz_cover_art (
    gid uuid NOT NULL,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_discogs_amz_cover_art_pkey PRIMARY KEY (gid)
);

CREATE TABLE bot_release_artwork_url (
    release uuid NOT NULL,
    url character varying,
    processed timestamp with time zone DEFAULT now(),
    CONSTRAINT bot_release_artwork_url_pkey PRIMARY KEY (release, url)
);
"""

mbid = sys.argv[1] if len(sys.argv) > 1 else None
if mbid:
    filter_clause = "r.gid = '%s'" % mbid
else:
    filter_clause = "rm.cover_art_presence != 'present'::cover_art_presence"

query = """
WITH
    releases_wo_coverart AS (
        SELECT r.id, discogs_url.url as discogs_url, amz_url.url AS amz_url
        FROM release r
            JOIN release_meta rm ON rm.id = r.id
            JOIN release_group rg ON r.release_group = rg.id
            LEFT JOIN release_group_primary_type rg_type ON rg.type = rg_type.id
            LEFT JOIN release_country rc ON rc.release = r.id
            LEFT JOIN area ON area.id = rc.country
            LEFT JOIN iso_3166_1 iso ON iso.area = area.id
            LEFT JOIN l_release_url discogs_link ON discogs_link.entity0 = r.id AND discogs_link.link IN (SELECT id FROM link WHERE link_type = 76)
                AND discogs_link.edits_pending = 0
            LEFT JOIN url discogs_url ON discogs_url.id = discogs_link.entity1
            LEFT JOIN l_release_url amz_link ON amz_link.entity0 = r.id AND amz_link.link IN (SELECT id FROM link WHERE link_type = 77)
                AND amz_link.edits_pending = 0
            LEFT JOIN url amz_url ON amz_url.id = amz_link.entity1
            LEFT JOIN release_status rs ON r.status = rs.id
            LEFT JOIN (SELECT encycl_link.entity0, encycl_link.entity1, encycl_url.url
                FROM l_release_url encycl_link
                JOIN url encycl_url ON encycl_url.id = encycl_link.entity1 AND encycl_url.url ~ 'encyclopedisque.fr/images/'
                WHERE encycl_link.link IN (SELECT id FROM link WHERE link_type = 78) AND encycl_link.edits_pending = 0
            ) encycl_link ON encycl_link.entity0 = r.id
        WHERE
            /* This release should not have another Discogs link attached */
            NOT EXISTS (SELECT 1 FROM l_release_url l WHERE l.entity0 = r.id AND l.entity1 <> discogs_url.id
                                AND l.link IN (SELECT id FROM link WHERE link_type = 76))
            /* Real filter: specified MBID or release with no cover art */
            AND """ + filter_clause + """
            /* Optional checks unless MBID is specified */
            AND (""" + ("TRUE OR " if mbid else "") + """(
                /* Artist is either French or Various Artists. Pick only French Various Artists releases */
                EXISTS (SELECT 1
                    FROM artist_credit_name acn
                    JOIN artist a ON acn.artist = a.id
                    LEFT JOIN iso_3166_1 iso_artist ON iso_artist.area = a.area
                    WHERE r.artist_credit = acn.artist_credit
                        /* (FR release & VA) OR FR artist */
                        AND ((iso.code = 'FR' AND a.id = 1) OR iso_artist.code = 'FR')
                )
                /* Discogs link should only be linked to this release */
                AND NOT EXISTS (SELECT 1 FROM l_release_url l WHERE l.entity1 = discogs_url.id AND l.entity0 <> r.id)
                /* Amazon link should only be linked to this release */
                AND NOT EXISTS (SELECT 1 FROM l_release_url l WHERE l.entity1 = amz_url.id AND l.entity0 <> r.id)
                /* this release should not have another Amazon link attached */
                AND NOT EXISTS (SELECT 1 FROM l_release_url l WHERE l.entity0 = r.id AND l.entity1 <> amz_url.id
                                    AND l.link IN (SELECT id FROM link WHERE link_type = 77))
                /* Encylopedisque link should only be linked to this release */
                AND NOT EXISTS (SELECT 1 FROM l_release_url l WHERE l.entity1 = encycl_link.entity1 AND l.entity0 <> r.id)
                /* Discogs URL required */
                AND discogs_url.url IS NOT NULL
                /* Various filter to limit the scope */
                AND (
                    /* promotion and bootleg */
                    rs.name IN ('Promotion','Bootleg')
                    /* non digital singles */
                    OR (EXISTS (SELECT 1 FROM medium m JOIN medium_format mf ON m.format = mf.id WHERE m.release = r.id AND mf.name <> 'Digital Media') AND rg_type.name in ('Single'))
                    /* release before 1996 */
                    OR date_year < 1996
                    /* release without barcode */
                    OR r.barcode = ''
                    /* if barcode exists, either we have ASIN or release has not been updated since a few days (to be sure asin_links scripts has run on it) */
                    OR (r.barcode IS NOT NULL AND (amz_url.url IS NOT NULL OR r.last_updated < now() - INTERVAL '5 DAY'))
                    /* release with an encyclopedique link */
                    OR encycl_link.url IS NOT NULL
                )
            ))
    )
SELECT r.id, r.gid, r.name, tr.discogs_url, tr.amz_url, ac.name AS artist, r.barcode, b.processed
FROM releases_wo_coverart tr
JOIN release r ON tr.id = r.id
JOIN artist_credit ac ON r.artist_credit=ac.id
LEFT JOIN bot_discogs_amz_cover_art b ON r.gid = b.gid
ORDER BY b.processed NULLS FIRST, r.artist_credit, r.name
LIMIT 100
"""

def amz_get_info(url):   
    params = { 'ResponseGroup' : 'Images' }
    
    m = re.match(r'^https?://(?:www.)?amazon\.(.*?)(?:\:[0-9]+)?/.*/([0-9B][0-9A-Z]{9})(?:[^0-9A-Z]|$)', url)
    if m is None:
        return (None, None)
        
    locale = m.group(1).replace('co.', '').replace('com', 'us')
    asin = m.group(2)   
    amazon_api = RetryAPI(cfg.AWS_KEY, cfg.AWS_SECRET_KEY, locale, cfg.AWS_ASSOCIATE_TAG)
    
    try:
        root = amazon_api.item_lookup(asin, **params)
    except amazonproduct.errors.InvalidParameterValue, e:
        return (None, None)
    except amazonproduct.errors.AWSError, e:
        return (None, None)
    item = root.Items.Item
    if not 'LargeImage' in item.__dict__:
        return (None, None)
    barcode = None
    if 'EAN' in item.__dict__:
        barcode = item.EAN
    elif 'UPC' in item.__dict__:
        barcode = item.UPC
    return (item.LargeImage, barcode)

def discogs_get_primary_image(url):
    if url is None:
        return None
    m = re.match(r'http://www.discogs.com/release/([0-9]+)', url)
    if m:
        release_id = int(m.group(1))
        release = discogs.Release(release_id)
        if 'images' in release.data and len(release.data['images']) >= 1:
            for image in release.data['images']:
                if image['type'] == 'primary':
                    return image
            # No primary image found => return first images
            return release.data['images'][0]
    return None
    
def discogs_get_secondary_images(url):
    if url is None:
        return []
    images = []
    m = re.match(r'http://www.discogs.com/release/([0-9]+)', url)
    if m:
        release_id = int(m.group(1))
        release = discogs.Release(release_id)
        if 'images' in release.data and len(release.data['images']) >= 2:
            found_primary = False
            for image in release.data['images']:
                if image['type'] == 'secondary':
                    images.append(image)
                elif image['type'] == 'primary':
                    found_primary = True
            # if all images are secondary, it means first one as already been considered as the primary one,
            # so it should be excluded
            if not found_primary:
                images = images[1:]
    return images

def save_processed(release, url):
    db.execute("INSERT INTO bot_release_artwork_url (release, url) VALUES (%s, %s)", (release, url))

def already_processed(release, url):
    res = db.execute("SELECT 1 FROM bot_release_artwork_url WHERE release = %s AND url = %s", (release, url))
    return res.scalar() is not None

def submit_cover_art(release, url, types):
    if already_processed(release, url):
        colored_out(bcolors.NONE, " * skipping already submitted image '%s'" % (url,))
    else:
        colored_out(bcolors.OKGREEN, " * Adding " + ",".join(types) + (" " if len(types)>0 else "") + "cover art '%s'" % (url,))
        img_file = urllib2.urlopen(url)
        im = Image.open(StringIO(img_file.read()))
        edit_note = "'''Dimension''': %sx%s\n'''Source''': %s" % (im.size[0], im.size[1], url)
        time.sleep(5)
        mb.add_cover_art(release, url, types, None, u'', edit_note, False)
        save_processed(release, url)

for release in db.execute(query):
    colored_out(bcolors.OKBLUE, 'Examining release "%s" by "%s" http://musicbrainz.org/release/%s' % (release['name'], release['artist'], release['gid']))

    # Front cover
    # Start with Discogs if available
    colored_out(bcolors.HEADER, ' * Discogs = %s' % (release['discogs_url'],))
    discogs_image = discogs_get_primary_image(release['discogs_url'])
    if discogs_image is None:
       best_score = 0
       front_uri = None
    else:
        best_score = discogs_image['height'] * discogs_image['width']
        front_uri = discogs_image['uri']
        colored_out(bcolors.NONE, ' * Discogs score:\t%s \t %s' % (best_score, front_uri))

    # Evaluate Amazon
    if amazonproduct is not None and release['amz_url'] is not None:
        colored_out(bcolors.HEADER, ' * Amazon = %s' % (release['amz_url'],))
        amz_image, amz_barcode = amz_get_info(release['amz_url'])
        # Amazon: check barcode matches
        if amz_barcode is not None and release['barcode'] is not None \
            and re.sub(r'^(0+)', '', amz_barcode) != re.sub(r'^(0+)', '', release['barcode']):
            colored_out(bcolors.FAIL, " * Amz barcode doesn't match MB barcode (%s vs %s) => skipping" % (amz_barcode, release['barcode']))
            continue
        if amz_image is not None:
            amz_score = amz_image.Height * amz_image.Width
            colored_out(bcolors.NONE, ' * Amazon score:\t%s \t %s' % (amz_score, amz_image.URL.pyval))
            if amz_score > best_score:
                front_uri = amz_image.URL.pyval
                best_score = amz_score

    # Evaluate Spotify
    if spotify is not None and release['barcode'] is not None and release['barcode'] != "":
        albums = spotify.search_albums('upc:%s' % release['barcode'])
        if len(albums) == 1:
            colored_out(bcolors.WARNING, ' * Spotify = https://embed.spotify.com/?uri=%s&view=coverart' % (albums[0]['href'],))
            image_url = spotify.artwork_url(albums[0]['href'])
            if image_url is not None:
                try:
                    img_file = urllib2.urlopen(image_url)
                    im = Image.open(StringIO(img_file.read()))
                    spotify_score = im.size[0] * im.size[1]
                    colored_out(bcolors.NONE, ' * Spotify score:\t%s \t %s' % (spotify_score, image_url))
                    if spotify_score > best_score:
                        front_uri = image_url
                        best_score = spotify_score
                except urllib2.HTTPError, e: pass

    # Evaluate iTunes
    if itunes is not None and release['barcode'] is not None and release['barcode'] != "":
        albums = itunes.search({'upc': release['barcode']})
        if len(albums) == 1:
            colored_out(bcolors.WARNING, ' * ITunes = %s' % (albums[0]['collectionViewUrl'],))
            image_url = albums[0]['artworkUrl100'].replace('100x100', '600x600')
            img_file = urllib2.urlopen(image_url)
            im = Image.open(StringIO(img_file.read()))
            itunes_score = im.size[0] * im.size[1]
            colored_out(bcolors.NONE, ' * iTunes score:\t%s \t %s' % (itunes_score, image_url))
            if itunes_score > best_score:
                front_uri = image_url
                best_score = itunes_score

    if front_uri is not None:
        submit_cover_art(release['gid'], front_uri, ['front'])

    # Other images
    for image in discogs_get_secondary_images(release['discogs_url']):
        submit_cover_art(release['gid'], image['uri'], [])

    out()

    if release['processed'] is None:
        db.execute("INSERT INTO bot_discogs_amz_cover_art (gid) VALUES (%s)", (release['gid'],))
    else:
        db.execute("UPDATE bot_discogs_amz_cover_art SET processed = now() WHERE gid = %s", (release['gid'],))


