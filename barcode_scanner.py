#!/usr/bin/env python

import sys
import os
import urllib2
from cStringIO import StringIO

import psycopg2
from psycopg2.extras import NamedTupleCursor

from editing import MusicBrainzClient
import config as cfg

try:
    from PIL import Image
except ImportError:
    print "Cannot import PIL. Install python-imaging"
    raise

try:
    import zbar
except ImportError:
    print "Cannot import zbar. Install python-zbar for barcode scanning"
    raise

CAA_SITE = 'https://coverartarchive.org/beta'
CAA_CACHE = 'caa-cache'

if not os.path.exists(CAA_CACHE):
    os.mkdir(CAA_CACHE)

opener = urllib2.build_opener()
opener.addheaders = [('User-Agent', cfg.WWW_USER_AGENT or 'musicbrainz-bot barcode_scanner')]

# Progress file - prevent duplicate edits
DBFILE = os.path.join(CAA_CACHE, 'barcode_scanner.db')
try:
    statefile = open(DBFILE, 'r+')
    state = set(x.split('#', 1)[0].strip() for x in statefile.readlines())
except IOError: # Not found? Try writing
    statefile = open(DBFILE, 'w')
    state = set()

def done(line):
    assert line not in state
    statefile.write("%s\n" % line)
    statefile.flush()
    state.add(line.split('#', 1)[0].strip())

def pretty_size(size):
    # http://www.dzone.com/snippets/filesize-nice-units
    suffixes = [('',2**10), ('k',2**20), ('M',2**30), ('G',2**40), ('T',2**50)]
    for suf, lim in suffixes:
        if size > lim:
            continue
        else:
            return "%s %sB" % (round(size/float(lim/2**10),1), suf)

symtypes = (zbar.Symbol.EAN13,  zbar.Symbol.EAN8, zbar.Symbol.ISBN10,
            zbar.Symbol.ISBN13, zbar.Symbol.UPCA, zbar.Symbol.UPCE,
            zbar.Symbol.CODE39)

def scan_barcode(img):
    gray = img.convert('L')
    w, h = gray.size

    scanner = zbar.ImageScanner()
    for type in symtypes:
        scanner.set_config(type, zbar.Config.ENABLE, 1)
    zimg = zbar.Image(w, h, 'Y800', gray.tostring())
    scanner.scan(zimg)
    return zimg.symbols

def fetch_image(release, art_id):
    url = '%s/release/%s/%d.jpg' % (CAA_SITE, release.gid, art_id)
    filename = os.path.join(CAA_CACHE, '%d.jpg') % art_id

    if os.path.exists(filename):
        f = open(filename, 'rb')
        print "SKIP fetching %s" % url
    else:
        resp = opener.open(url)
        info = resp.info()
        ctype = info.getheader('Content-Type')
        size = int(info.getheader('Content-Length'))
        assert ctype.startswith('image/')

        print "Downloading %s (%s)" % (url, pretty_size(size))

        try:
            f = open(filename, 'wb+')
            f.write(resp.read())
            f.flush()
        except BaseException as e:
            # If writing failed, try to remove the file
            try:
                os.remove(filename)
            except:
                pass
            raise e
        f.seek(0)

    return f, url

def get_annotation(rel_id):
    cur = db.cursor()
    cur.execute("""
        SELECT a.text
        FROM annotation a
        JOIN release_annotation ra on (ra.annotation=a.id)
        JOIN release r on (ra.release=r.id)
        WHERE r.id=%s
        ORDER BY created DESC LIMIT 1
        """, [rel_id])

    ann = cur.fetchone()
    if ann:
        return ann[0]
    return None

def handle_release(release):
    # May have multiple cover images with the same barcode
    codes = set()
    note = ""
    txn_ids = []
    misc_codes = set()

    for art_id, art_type in zip(release.ids, release.types):
        txn_id = "%s %s" % (release.gid, art_id)
        if txn_id in state:
            print "SKIP %s" % txn_id
            continue

        f, url = fetch_image(release, art_id)
        try:
            img = Image.open(f)
            symbols = scan_barcode(img)
        except IOError as err:
            print "Error opening URL %s %s" % (url, err)
            txn_ids.append("%s # Error: %s" % (txn_id, err))
            continue

        if not symbols:
            txn_ids.append("%s # No barcode" % txn_id)
        for sym in symbols:
            txn_my = txn_id + " # %s: %s (confidence %d)" % (sym.type, sym.data, sym.quality)
            print txn_my
            txn_ids.append(txn_my)
            if sym.type in symtypes:
                # Can't enter this code on the "barcode" field

                if sym.type == zbar.Symbol.CODE39:
                    misc_codes.add(('Code 39 barcode:', sym.data))
                else:
                    codes.add(sym.data)
                note += ("Recognized %s: %s from %s cover image %s (confidence %d)\n" %
                         (sym.type, sym.data, art_type_map[art_type], url, sym.quality))

    if not txn_ids:
        # Nothing to do
        return

    if misc_codes:
        old_annotation = get_annotation(release.id) or ""
        changed = False
        annotation = old_annotation + "\r\n"
        for typ, code in misc_codes:
            if code in annotation:
                print "SKIP, code %s already written on annotation" % code
            else:
                annotation += "\r\n%s %s" % (typ, code)
                changed = True

        annotation = annotation.strip()

        if changed:
            ok = mb._edit_release_information(release.id, {"annotation": (old_annotation, annotation)}, note, auto=False)
            if not ok:
                return
            print "Annotation edited"

    if not codes:
        if not misc_codes:
            print "No barcode"
        for txn_id in txn_ids:
            done(txn_id)

    elif len(codes) > 1:
        print "Too many barcodes: %d" % len(codes)
        for txn_id in txn_ids:
            done(txn_id + " (TOOMANY)")

    else:
        code = codes.pop()
        ok = mb._edit_release_information(release.id, {"barcode": ('', code)}, note, auto=False)
        if not ok:
            return

        # If edit went well, "commit" these txn_ids
        for txn_id in txn_ids:
            done(txn_id)

def bot_main():
    print "Initializing..."
    init_db()
    cur = db.cursor(cursor_factory=NamedTupleCursor)

    skip_ids = [line.split(' ',2)[1] for line in state if ' ' in line]
    # Format as PostgreSQL array literal
    skip_ids = '{%s}' % ','.join(skip_ids)

    cur.execute("""
        SELECT r.id, r.gid, array_agg(ca.id) as ids, array_agg(cat.type_id) as types
        FROM release r
        JOIN cover_art ca ON (ca.release=r.id)
        JOIN cover_art_type cat on (cat.id=ca.id)
        WHERE r.barcode is null AND cat.type_id IN (2,5) /*Back,Obi*/
          AND (r.packaging is null OR r.packaging != 7 /*None*/)
          AND exists (SELECT * FROM medium m
                      WHERE m.release=r.id
                        AND (m.format is null OR m.format NOT IN (12,26,27) /*Digital Media etc*/))
          AND ca.edits_pending = 0
          AND ca.id != all(%s)
        GROUP BY r.id
        """, [skip_ids])

    if not cur.rowcount:
        print "No new images to scan"
        return

    init_mb()

    for release in cur:
        handle_release(release)

def init_db():
    global db, art_type_map

    db = psycopg2.connect(cfg.MB_DB)
    cur = db.cursor()
    cur.execute("SELECT id, name FROM art_type")
    art_type_map = dict(cur.fetchall())

def init_mb():
    global mb
    mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

if __name__ == '__main__':
    bot_main()

