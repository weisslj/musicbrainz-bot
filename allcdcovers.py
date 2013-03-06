#!/usr/bin/env python2

import sys
import os
import re
from hashlib import sha1
import urllib
import mechanize

from editing import MusicBrainzClient
import utils
import config as cfg

try:
    from PIL import Image

    try:
        import zbar
    except ImportError:
        zbar = None
        print "Warning: Cannot import zbar. Install python-zbar for barcode scanning"
except ImportError:
    Image = None
    print "Warning: Cannot import PIL. Install python-imaging for image dimension information"

try:
    import psycopg2
    from psycopg2.extras import NamedTupleCursor
except ImportError:
    psycopg2 = None

ACC_CACHE = 'acc-cache'

utils.monkeypatch_mechanize()

def re_find1(regexp, string):
    m = re.findall(regexp, string)
    if len(m) != 1:
        pat = getattr(regexp, 'pattern', regexp)
        if len(string) > 200:
            filename = '/tmp/debug.html'
            with open(filename, 'wb') as f:
                f.write(string)
            raise AssertionError("Expression %s matched %d times, see %s" % (pat, len(m), filename))
        else:
            raise AssertionError("Expression %s matched %d times: %r" % (pat, len(m), string))
    return m[0]

# Progress file - prevent duplicate uplpoads
DBFILE = os.path.join(ACC_CACHE, 'progress.db')
try:
    statefile = open(DBFILE, 'r+')
    state = set(x.strip() for x in statefile.readlines())
except IOError: # Not found? Try writing
    statefile = open(DBFILE, 'w')
    state = set()

def done(line):
    assert line not in state
    statefile.write("%s\n" % line)
    statefile.flush()
    state.add(line)

#### DOWNLOADING

acc_url_rec = re.compile('/show/([0-9]+)/[^/-]+/([a-z0-9_]+)')
# <div class="thumbnail"><a href="/show/63158/acid_drinkers_vile_vicious_vision_1994_retail_cd/back"><img alt="Back" [...]
#acc_show_re = '"(/show/%s/[^/-]+/(front|back|inside|inlay|cd))"'
acc_show_re = '"(/show/%s/[^/-]+/([a-z0-9_]+))"'
# <a href="/download/97e2d4d994aa7ca42da524ca333ff8d9/263803/8c4a7a3a4515ad214846617c90262367/51326581/acid_drinkers_vile_vicious_vision_1997_retail_cd-front">
acc_download_re = '"(/download/[0-9a-f]{32}/%s/[0-9a-f]{32}/[0-9a-f]+/([^/-]+-([a-z0-9_]+)))"'
# Content-Disposition: inline; filename=allcdcovers.jpg
disposition_re = '(?:; ?|^)filename=((?:[^/]+).jpg)'

ERR_SHA1 = '5dd9c1734067f7a6ee8791961130b52f804211ce'
def download_cover(release_id, typ, resp=None, data=None):
    href, fragment, dtyp = re_find1(acc_download_re % re.escape(release_id), data)

    assert typ == dtyp, "%s != %s" % (typ, dtyp)

    filename = os.path.join(ACC_CACHE, "[AllCDCovers]_%s.jpg" % fragment)
    referrer = resp.geturl()

    cov = {
       'referrer': referrer,
       'type': typ,
       'file': filename,
       'title': br.title(),
    }
    if os.path.exists(filename):
        print "SKIP download, already done: %r" % filename
        cov['cached'] = True
        return cov

    resp = br.open_novisit(href)
    disp = resp.info().getheader('Content-Disposition')
    tmp_name = re_find1(disposition_re, disp)
    if tmp_name == 'allcdcovers.jpg':
        resp.close()
        raise Exception("Got response filename %r, URL is stale? %r" % (tmp_name, href))
    #filename = os.path.join(ACC_CACHE, tmp_name)
    print "Downloading to %r" % (filename)

    data = resp.read()
    resp.close()
    if sha1(data).hexdigest() == ERR_SHA1:
        raise Exception("Got error image back! URL is stale? %r" % href)

    with open(filename, 'wb') as f:
        f.write(data)

    return cov

def fetch_covers(base_url):
    release_id, typ = re_find1(acc_url_rec, base_url)

    resp = br.open(base_url)
    data = resp.read()
    print "Title: %s" % br.title()

    pages = list(set(re.findall(acc_show_re % re.escape(release_id), data)))
    covers = []

    cov = download_cover(release_id, typ, resp, data)
    covers.append(cov)

    for href, typ in pages:
        if href in base_url:
            continue

        resp = br.open(href)
        data = resp.read()
        cov = download_cover(release_id, typ, resp, data)
        covers.append(cov)

    return covers

#### IMAGE PROCESSING

def pretty_size(size):
    # http://www.dzone.com/snippets/filesize-nice-units
    suffixes = [('',2**10), ('k',2**20), ('M',2**30), ('G',2**40), ('T',2**50)]
    for suf, lim in suffixes:
        if size > lim:
            continue
        else:
            return "%s %sB" % (round(size/float(lim/2**10),1), suf)

if zbar:
    symtypes = (zbar.Symbol.EAN13,  zbar.Symbol.EAN8, zbar.Symbol.ISBN10,
                zbar.Symbol.ISBN13, zbar.Symbol.UPCA, zbar.Symbol.UPCE)
else:
    symtypes = ()

def scan_barcode(img):
    gray = img.convert('L')
    w, h = gray.size

    scanner = zbar.ImageScanner()
    for type in symtypes:
        scanner.set_config(type, zbar.Config.ENABLE, 1)
    zimg = zbar.Image(w, h, 'Y800', gray.tostring())
    scanner.scan(zimg)

    codes = []
    for sym in zimg:
        codes.append((sym.type, sym.data))

    return codes

def annotate_image(filename):
    """Returns image information as dict"""
    data = {}
    data['size_bytes'] = bytesize = os.stat(filename).st_size
    data['size_pretty'] = pretty_size(bytesize)

    if Image:
        img = Image.open(filename)
        try:
            if zbar:
                data['barcode'] = barcode = scan_barcode(img)
                if barcode:
                    print ", ".join("%s: %s" % bc for bc in data['barcode'])
                    print "Barcode: %s (%r)" % (", ".join("%s: %s" % bc for bc in data['barcode']), filename)

            else:
                data['barcode'] = None
                # Verify image - makes sure we don't upload corrupt junk
                img.tostring()

        except IOError as err:
            print "Error in image %r: %s" % (filename, err)
            sys.exit(1)
        data['dims'] = "%dx%d" % img.size
    else:
        data['dims'] = None
        data['barcode'] = None

    return data

#### UPLOADING

ordering = {
    'front': 0,
    'back': 1,
    'inside': 2,
    'inlay': 3,
    'cd': 4,
}
def cov_order(cov):
    typ = cov['type']
    return ordering[typ.split('_',1)[0]], typ

COMMENT = "AllCDCovers"
def upload_covers(covers, mbid):
    for cov in sorted(covers, key=cov_order):
        upload_id = "%s %s" % (mbid, cov['referrer'])
        if upload_id in state:
            print "SKIP upload, already done: %r" % cov['file']
            continue

        typ = cov['type']
        # type can be: front, back, inside, inlay, cd, cd_2
        if typ in ['front', 'back']:
            types = [typ]
        elif typ.startswith('cd'):
            types = ['medium']
        elif typ == 'inside':
            types = ['booklet']
        elif typ == 'inlay':
            types = ['tray']
        else: # ???
            types = []

        note = "\"%(title)s\"\nType: %(type)s / Size: %(size_pretty)s (%(size_bytes)s bytes)\n" % (cov)
        if cov['dims']:
            note += "Dimensions: " + cov['dims']
            if cov['barcode']:
                note += " / Barcode: " + ", ".join("%s: %s" % bc for bc in cov['barcode'])

        note += "\n" + cov['referrer']

        print "Uploading %r (%s) from %r" % (types, cov['size_pretty'], cov['file'])
        # Doesn't work: position = '0' if cov['type'] == 'front' else None
        # ValueError: control 'add-cover-art.position' is readonly
        mb.add_cover_art(mbid, cov['file'], types, None, COMMENT, note, False, False)

        done(upload_id)

#### BARCODE MATCHING

def find_mbid_by_barcode(barcodes):
    if psycopg2 is None:
        print "Warning: psycopg2 could not be imported, skipping barcode lookup"
        return

    try:
        db = psycopg2.connect(cfg.MB_DB)
        cur = db.cursor(cursor_factory=NamedTupleCursor)
    except psycopg2.Error as err:
        print "Warning: Cannot look up barcode: %s" % err
        return

    lookup = []

    for typ, code in barcodes:
        lookup.append(code)
        if typ == zbar.Symbol.UPCA:
            # Equivalent codes, UPCA => EAN13
            lookup.append('0' + code)

    cur.execute("""\
        SELECT r.gid, r.name, r.barcode,
               (SELECT count(*) FROM cover_art ca WHERE ca.release=r.id) as art_count
        FROM s_release r
        WHERE r.barcode = any(%s)
        """, [lookup])

    if cur.rowcount == 0:
        print "No matches in MusicBrainz"
    else:
        print # Empty line
        print "Found:"

    res = cur.fetchall()
    for r in res:
        print "\"%s\" matching %s (%d images): %s/release/%s" % (r.name, r.barcode, r.art_count, cfg.MB_SITE, r.gid)

    if len(res) == 1 and res[0].art_count == 0:
        print "Found 1 good match, auto-uploading..."
        return res[0].gid

    if len(res) > 1:
        query = urllib.quote_plus(' OR '.join(lookup))
        print "Please go here: %s/search?type=release&query=%s" % (cfg.MB_SITE, query)

def handle_acc_covers(acc_url, mbids):
    print "Downloading from", acc_url
    covers = fetch_covers(acc_url)

    barcodes = set()

    for cov in covers:
        data = annotate_image(cov['file'])
        cov.update(data)
        if data['barcode']:
            barcodes.update(bc for bc in data['barcode'] if bc[0] in symtypes)

    # If no MB release was provided and we found some barcodes, try matching
    # it up against MusicBrainz barcodes.
    if not mbids and barcodes:
        mbid = find_mbid_by_barcode(barcodes)
        if mbid:
            mbids = [mbid]

    if mbids:
        init_mb()

    for mbid in mbids:
        mburl = '%s/release/%s/cover-art' % (cfg.MB_SITE, mbid)
        print "Uploading to", mburl
        upload_covers(covers, mbid)
        print "Done!", mburl

def print_help():
    print "Usage: %s allcdcovers_url [mbid ...]" % sys.argv[0]
    print "MBIDs can be given as musicbrainz.org URLs, will be automatically parsed."
    print "Example: %s http://www.allcdcovers.com/show/160217/boards_of_canada_twoism_2002_retail_cd/front https://musicbrainz.org/release/a95dbc6e-3066-46ea-91ed-cfb9539f0c7c" % sys.argv[0]

uuid_rec = re.compile('[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}')
def bot_main():
    if len(sys.argv) <= 1 or '--help' in sys.argv or '-h' in sys.argv:
        sys.exit(1)

    acc_url = None
    mbids = []
    for arg in sys.argv[1:]:
        if uuid_rec.findall(arg):
            mbids.append(re_find1(uuid_rec, arg))

        elif acc_url_rec.findall(arg):
            if acc_url is not None:
                print "Specify only one allcdcovers.com URL"
                sys.exit(1)
            acc_url = arg

        else:
            print "Unrecognized argument:", arg
            print
            print_help()
            sys.exit(1)

    init_br()
    handle_acc_covers(acc_url, mbids)

def init_br():
    global br

    br = mechanize.Browser()
    br.set_handle_robots(False) # no robots
    br.set_handle_refresh(False) # can sometimes hang without this
    br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]

def init_mb():
    global mb

    print "Logging in..."
    mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

if __name__ == '__main__':
    bot_main()
