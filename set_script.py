# -*- coding: utf-8 -*-
import random
import locale
from collections import defaultdict
from optparse import OptionParser
import pprint
import itertools
import operator
import urllib2

import sqlalchemy

from editing import MusicBrainzClient
import utils
from utils import out
from mbbot.utils.pidfile import PIDFile
import config as cfg
import iso15924

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute('SET search_path TO musicbrainz')

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

query_releases_with_unknown_script = '''
SELECT DISTINCT r.artist_credit, r.id, r.gid, rn.name, r.script
FROM release r
JOIN medium m ON m.release = r.id
JOIN release_name rn ON rn.id = r.name
WHERE r.script IS NULL AND r.edits_pending = 0 AND m.edits_pending = 0
ORDER BY r.artist_credit
'''

query_medium_names = '''
SELECT DISTINCT m.name
FROM release r
JOIN medium m ON m.release = r.id
WHERE r.id = %s
'''

query_track_names = '''
SELECT DISTINCT tn.name
FROM release r
JOIN medium m ON m.release = r.id
JOIN track t ON t.tracklist = m.tracklist
JOIN track_name tn ON t.name = tn.id
WHERE r.id = %s
'''

utils.parse_scripts()
script_range_to_iso_code = sorted((range, iso15924.unicode_alias_to_iso_code[script]) for script, ranges in utils.script_ranges.items() for range in ranges)
def get_scripts(text):
    d = defaultdict(int)
    for u in text:
        i = ord(u)
        for k, v in script_range_to_iso_code:
            if i >= k[0] and i <= k[1]:
                d[v] += 1
                break
        else:
            d['Zzzz'] += 1
    return dict(d)

whitelisted_iso_codes = set([
    'Latn',
    'Cyrl',
    'Grek', # Greek without Coptic
    'Hebr',
    'Arab',
    'Thai',
    'Guru', # Gurmukhi, most used in Punjabi language, ~10 releases
    'Deva', # Devanagari, used in India and Nepal, ~10 releases
    'Armn', # Armenian, ~5 releases
    'Sinh', # Sinhala, used in Sri Lanka, ~5 releases
    'Beng', # Bengali, ~2 releases
    'Geor', # Georgian, only Mkhedruli, not Asomtavruli, 1 release
    'Dsrt', # Deseret, phonemic English spelling, mid-19th century, 1 release
    'Cans', # Canadian Syllabics, used by Inuit, 1 release
])

stats = defaultdict(int)

query_scripts = '''SELECT DISTINCT id, iso_code, name FROM script'''
iso15924_to_mb = dict((iso_code, {'id': script_id, 'name': name}) for (script_id, iso_code, name) in db.execute(query_scripts))
mb_to_iso15924 = dict((v['id'], k) for k, v in iso15924_to_mb.items())

def main(verbose=False):
    r_by_ac = defaultdict(list)
    for count_all, (ac, release, gid, release_name, old_script_id) in enumerate(db.execute(query_releases_with_unknown_script)):
        track_names = u''.join(track_name for (track_name,) in db.execute(query_track_names, release))
        medium_names = u''.join(medium_name for (medium_name,) in db.execute(query_medium_names, release) if medium_name)
        if len(track_names) < 5:
            if verbose:
                out('too short http://musicbrainz.org/release/%s' % gid)
            continue
        scripts = get_scripts(track_names + medium_names + release_name)
        scripts_sorted = sorted(scripts.iteritems(), key=operator.itemgetter(1), reverse=True)
        stats[', '.join(scripts)] += 1
        if (len(scripts) == 1 or (len(scripts) == 2 and 'Zyyy' in scripts)) and float(scripts_sorted[0][1])/sum(scripts.values()) > 0.40:
            script = scripts_sorted[0][0]
            #if script == 'Latn':
            #    continue
            if script in whitelisted_iso_codes and old_script_id != iso15924_to_mb[script]['id']:
                if verbose:
                    out('http://musicbrainz.org/release/%s' % gid)
                    out('%s -> %s' % (mb_to_iso15924[old_script_id] if old_script_id else '', script))
                r_by_ac[ac].append((gid, old_script_id, script, scripts))
    if verbose:
        out(pprint.pformat(dict(stats)))
    r_grouped = r_by_ac.values()
    random.shuffle(r_grouped)
    r_flat = list(itertools.chain(*r_grouped))
    count = len(r_flat)
    if verbose:
        out('script can be set for %d out of %d releases' % (count, count_all))

    for i, (gid, old_script_id, new_script, script_stats) in enumerate(r_flat):
        if verbose:
            out('%d/%d - %.2f%%' % (i+1, count, (i+1) * 100.0 / count))
        out('http://musicbrainz.org/release/%s %s -> %s' % (gid, mb_to_iso15924[old_script_id] if old_script_id else '', new_script))
        new_script_name = iso15924_to_mb[new_script]['name']
        new_script_id = iso15924_to_mb[new_script]['id']
        text = u'I’m setting this to “%s” because it is the predominant script on the tracklist (>40%%), and no other (determined) script is on the tracklist.' % new_script_name
        if not old_script_id:
            old_script_id = ''
        try:
            mb.set_release_script(gid, old_script_id, new_script_id, text, auto=True)
        except (urllib2.HTTPError, urllib2.URLError, socket.timeout) as e:
            out(e)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-v', '--verbose', action='store_true', default=False,
            help='be more verbose')
    (options, args) = parser.parse_args()
    with PIDFile('/tmp/mbbot_set_script.pid'):
        main(options.verbose)
