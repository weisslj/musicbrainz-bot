# -*- coding: utf-8 -*-
import random
import locale
from collections import defaultdict
import pprint
import itertools
import urllib2
import sqlalchemy
from editing import MusicBrainzClient
import utils
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

query_release_artist_names = '''
SELECT DISTINCT an.name, acn.join_phrase
FROM release r
JOIN artist_credit_name acn ON r.artist_credit = acn.artist_credit
JOIN artist_name an ON an.id = acn.name
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

query_track_artist_names = '''
SELECT DISTINCT an.name, acn.join_phrase
FROM release r
JOIN medium m ON m.release = r.id
JOIN track t ON t.tracklist = m.tracklist
JOIN artist_credit_name acn ON t.artist_credit = acn.artist_credit
JOIN artist_name an ON an.id = acn.name
WHERE r.id = %s
'''

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

r_by_ac = defaultdict(list)
for count_all, (ac, release, gid, release_name, old_script_id) in enumerate(db.execute(query_releases_with_unknown_script)):
    track_names = u''.join(track_name for (track_name,) in db.execute(query_track_names, release))
    if len(track_names) <= 40:
        #utils.out('too short http://musicbrainz.org/release/%s' % gid)
        continue
    medium_names = list(set(medium_name for (medium_name,) in db.execute(query_medium_names, release) if medium_name))
    track_acs = [x for x in list(set(itertools.chain(*[[name, join_phrase] for name, join_phrase in db.execute(query_track_artist_names, release)]))) if x]
    release_acs = [x for x in list(set(itertools.chain(*[[name, join_phrase] for name, join_phrase in db.execute(query_release_artist_names, release)]))) if x]
    scripts_tracks = get_scripts(track_names)
    scripts = [s for s in scripts_tracks.keys() if s != 'Zyyy']
    stats[', '.join(scripts)] += 1
    if len(scripts) == 1 and float(scripts_tracks[scripts[0]])/sum(scripts_tracks.values()) > 0.70:
        script = scripts[0]
        scripts_rest = [s for s in get_scripts(list(itertools.chain(*([release_name] + release_acs + medium_names + track_acs)))).keys() if s != 'Zyyy']
        # allow Latin script on non-tracklist names for non-Latin releases
        latin_rest = False
        if script != 'Latn':
            latin_rest = 'Latn' in scripts_rest
            scripts_rest = [s for s in scripts_rest if s != 'Latn']
        if script in whitelisted_iso_codes and [script] == scripts_rest and old_script_id != iso15924_to_mb[script]['id']:
            #utils.out('http://musicbrainz.org/release/%s' % gid)
            #utils.out('%s -> %s' % (mb_to_iso15924[old_script_id] if old_script_id else '', script))
            r_by_ac[ac].append((gid, old_script_id, script, scripts_tracks, latin_rest))

pprint.pprint(dict(stats))
r_grouped = r_by_ac.values()
random.shuffle(r_grouped)
r_flat = list(itertools.chain(*r_grouped))
count = len(r_flat)
utils.out('script can be set for %d out of %d releases' % (count, count_all))

for i, (gid, old_script_id, new_script, script_stats, latin_rest) in enumerate(r_flat):
    utils.out('%d/%d - %.2f%%' % (i+1, count, (i+1) * 100.0 / count))
    utils.out('http://musicbrainz.org/release/%s' % gid)
    new_script_name = iso15924_to_mb[new_script]['name']
    new_script_id = iso15924_to_mb[new_script]['id']
    text = u'I’m setting this to “%s” because it is the predominant script on the tracklist (>80%% and >40 characters), and no other (determined) script is on the tracklist. ' % new_script_name
    if not latin_rest:
        text += u'All other names on the release are also in %s. ' % new_script_name
    else:
        text += u'All other names on the release are in %s or in Latin. ' % new_script_name
    if not old_script_id:
        old_script_id = ''
    for j in range(5):
        try:
            mb.set_release_script(gid, old_script_id, new_script_id, text, auto=True)
        except urllib2.HTTPError, e:
            if e.code == 503:
                utils.out(e)
                continue
        break
