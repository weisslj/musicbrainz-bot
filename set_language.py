#!/usr/bin/env python
# -*- coding: utf-8 -*-
import re
import random
from collections import defaultdict
from optparse import OptionParser
import itertools
import pprint
import urllib2
import socket
import sqlalchemy
from utils import out, program_string
from mbbot.utils.pidfile import PIDFile
from editing import MusicBrainzClient
import config as cfg

MIN_WORDS = 5
MIN_WORD_LEN = 4

engine = sqlalchemy.create_engine(cfg.MB_DB)
db = engine.connect()
db.execute('SET search_path TO musicbrainz, %s' % cfg.BOT_SCHEMA_DB)

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

query_releases_with_unknown_language = '''
SELECT DISTINCT r.artist_credit, r.id, r.gid, rn.name, r.language
FROM release r
JOIN release_name rn ON rn.id = r.name
JOIN medium m ON m.release = r.id
WHERE r.language IS NULL AND r.edits_pending = 0 AND m.edits_pending = 0
ORDER BY r.artist_credit
'''

query_release_languages_from_artist = '''
SELECT l.iso_code_1
FROM release r
JOIN language l ON l.id = r.language
WHERE r.artist_credit = %s
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

dict_paths = {
#    'en': ['/usr/share/dict/american-english'],
    'en': ['/usr/share/dict/american-english-insane'],
    'de': ['/usr/share/dict/ngerman'],
    'es': ['/usr/share/dict/spanish'],
    'fr': ['/usr/share/dict/french'],
}

def get_words(paths):
    wordlist = set()
    for path in paths:
        with open(path) as f:
            for w in f:
                wordlist.add(unicode(w.rstrip('\n'), 'utf-8').lower())
    return wordlist

dicts = dict((iso_code, get_words(paths)) for iso_code, paths in dict_paths.items())

# music specific words
for w in [u'remix', u'intro', u'feat', u'volume', u'mix', u'disc', u'dj', u'outro']:
    for lang in dicts.keys():
        dicts[lang].add(w)

query_languages = '''SELECT DISTINCT id, iso_code_1, name FROM language'''
iso_to_mb = dict((iso_code, {'id': language_id, 'name': name}) for (language_id, iso_code, name) in db.execute(query_languages))
mb_to_iso = dict((v['id'], k) for k, v in iso_to_mb.items())

LANG = 'en'
wordlist = dicts[LANG]

_re_word = re.compile(ur"^([^\W\d_]|')+$", re.UNICODE)
_re_split_words = re.compile(ur"[^\w']", re.UNICODE)
def split_words(text):
    text = re.sub(ur"’", ur"'", unicode(text))
    words = [w.strip(u"'") for w in _re_split_words.split(text)]
    return [w.lower() for w in words if _re_word.match(w)]

def main(verbose=False):
    releases_by_ac = defaultdict(list)
    hist = defaultdict(int)
    for count_all, (ac, release, gid, release_name, old_language_id) in enumerate(db.execute(query_releases_with_unknown_language)):
        names = u' '.join([release_name] + [track_name for (track_name,) in db.execute(query_track_names, release)] + [medium_name for (medium_name,) in db.execute(query_medium_names, release) if medium_name])
        words = sorted(set(split_words(names)), key=len, reverse=True)
        if len(words) < MIN_WORDS:
            if verbose:
                out(u'too few words (%d): http://musicbrainz.org/release/%s' % (len(words), gid))
            continue
        if len(words[0]) < MIN_WORD_LEN:
            if verbose:
                out(u'longest word too short (%d): http://musicbrainz.org/release/%s' % (len(words[0]), gid))
            continue
        #print count_all
        #if len(words) >= 10 and not not_inside:
        if all(w in wordlist for w in words):
            #out(u'http://musicbrainz.org/release/%s' % gid)
            if ac != 1: # Various Artists
                other_languages = defaultdict(int)
                for l, in db.execute(query_release_languages_from_artist, ac):
                    other_languages[l] += 1
                other_languages = sorted([(n, l) for l, n in other_languages.iteritems()], reverse=True)
                if other_languages and other_languages[0][1] != LANG:
                    if verbose:
                        out('other releases are mostly in %s: http://musicbrainz.org/release/%s' % (other_languages[0], gid))
                    continue
            releases_by_ac[ac].append((gid, old_language_id, iso_to_mb[LANG]))
        not_inside = [w for w in words if w not in wordlist]
        for w in not_inside:
            hist[w] += 1

    if verbose:
        out(pprint.pformat(sorted([(i, w) for w, i in hist.iteritems()], reverse=True)[:50]))

    r_grouped = releases_by_ac.values()
    random.shuffle(r_grouped)
    r_flat = list(itertools.chain(*r_grouped))
    count = len(r_flat)
    if verbose:
        out('language can be set for %d out of %d releases' % (count, count_all))

    for i, (gid, old_language_id, new_language) in enumerate(r_flat):
        if verbose:
            out('%d/%d - %.2f%%' % (i+1, count, (i+1) * 100.0 / count))
        out(u'http://musicbrainz.org/release/%s -> %s' % (gid, new_language['name']))
        text = u'All words in release name/medium names/tracklist are from the %s dictionary, there are %d or more words and at least one of them is longer than %d characters. So I’m setting the language to “%s”.' % (new_language['name'], MIN_WORDS, MIN_WORD_LEN-1, new_language['name'])
        text += '\n\n%s' % program_string(__file__)
        if not old_language_id:
            old_language_id = ''
        try:
            mb.set_release_language(gid, old_language_id, new_language['id'], text, auto=False)
        except (urllib2.HTTPError, urllib2.URLError, socket.timeout) as e:
            out(e)

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-v', '--verbose', action='store_true', default=False,
            help='be more verbose')
    (options, args) = parser.parse_args()
    with PIDFile('/tmp/mbbot_set_language.pid'):
        main(options.verbose)
