# -*- coding: utf-8 -*-

import urllib
import re
import locale
import sys
import os
import unicodedata

def mangle_name(s):
    s = unaccent(s.lower())
    s = re.sub(r'\(feat\. [^)]+\)$', '', s)
    return re.sub(r'\W', '', s, flags=re.UNICODE)


def join_names(type, strings):
    if not strings:
        return ''
    if len(strings) > 1:
        if type == 'category':
            result = 'categories'
        elif not type:
            result = type
        else:
            result = type + 's'
    else:
        result = type
    if result:
        result += ' '
    strings = ['"%s"' % s for s in strings]
    if len(strings) < 2:
        result += strings[0]
    elif len(strings) < 4:
        result += ', '.join(strings[:-1])
        result += ' and %s' % strings[-1]
    else:
        result += ', '.join(strings[:3])
        result += ' and %s more' % (len(strings) - 3)
    return result


script_ranges = {}
script_regexes = {}
for line in open('Scripts.txt'):
    line = line.strip()
    if line.startswith('#') or not line:
        continue
    parts = line.split(';', 2)
    range_str = parts[0].strip()
    script = parts[1].split()[0]
    if '..' in range_str:
        range = tuple(int(a, 16) for a in range_str.split('..'))
    else:
        range = (int(range_str, 16), int(range_str, 16))
    if script in script_ranges and range[0] - script_ranges[script][-1][1] == 1:
        script_ranges[script][-1] = (script_ranges[script][-1][0], range[1])
    else:
        script_ranges.setdefault(script, []).append(range)

def is_in_script(text, scripts):
    regex = ''
    for script in scripts:
        script_regex = script_regexes.get(script, '')
        if not script_regex:
            for range in script_ranges[script]:
                if range[0] == range[1]:
                    script_regex += '%s' % (re.escape(unichr(range[0])),)
                else:
                    script_regex += '%s-%s' % tuple(map(re.escape, map(unichr, range)))
            script_regexes[script] = script_regex
        regex += script_regex
    regex = '^[%s]+$' % regex
    print regex
    return bool(re.match(regex, text))


def contains_text_in_script(text, scripts):
    regex = ''
    for script in scripts:
        for range in script_ranges[script]:
            if range[0] == range[1]:
                regex += '%s' % (re.escape(unichr(range[0])),)
            else:
                regex += '%s-%s' % tuple(map(re.escape, map(unichr, range)))
    regex = '[%s]+' % regex
    return bool(re.search(regex, text))


def mw_remove_markup(text):
    result = []
    in_template = 0
    in_comment = 0
    for token in re.split(r'(\{\{|\}\}|<!--|-->)', text):
        if token == '{{':
            in_template += 1
        elif token == '}}':
            in_template -= 1
        elif token == '<!--':
            in_comment += 1
        elif token == '-->':
            in_comment -= 1
        elif not in_template and not in_comment:
            result.append(token)
    return ''.join(result)


def out(*args):
    args = [unicode(a).encode(locale.getpreferredencoding()) for a in args]
    sys.stdout.write(' '.join(args) + '\n')
    sys.stdout.flush()

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    NONE = ''

def colored_out(color, *args):
    args = [unicode(a).encode(locale.getpreferredencoding()) for a in args]
    sys.stdout.write(color + ' '.join(args) + bcolors.ENDC + '\n')
    sys.stdout.flush()

def get_page_content_from_cache(title, wp_lang):
    key = title.encode('utf-8', 'xmlcharrefreplace').replace('/', '_')
    file = os.path.join('wiki-cache', wp_lang, key[0], key)
    if os.path.exists(file):
        return open(file).read().decode('utf8')


def add_page_content_to_cache(title, content, wp_lang):
    key = title.encode('utf-8', 'xmlcharrefreplace').replace('/', '_')
    dir = os.path.join('wiki-cache', wp_lang, key[0])
    if not os.path.exists(dir):
        os.mkdir(dir)
    file = os.path.join(dir, key)
    f = open(file, 'w')
    f.write(content.encode('utf8'))
    f.close()


def get_page_content(wp, title, wp_lang, use_cache=True):
    if use_cache:
        content = get_page_content_from_cache(title, wp_lang)
        if content:
            return content
    resp = wp.call({'action': 'query', 'prop': 'revisions', 'titles': title.encode('utf8'), 'rvprop': 'content'})
    pages = resp['query']['pages'].values()
    if not pages or 'revisions' not in pages[0]:
        return None
    content = pages[0]['revisions'][0].values()[0]
    add_page_content_to_cache(title, content, wp_lang)
    return content


def extract_page_title(url, wp_lang):
    prefix = 'http://%s.wikipedia.org/wiki/' % wp_lang
    if not url.startswith(prefix):
        return None
    return urllib.unquote(url[len(prefix):].encode('utf8')).decode('utf8')

def wp_is_canonical_page(title, page_orig):
    page = mangle_name(page_orig)
    if 'redirect' in page:
        return False, "redirect page"
    if 'disambiguation' in title or \
        '{{disambig' in page_orig.lower() or '{{disamb' in page_orig.lower() or \
        'disambiguationpages' in page or \
        '{{hndis' in page_orig.lower() or \
        '{{homonymie}}' in page:
        return False, "disambiguation page"
    return True, ""

def quote_page_title(title):
    return urllib.quote(title.encode('utf8').replace(' ', '_'), '/$,:;@')

_unaccent_dict = {u'Æ': u'AE', u'æ': u'ae', u'Œ': u'OE', u'œ': u'oe', u'ß': 'ss',
                u"…": u"...", u"‘": u"'", u"’": u"'", u"‚": u"'", u"“": u"\"", u"”": u"\"",
                u"„": u"\"", u"′": u"'", u"″": u"\"", u"‹": u"<", u"›": u">", u"‐": u"-",
                u"‒": u"-", u"–": u"-", u"−": u"-", u"—": u"-", u"―": u"-"}
_re_latin_letter = re.compile(r"^(LATIN [A-Z]+ LETTER [A-Z]+) WITH")
def unaccent(string):
    """Remove accents ``string``."""
    result = []
    for char in string:
        if char in _unaccent_dict:
            char = _unaccent_dict[char]
        else:
            try:
                name = unicodedata.name(char)
                match = _re_latin_letter.search(name)
                if match:
                    char = unicodedata.lookup(match.group(1))
            except:
                pass
        result.append(char)
    return "".join(result)

_re_duration = re.compile(r"^(\d{1,2})\:(\d{2})")
def durationToMS(string):
    m = _re_duration.match(string)
    if not m:
        return None
    return (int(m.group(1))*60 + int(m.group(2)))*1000

def msToDuration(length):
    minutes = int( length/1000/60 ) % 60
    seconds = int( length/1000 ) % 60
    return "%02d:%02d" % (minutes, seconds)

def escape_query(s):
    s = re.sub(r'\bOR\b', 'or', s)
    s = re.sub(r'\bAND\b', 'and', s)
    s = re.sub(r'\bNOT\b', 'not', s)
    s = re.sub(r'\+', '\\+', s)
    s = re.sub(r'\-', '\\-', s)
    return s

# from Picard 1.0
def asciipunct(string):
    """Convert some Unicode punctation characters to ASCII ones in ``string``."""
    mapping = {
        u"…": u"...",
        u"‘": u"'",
        u"’": u"'",
        u"‚": u"'",
        u"“": u"\"",
        u"”": u"\"",
        u"„": u"\"",
        u"′": u"'",
        u"″": u"\"",
        u"‹": u"<",
        u"›": u">",
        u"‐": u"-",
        u"‒": u"-",
        u"–": u"-",
        u"−": u"-",
        u"—": u"-",
        u"―": u"-", # modification: "-" instead of "--"
    }
    for orig, repl in mapping.iteritems():
        string = string.replace(orig, repl)
    return string

def structureToString(obj):
    if obj is None:
        return ''
    elif isinstance (obj, (int, float)):
        return str(obj)
    elif isinstance (obj, (str)):
        return obj
    elif isinstance (obj, (unicode)):
        obj.encode('utf8')
    elif isinstance (obj, (list, tuple)):
        ret = []
        for item in obj:
            ret.append(structureToString(item))
        return '[' + ",".join(ret) + ']'
    else:
        ret = []
        for key in sorted(obj.iterkeys()):
            ret.append("%s:%s" % ( key, structureToString(obj[key]) ))
        return '{' + ",".join(ret) + '}'
