# -*- coding: utf-8 -*-

import re
from simplemediawiki import MediaWiki
from utils import mw_remove_markup, get_page_content, extract_page_title

category_re = {}
category_re['en'] = re.compile(r'\[\[Category:(.+?)(?:\|.*?)?\]\]')
category_re['fr'] = re.compile(r'\[\[Cat\xe9gorie:(.+?)\]\]')

infobox_re = {}
infobox_re['en'] = re.compile(r'\{\{Infobox (musical artist|person)[^|]*((?:[^{}].*?|\{\{.*?\}\})*)\}\}', re.DOTALL)
infobox_re['fr'] = re.compile(r'\{\{Infobox (Musique \(artiste\)|Musique classique \(personnalit\xe9\))[^|]*((?:[^{}].*?|\{\{.*?\}\})*)\}\}', re.DOTALL)

persondata_re = {}
persondata_re['en'] = re.compile(r'\{\{Persondata[^|]*((?:[^{}].*?|\{\{.*?\}\})*)\}\}?', re.DOTALL)
persondata_re['fr'] = re.compile(r'\{\{Métadonn\xe9es personne[^|]*((?:[^{}].*?|\{\{.*?\}\})*)\}\}', re.DOTALL)

persondata_fields_mapping = {}
persondata_fields_mapping['fr'] = {
    'nom': 'name',
    'noms alternatifs': 'alternatives names',
    'courte description': 'short description',
    'date de naissance': 'date of birth',
    'lieu de naissance': 'place of birth',
    'date de décès': 'date of death',
    'lieu de décès': 'place of death',
}

class WikiPage(object):

    def __init__(self, title, text, lang, wikidata_id = None):
        self.title = title
        self.text = text
        self.lang = lang
        self.wikidata_id = wikidata_id
        self.categories = self.extract_page_categories(text)
        self.infobox = self.parse_infobox(text)
        self.persondata = self.parse_persondata(text)
        self.abstract = self.extract_first_paragraph(text)

    def extract_page_categories(self, page):
        if self.lang not in category_re:
            return []
        categories = category_re[self.lang].findall(page)
        return categories

    def parse_infobox(self, page):
        info = {}
        if self.lang not in infobox_re:
            return info
        match = infobox_re[self.lang].search(page)
        if match is None:
            return info
        info['_type'] = match.group(1)
        for line in match.group(2).splitlines():
            if '=' not in line:
                continue
            name, value = tuple(s.strip() for s in line.split('=', 1))
            info[name.lstrip('| ').lower()] = value
        return info


    def parse_persondata(self, page):
        info = {}
        if self.lang not in persondata_re:
            return info
        match = persondata_re[self.lang].search(page)
        if match is None:
            return info
        for line in match.group(1).splitlines():
            if '=' not in line:
                continue
            name, value = tuple(s.strip() for s in line.split('=', 1))
            name = name.lstrip('| ').lower()
            if self.lang in persondata_fields_mapping and len(persondata_fields_mapping[self.lang][name]) > 1:
                name = persondata_fields_mapping[self.lang][name]
            info[name] = value
        return info


    def extract_first_paragraph(self, page):
        page = mw_remove_markup(page)
        return page.strip().split('\n\n')[0]

    @classmethod
    def fetch(cls, url, use_cache=True):
        m = re.match(r'^http://([a-z\-]+)\.wikipedia\.org', url)
        page_lang = m.group(1).encode('utf8')
        page_title = extract_page_title(url, page_lang)
        wp = MediaWiki('http://%s.wikipedia.org/w/api.php' % page_lang)
        resp = wp.call({'action': 'query', 'prop': 'pageprops|revisions', 'titles': page_title.encode('utf8'), 'rvprop': 'content'})
        page = resp['query']['pages'].values()[0]
        content = page['revisions'][0].values()[0] if 'revisions' in page else None
        if 'pageprops' in page and 'wikibase_item' in page['pageprops']:
            wikidata_id = page['pageprops']['wikibase_item']
        else:
            wikidata_id = None
        return cls(page_title, content or '', page_lang, wikidata_id)
