# (c) 2012 Ian Weller
# This program is free software. It comes without any warranty, to the extent
# permitted by applicable law. You can redistribute it and/or modify it under
# the terms of the Do What The Fuck You Want To Public License, Version 2, as
# published by Sam Hocevar. See COPYING for more details.

import codecs
from datetime import datetime
from getpass import getpass
import json
from kitchen.text.converters import to_bytes, to_unicode
import logging
import musicbrainzngs
import os
import os.path
from picard.similarity import similarity2
from pprint import pprint
import time
import urllib
import urllib2

musicbrainzngs.set_useragent(
    "yankisrc.py",
    "0.1",
    "ianweller@gmail.com",
)


class SpotifyWebService(object):
    """
    This product uses a SPOTIFY API but is not endorsed, certified or otherwise
    approved in any way by Spotify. Spotify is the registered trade mark of the
    Spotify Group.
    """

    def __init__(self):
        self.last_request_time = datetime.min

    def _fetch_json(self, url, params):
        self._check_rate_limit()
        # urllib.urlencode expects str objects, not unicode
        fixed = dict([(to_bytes(b[0]), to_bytes(b[1]))
                      for b in params.items()])
        request = urllib2.Request(url + '?' + urllib.urlencode(fixed))
        request.add_header('Accept', 'application/json')
        response = urllib2.urlopen(request)
        data = json.loads(response.read())
        self.last_request_time = datetime.now()
        return data

    def _check_rate_limit(self):
        diff = datetime.now() - self.last_request_time
        if diff.total_seconds() < 0.15:
            time.sleep(0.15 - diff.total_seconds())

    def lookup(self, uri, detail=0):
        """
        Detail ranges from 0 to 2 and determines the level of detail of child
        objects (i.e. for an artist, detail changes how much information is
        returned on albums).
        """
        params = {'uri': uri}
        if detail != 0:
            if 'artist' in uri:
                extras = [None, 'album', 'albumdetail'][detail]
            elif 'album' in uri:
                extras = [None, 'track', 'trackdetail'][detail]
            else:
                extras = None
            if extras:
                params['extras'] = extras
        data = self._fetch_json('http://ws.spotify.com/lookup/1/', params)
        return data[uri.split(':')[1]]

    def search_albums(self, query):
        data = self._fetch_json('http://ws.spotify.com/search/1/album',
                                {'q': query})
        return data['albums']


def fetch_mbrainz_album_info_from_id(mbid):
    data = musicbrainzngs.get_release_by_id(
        mbid, includes=['artists', 'recordings', 'isrcs'])
    return data['release']


def fetch_spotify_album_info_from_barcode(sws, barcode):
    albums = sws.search_albums('upc:%s' % barcode)
    if len(albums) != 1:
        return None
    uri = albums[0]['href']
    return sws.lookup(uri, detail=2)


def normalize_mbrainz_data(mbrainz):
    data = {}
    data['title'] = mbrainz['title']
    data['artist'] = mbrainz['artist-credit-phrase']
    data['tracks'] = []
    for medium in mbrainz['medium-list']:
        for track in medium['track-list']:
            trackdata = {
                'title': track['recording']['title'],
                'length': float(track['recording']['length']) / 1000,
            }
            data['tracks'].append(trackdata)
    return data


def normalize_spotify_data(spotify):
    data = {}
    data['title'] = spotify['name']
    data['artist'] = spotify['artist']
    data['tracks'] = []
    for track in spotify['tracks']:
        data['tracks'].append({
            'title': track['name'],
            'length': track['length'],
        })
    return data


def similarity(a, b):
    return int(similarity2(to_unicode(a), to_unicode(b)) * 100)


def compare_data(mbrainz_in, spotify_in):
    mbrainz = normalize_mbrainz_data(mbrainz_in)
    spotify = normalize_spotify_data(spotify_in)
    title = similarity(mbrainz['title'], spotify['title'])
    artist = similarity(mbrainz['artist'], spotify['artist'])
    if abs(len(mbrainz['tracks']) - len(spotify['tracks'])) != 0:
        return 0
    track = []
    track_time_diff = []
    track_sim = []
    for i in range(len(mbrainz['tracks'])):
        track.append(similarity(mbrainz['tracks'][i]['title'], spotify['tracks'][i]['title']))
        track_time_diff.append(abs(mbrainz['tracks'][i]['length'] - spotify['tracks'][i]['length']))
        if track_time_diff[i] > 15:
            track_time_sim = 0
        else:
            track_time_sim = int((15 - track_time_diff[i]) / 15 * 100)
        track_sim.append(int(track[i] * 0.50) + int(track_time_sim * 0.50))
    return int(title * 0.15) + int(artist * 0.15) + int(sum(track_sim) * 0.70 / len(mbrainz['tracks']))


def mblogin():
    print "Username:",
    user = raw_input()
    passwd = getpass()
    musicbrainzngs.auth(user, passwd)


def seconds_to_minsec(seconds):
    minutes = seconds / 60
    newsec = seconds % 60
    return '%d:%06.3f' % (minutes, newsec)


def make_html_comparison_page(mbrainz, spotify):
    with codecs.open('compare.html', mode='w', encoding='utf-8') as f:
        f.write('<html><head><meta charset="utf-8"></head><body><div style="float:left;width:50%">')
        f.write('<div style="font-weight:bold">%s</div>' % mbrainz['title'])
        f.write('<div style="font-weight:bold">%s</div>' % mbrainz['artist-credit-phrase'])
        for medium in mbrainz['medium-list']:
            mediumno = medium['position']
            for track in medium['track-list']:
                f.write('<div>%s-%s: %s (%s)</div>' %
                        (mediumno, track['position'],
                         track['recording']['title'],
                         seconds_to_minsec(int(track['recording']['length']) * .001)))
        f.write('</div><div style="float:right;width:50%">')
        f.write('<div style="font-weight:bold">%s</div>' % spotify['name'])
        f.write('<div style="font-weight:bold">%s</div>' % spotify['artist'])
        for track in spotify['tracks']:
            f.write('<div>%s-%s: %s (%s)</div>' %
                    (track['disc-number'], track['track-number'],
                     track['name'], seconds_to_minsec(track['length'])))
        f.write('</div></body></html>')


def isrcify_mbid(mbid, sws):
    info_mb = fetch_mbrainz_album_info_from_id(mbid)
    info_sp = fetch_spotify_album_info_from_barcode(sws, info_mb['barcode'])
    if not info_sp:
        return
    for track in info_sp['tracks']:
        for extid in track['external-ids']:
            if extid['type'] == 'isrc':
                if extid['id'].upper()[:2] == 'TC':
                    print 'TuneCore song IDs detected! Bailing out'
                    return
    sim = compare_data(info_mb, info_sp)
    print '%s by %s' % (info_mb['title'], info_mb['artist-credit-phrase'])
    print 'Similarity: %d%%' % sim
    print 'http://musicbrainz.org/release/%s' % mbid
    print 'http://open.spotify.com/album/%s' % info_sp['href'].split(':')[-1]
    print 'http://ws.spotify.com/lookup/1/?uri=%s&extras=trackdetail' % info_sp['href']
    make_html_comparison_page(info_mb, info_sp)
    print 'Add ISRCs? [y/n]',
    response = raw_input()
    if response.lower() == 'y':
        submit_isrcs(info_mb, info_sp)
        print "Added by yankisrc.py"
        print
        print "Data from http://ws.spotify.com/lookup/1/?uri=%s&extras=trackdetail" % info_sp['href']
        print "Barcodes match, metadata matched %d%%" % sim


def submit_isrcs(info_mb, info_sp):
    mbids = []
    for medium in info_mb['medium-list']:
        for track in medium['track-list']:
            mbids.append(track['recording']['id'])
    isrcs = []
    for track in info_sp['tracks']:
        this_isrc = []
        for extid in track['external-ids']:
            if extid['type'] == 'isrc':
                this_isrc.append(extid['id'].upper())
        isrcs.append(this_isrc)
    musicbrainzngs.submit_isrcs(dict(zip(mbids, isrcs)))


def do_mb_search(entity, query='', fields={}, limit=None, offset=None):
	"""Perform a full-text search on the MusicBrainz search server.
	`query` is a free-form query string and `fields` is a dictionary
	of key/value query parameters. They keys in `fields` must be valid
	for the given entity type.
	"""
	# Encode the query terms as a Lucene query string.
	query_parts = [query.replace('\x00', '').strip()]
	for key, value in fields.iteritems():
		# Ensure this is a valid search field.
		if key not in musicbrainzngs.VALID_SEARCH_FIELDS[entity]:
			raise InvalidSearchFieldError(
				'%s is not a valid search field for %s' % (key, entity)
			)

		# Escape Lucene's special characters.
        #value = re.sub(r'([+\-&|!(){}\[\]\^"~*?:\\])', r'\\\1', value)
		value = value.replace('\x00', '').strip()
		if value:
			query_parts.append(u'%s:(%s)' % (key, value))
	full_query = u' '.join(query_parts).strip()
	if not full_query:
		raise ValueError('at least one query term is required')

	# Additional parameters to the search.
	params = {'query': full_query}
	if limit:
		params['limit'] = str(limit)
	if offset:
		params['offset'] = str(offset)

	return musicbrainzngs.musicbrainz._do_mb_query(entity, '', [], params)


if __name__ == '__main__':
    donepath = os.path.join(os.getenv('HOME'), '.yankisrc_done')
    with open(donepath, 'r') as f:
        done = [x.strip() for x in f.readlines()]
    donefile = open(donepath, 'a')
    mblogin()
    sws = SpotifyWebService()
    offset = 0
    while True:
        fields = {
            'barcode': "[0 TO 99999999999999999]",
            'type': "album",
            'status': "official",
        }
        rels = do_mb_search('release', '', fields, 100, offset)
        if len(rels['release-list']) == 0:
            break
        offset += len(rels['release-list'])
        for rel in rels['release-list']:
            mbid = rel['id']
            if mbid in done:
                continue
            print 'MBID: %s' % mbid
            isrcify_mbid(mbid, sws)
            done.append(mbid)
            donefile.write(mbid + '\n')
            donefile.flush()
