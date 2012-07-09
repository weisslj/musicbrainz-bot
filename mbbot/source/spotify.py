import urllib
import urllib2
import json
import time
from kitchen.text.converters import to_bytes
from datetime import datetime
from bs4 import BeautifulSoup

class SpotifyWebService(object):
    """
    This product uses a SPOTIFY API but is not endorsed, certified or otherwise
    approved in any way by Spotify. Spotify is the registered trade mark of the
    Spotify Group.
    """

    def __init__(self):
        self.last_request_time = datetime.min
        self.REQUESTS_DELAY = 2.0

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
        if diff.total_seconds() < self.REQUESTS_DELAY:
            time.sleep(self.REQUESTS_DELAY - diff.total_seconds())

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
        data = self._fetch_json('http://ws.spotify.com/search/1/album', {'q': query})
        return data['albums']

    def artwork_url(self, uri):
        url = 'https://embed.spotify.com/?uri=%s&view=coverart' % uri
        soup = BeautifulSoup(urllib2.urlopen(url))
        tracks = soup.select('#content li')
        return tracks[0]['data-ca'] if len(tracks) > 0 else None
