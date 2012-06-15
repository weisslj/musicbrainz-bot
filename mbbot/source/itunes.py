import urllib2
import urllib
import re
import json
import datetime
from datetime import datetime
from kitchen.text.converters import to_bytes
from xml.etree import ElementTree
from BeautifulSoup import BeautifulStoneSoup

NS_ITMS = '{http://phobos.apple.com/rss/1.0/modules/itms/}'


class ItunesStoreFeedItem(object):

    def __init__(self, item):
        self.album_type = None
        self.parse(item)
        self.fixup()

    def parse(self, item):
        self.artist = item.findtext(NS_ITMS + 'artist')
        self.artist_id = extractItmsId(item.findtext(NS_ITMS + 'artistLink'))
        self.album = item.findtext(NS_ITMS + 'album')
        album_url = item.findtext(NS_ITMS + 'albumLink')
        if '/album/' in album_url:
            self.album_id = extractItmsId(album_url)
        else:
            self.album_id = None
        self.release_date = parseItmsReleaseDate(item.findtext(NS_ITMS + 'releasedate'))

    def fixup(self):
        if self.album.endswith(' - EP'):
            self.album_type = 'EP'
            self.album = self.album[:-5]
        elif self.album.endswith(' - Single'):
            self.album_type = 'Single'
            self.album = self.album[:-9]
        BRACKETS_FEAT_RE = r'^(.+?) \[(feat\. .+?)\]$'
        if re.match(BRACKETS_FEAT_RE, self.album):
            self.album = re.sub(BRACKETS_FEAT_RE, r'\1 (\2)', self.album)


class ItunesStoreFeed(object):

    def __init__(self, rss):
        self.items = []
        for item in rss.findall('channel/item'):
            self.items.append(ItunesStoreFeedItem(item))


def extractItmsId(url):
    if url:
        m = re.search(r'/id(\d+)', url)
        if m is not None:
            return int(m.group(1))


def parseItmsReleaseDate(value):
    if not value:
        return None
    try:
        return datetime.datetime.strptime(value, '%B %d, %Y').date()
    except ValueError:
        return None


def itms_artist_url(id):
    if not id:
        return None
    return 'http://itunes.apple.com/us/artist/id%s' % (id,)


def itms_album_url(id):
    if not id:
        return None
    return 'http://itunes.apple.com/us/album/id%s' % (id,)


class ItunesSearchAPI:

    def __init__(self):
        self.last_request_time = datetime.min
        self.REQUESTS_DELAY = 1.0
        self.BASE_URL = 'http://itunes.apple.com/lookup'

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

    def lookup(self, id):
        if not id:
            return None
        data = self._fetch_json(self.BASE_URL, {'id': id})
        return data['results'][0] if len(data['results']) > 0 else None

    def search(self, params):
        data = self._fetch_json(self.BASE_URL, params)
        return data['results']


if __name__ == '__main__':
    rss = ElementTree.parse(open('rss.xml'))

    feed = ItunesStoreFeed(rss)
    for item in feed.items:
        print 'Artist:', item.artist, itms_artist_url(item.artist_id)
        print 'Album:', item.album, itms_album_url(item.album_id)
        print 'Album type:', item.album_type
        print 'Released:', item.release_date
        print

    #urllib2.urlopen('http://itunes.apple.com/WebObjects/MZStore.woa/wpa/MRSS/newreleases/sf=143441/limit=100/explicit=true/rss.xml')

