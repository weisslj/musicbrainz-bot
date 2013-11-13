import urllib
import urllib2
import json
import time
from kitchen.text.converters import to_bytes
from datetime import datetime
from bs4 import BeautifulSoup

class SHSWebService(object):

    def __init__(self):
        self.last_request_time = datetime.min
        self.REQUESTS_DELAY = 10

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

    def lookup(self, entityType, entityId):
        endpoint = entityType
        if entityType == 'recording':
            endpoint = 'performance'
        url = 'http://www.secondhandsongs.com/%s/%s' % (endpoint, entityId)
        data = self._fetch_json(url, {})
        return data

    def lookup_work(self, work_id):
        return self.lookup('work', work_id)

    def search(self, entityType, params):
        endpoint = entityType
        if entityType == 'recording':
            endpoint = 'performance'
        url = 'http://www.secondhandsongs.com/search/%s' % endpoint
        data = self._fetch_json(url, params)
        return data

    def search_works(self, title, credits):
        params = {'title': title}
        if credits is not None:
            params['credits'] = credits
        return self.search('work', params)
