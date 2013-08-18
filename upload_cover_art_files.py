#!/usr/bin/python

import sys
import os
import re
import time
import urllib2
import json
from editing import MusicBrainzClient
from utils import out, colored_out, bcolors, monkeypatch_mechanize
import config as cfg

# Work around mechanize bug. See: https://github.com/jjlee/mechanize/pull/58
monkeypatch_mechanize()

mb = MusicBrainzClient(cfg.MB_USERNAME, cfg.MB_PASSWORD, cfg.MB_SITE)

FILE_RE = re.compile(r'^(?P<mbid>[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})-(?P<type>front|back|medium|booklet|tray)(?:-\d+)?\.(?:jpeg|jpg|png|gif)', re.I)

class CoverArtArchiveReleaseInfo(object):
	def __init__(self, release_id):
		try:
			data = urllib2.urlopen('http://coverartarchive.org/release/%s/' % release_id)
			self.metadata = json.load(data)
		except urllib2.HTTPError:
			self.metadata = {'images':  [], 'release': 'http://musicbrainz.org/release/%s' % release_id}

	def hasType(self, type):
		for image in self.metadata['images']:
			for img_type in image['types']:
				if img_type.lower() == type.lower():
					return True
		return False

	def getImages(self, type=None):
		if type is None:
			images = self.metadata['images']
		else:
			images = []
			for image in self.metadata['images']:
				for img_type in image['types']:
					if img_type == type:
						images.append(image)
						break
		return images

for file in sys.argv[1:]:
	colored_out(bcolors.OKBLUE, "File '%s'" % os.path.basename(file))
	if not os.path.exists(file):
		colored_out(bcolors.FAIL, " * File not found")
		continue
	m = FILE_RE.match(os.path.basename(file))
	if m is None:
		colored_out(bcolors.FAIL, " * File doesn't match defined regular expression")
		continue

	mbid = m.group('mbid')
	type = m.group('type')
	caa_rel_info = CoverArtArchiveReleaseInfo(mbid)
	if caa_rel_info.hasType(type) and type not in ('medium', 'booklet') and False:
		colored_out(bcolors.WARNING, " * Release already has an image of type '%s' => skipping" % type.lower())
		continue

	colored_out(bcolors.OKGREEN, " * Adding %s cover art to http://musicbrainz.org/release/%s" % (type, mbid))
	time.sleep(10)
	mb.add_cover_art(mbid, file, [type], None, '', '', False)
