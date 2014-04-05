import mechanize
import urllib
import urllib2
import time
import re
import os
import random
import string
import json
import config as cfg
import hashlib
import base64
import imghdr
from utils import structureToString
from datetime import datetime
from mbbot.guesscase import guess_artist_sort_name

def test_plain_jpeg(h, f):
    """Without this, imghdr only recognizes images with JFIF/Exif header. http://bugs.python.org/issue16512"""
    if h.startswith('\xff\xd8'):
        return 'jpeg'

imghdr.tests.append(test_plain_jpeg)


def format_time(secs):
    return '%0d:%02d' % (secs // 60, secs % 60)


def album_to_form(album):
    form = {}
    form['artist_credit.names.0.artist.name'] = album['artist']
    form['artist_credit.names.0.name'] = album['artist']
    if album.get('artist_mbid'):
        form['artist_credit.names.0.mbid'] = album['artist_mbid']
    form['name'] = album['title']
    if album.get('date'):
        date_parts = album['date'].split('-')
        if len(date_parts) > 0:
            form['date.year'] = date_parts[0]
            if len(date_parts) > 1:
                form['date.month'] = date_parts[1]
                if len(date_parts) > 2:
                    form['date.day'] = date_parts[2]
    if album.get('label'):
        form['labels.0.name'] = album['label']
    if album.get('barcode'):
        form['barcode'] = album['barcode']
    for medium_no, medium in enumerate(album['mediums']):
        form['mediums.%d.format' % medium_no] = medium['format']
        form['mediums.%d.position' % medium_no] = medium['position']
        for track_no, track in enumerate(medium['tracks']):
            form['mediums.%d.track.%d.position' % (medium_no, track_no)] = track['position']
            form['mediums.%d.track.%d.name' % (medium_no, track_no)] = track['title']
            form['mediums.%d.track.%d.length' % (medium_no, track_no)] = format_time(track['length'])
    form['edit_note'] = 'http://www.cdbaby.com/cd/' + album['_id'].split(':')[1]
    return form


class MusicBrainzClient(object):

    def __init__(self, username, password, server="http://musicbrainz.org", editor_id=None):
        self.server = server
        self.username = username
        self.editor_id = editor_id
        self.b = mechanize.Browser()
        self.b.set_handle_robots(False)
        self.b.set_debug_redirects(False)
        self.b.set_debug_http(False)
        self.b.addheaders = [('User-agent', 'musicbrainz-bot/1.0 ( %s/user/%s )' % (server, username))]
        self.login(username, password)

    def url(self, path, **kwargs):
        query = ''
        if kwargs:
            query = '?' + urllib.urlencode([(k, v.encode('utf8')) for (k, v) in kwargs.items()])
        return self.server + path + query

    def _select_form(self, action):
        self.b.select_form(predicate=lambda f: f.method == "POST" and action in f.action)

    def login(self, username, password):
        self.b.open(self.url("/login"))
        self._select_form("/login")
        self.b["username"] = username
        self.b["password"] = password
        self.b.submit()
        resp = self.b.response()
        if resp.geturl() != self.url("/user/" + username):
            raise Exception('unable to login')

    # return number of edits that left for today
    def edits_left(self, max_edits=1000):
        if self.editor_id is None:
            print 'error, pass editor_id to constructor for edits_left()'
            return 0
        today = datetime.utcnow().strftime('%Y-%m-%d')
        kwargs = {
                'page': '2000',
                'combinator': 'and',
                'negation': '0',
                'conditions.0.field': 'open_time',
                'conditions.0.operator': '>',
                'conditions.0.args.0': today,
                'conditions.0.args.1': '',
                'conditions.1.field': 'editor',
                'conditions.1.operator': '=',
                'conditions.1.name': self.username,
                'conditions.1.args.0': str(self.editor_id)
        }
        url = self.url("/search/edits", **kwargs)
        self.b.open(url)
        page = self.b.response().read()
        m = re.search(r'Found (?:at least )?([0-9]+(?:,[0-9]+)?) edits', page)
        if not m:
            print 'error, could not determine remaining edits'
            return 0
        return max_edits - int(re.sub(r'[^0-9]+', '', m.group(1)))

    def _extract_mbid(self, entity_type):
        m = re.search(r'/'+entity_type+r'/([0-9a-f-]{36})$', self.b.geturl())
        if m is None:
            raise Exception('unable to post edit')
        return m.group(1)

    def add_release(self, album, edit_note, auto=False):
        form = album_to_form(album)
        self.b.open(self.url("/release/add"), urllib.urlencode(form))
        time.sleep(2.0)
        self._select_form("/release")
        self.b.submit(name="step_editnote")
        time.sleep(2.0)
        self._select_form("/release")
        print self.b.response().read()
        self.b.submit(name="save")
        return self._extract_mbid('release')

    def add_artist(self, artist, edit_note, auto=False):
        self.b.open(self.url("/artist/create"))
        self._select_form("/artist/create")
        self.b["edit-artist.name"] = artist['name']
        self.b["edit-artist.sort_name"] = artist.get('sort_name', guess_artist_sort_name(artist['name']))
        self.b["edit-artist.edit_note"] = edit_note.encode('utf8')
        self.b.submit()
        return self._extract_mbid('artist')

    def _as_auto_editor(self, prefix, auto):
        try: self.b[prefix+"as_auto_editor"] = ["1"] if auto else []
        except mechanize.ControlNotFoundError: pass

    def _check_response(self, already_done_msg='any changes to the data already present'):
        page = self.b.response().read()
        if "Thank you, your " not in page:
            if not already_done_msg or already_done_msg not in page:
                raise Exception('unable to post edit')
            else:
                return False
        return True
    def edit_note_and_auto_editor_and_submit_and_check_response(self, prefix, auto, edit_note, already_done_msg='default'):
        self.b[prefix+"edit_note"] = edit_note.encode('utf8')
        self._as_auto_editor(prefix, auto)
        self.b.submit()
        if already_done_msg!='default':
            return self._check_response(already_done_msg)
        else:
            return self._check_response()

    def add_url(self, entity_type, entity_id, link_type_id, url, edit_note='', auto=False):
        self.b.open(self.url("/edit/relationship/create_url", entity=entity_id, type=entity_type))
        self._select_form("create_url")
        self.b["ar.link_type_id"] = [str(link_type_id)]
        self.b["ar.url"] = str(url)
        return self._edit_note_and_auto_editor_and_submit_and_check_response('ar.',auto,edit_note,'already exists')

    def _update_entity_if_not_set(self, update, entity_dict, entity_type, item, suffix="_id", utf8ize=False, inarray=False):
        if item in update:
            key = "edit-"+entity_type+"."+item+suffix
            if self.b[key] != (inarray and [''] or ''):
                print " * "+item+" already set, not changing"
                return False
            val = (
                utf8ize and entity_dict[item].encode('utf-8') or str(entity_dict[item]))
            self.b[key] = (inarray and [val] or val)
        return True

    def _update_artist_date_if_not_set(self, update, artist, item_prefix):
        item = item_prefix+'_date'
        if item in update:
            prefix = "edit-artist.period."+item
            if self.b[prefix+".year"]:
                print " * "+item.replace('_',' ')+" year already set, not changing"
                return False
            self.b[prefix+".year"] = str(artist[item+'_year'])
            if artist[item+'_month']:
                self.b[prefix+".month"] = str(artist[item+'_month'])
                if artist[item+'_day']:
                    self.b[prefix+".day"] = str(artist[item+'_day'])
        return True

    def edit_artist(self, artist, update, edit_note, auto=False):
        self.b.open(self.url("/artist/%s/edit" % (artist['gid'],)))
        self._select_form("/edit")
        self.b.set_all_readonly(False)
        if not self._update_entity_if_not_set(update,artist,'artist','area'):
            return
        for item in ['type','gender']:
            if not self._update_entity_if_not_set(update,artist,'artist',item, inarray=True):
                return
        for item_prefix in ['begin', 'end']:
            if not self._update_artist_date_if_not_set(update, artist, item_prefix):
                return
        if not self._update_entity_if_not_set(update,artist,'artist', 'comment','',utf8ize=True):
            return
        return self._edit_note_and_auto_editor_and_submit_and_check_response('edit-artist.',auto,edit_note)

    def edit_artist_credit(self, entity_id, credit_id, ids, names, join_phrases, edit_note):
        assert len(ids) == len(names) == len(join_phrases)+1
        join_phrases.append('')

        self.b.open(self.url("/artist/%s/credit/%d/edit" % (entity_id, int(credit_id))))
        self._select_form("/edit")

        for i in range(len(ids)):
            for field in ['artist.id', 'artist.name', 'name', 'join_phrase']:
                k = "split-artist.artist_credit.names.%d.%s" % (i, field)
                try:
                    self.b.form.find_control(k).readonly = False
                except mechanize.ControlNotFoundError:
                    self.b.form.new_control('text', k, {})
        self.b.fixup()

        for i, aid in enumerate(ids):
            self.b["split-artist.artist_credit.names.%d.artist.id" % i] = str(int(aid))
        # Form also has "split-artist.artist_credit.names.%d.artist.name", but it is not required
        for i, name in enumerate(names):
            self.b["split-artist.artist_credit.names.%d.name" % i] = name.encode('utf-8')
        for i, join in enumerate(join_phrases):
            self.b["split-artist.artist_credit.names.%d.join_phrase" % i] = join.encode('utf-8')

        self.b["split-artist.edit_note"] = edit_note.encode('utf-8')
        self.b.submit()
        return self._check_response()

    def set_artist_type(self, entity_id, type_id, edit_note, auto=False):
        self.b.open(self.url("/artist/%s/edit" % (entity_id,)))
        self._select_form("/edit")
        if self.b["edit-artist.type_id"] != ['']:
            print " * already set, not changing"
            return
        self.b["edit-artist.type_id"] = [str(type_id)]
        return self._edit_note_and_auto_editor_and_submit_and_check_response('edit-artist.',auto,edit_note)

    def edit_url(self, entity_id, old_url, new_url, edit_note, auto=False):
        self.b.open(self.url("/url/%s/edit" % (entity_id,)))
        self._select_form("/edit")
        if self.b["edit-url.url"] != str(old_url):
            print " * value has changed, aborting"
            return
        if self.b["edit-url.url"] == str(new_url):
            print " * already set, not changing"
            return
        self.b["edit-url.url"] = str(new_url)
        return self._edit_note_and_auto_editor_and_submit_and_check_response('edit-url.',auto,edit_note)

    def edit_work(self, work, update, edit_note, auto=False):
        self.b.open(self.url("/work/%s/edit" % (work['gid'],)))
        self._select_form("/edit")
        for item in ['type','language']:
            if not self._update_entity_if_not_set(update,work,'work',item, inarray=True):
                return
        if not self._update_entity_if_not_set(update,work,'work','comment','',utf8ize=True):
            return
        return self._edit_note_and_auto_editor_and_submit_and_check_response('edit-work.',auto,edit_note)

    def edit_relationship(self, rel_id, entity0_type, entity1_type, old_link_type_id, new_link_type_id, attributes, begin_date, end_date, edit_note, auto=False):
        self.b.open(self.url("/edit/relationship/edit", id=str(rel_id), type0=entity0_type, type1=entity1_type))
        self._select_form("/edit")
        if self.b["ar.link_type_id"] == [str(new_link_type_id)] and new_link_type_id != old_link_type_id:
            print " * already set, not changing"
            return
        if self.b["ar.link_type_id"] != [str(old_link_type_id)]:
            print " * value has changed, aborting"
            return
        self.b["ar.link_type_id"] = [str(new_link_type_id)]
        for k, v in attributes.items():
            self.b["ar.attrs."+k] = v
        for k, v in begin_date.items():
            self.b["ar.period.begin_date."+k] = str(v)
        for k, v in end_date.items():
            self.b["ar.period.end_date."+k] = str(v)
        return self._edit_note_and_auto_editor_and_submit_and_check_response('ar.',auto,edit_note, "exists with these attributes")

    def remove_relationship(self, rel_id, entity0_type, entity1_type, edit_note):
        self.b.open(self.url("/edit/relationship/delete", id=str(rel_id), type0=entity0_type, type1=entity1_type))
        self._select_form("/edit")
        self.b["confirm.edit_note"] = edit_note.encode('utf8')
        self.b.submit()
        self._check_response(None)

    def merge(self, entity_type, entity_ids, target_id, edit_note):
        params = [('add-to-merge', id) for id in entity_ids]
        self.b.open(self.url("/%s/merge_queue" % entity_type), urllib.urlencode(params))
        page = self.b.response().read()
        if "You are about to merge" not in page:
            raise Exception('unable to add items to merge queue')

        params = {'merge.target': target_id, 'submit': 'submit', 'merge.edit_note': edit_note}
        for idx, val in enumerate(entity_ids):
            params['merge.merging.%s' % idx] = val
        self.b.open(self.url("/%s/merge" % entity_type), urllib.urlencode(params))
        self._check_response(None)

    def _edit_release_information(self, entity_id, attributes, edit_note, auto=False):
        self.b.open(self.url("/release/%s/edit" % (entity_id,)))
        self._select_form("/edit")
        changed = False
        for k, v in attributes.items():
            self.b.form.find_control(k).readonly = False
            if self.b[k] != v[0] and v[0] is not None:
                print " * %s has changed to %r, aborting" % (k, self.b[k])
                return False
            if self.b[k] != v[1]:
                changed = True
                self.b[k] = v[1]
        if not changed:
            print " * already set, not changing"
            return False
        self.b["barcode_confirm"] = ["1"]
        self.b.submit(name="step_editnote")
        page = self.b.response().read()
        self._select_form("/edit")
        try:
            self.b["edit_note"] = edit_note.encode('utf8')
        except mechanize.ControlNotFoundError:
            raise Exception('unable to post edit')
        self._as_auto_editor("", auto)
        self.b.submit(name="save")
        page = self.b.response().read()
        if "Release information" not in page:
            raise Exception('unable to post edit')
        return True

    def edit_release_tracklisting(self, entity_id, mediums, edit_note=u'', auto=False):
        """
        Edit a release tracklisting. Doesn't handle adding/deleting tracks.
        Each medium object may contain the following properties: position, format, name, tracklist.
        Each track object may contain the following properties: position, name, length, number, artist_credit.
        """
        self.b.open(self.url("/release/%s/edit" % (entity_id,)))

        self._select_form("/edit")
        self.b["barcode_confirm"] = ["1"]
        self.b.submit(name="step_tracklist")

        self._select_form("/edit")
        for medium_no, medium in enumerate(mediums):
            if 'position' in medium:
                self.b["mediums.%s.position" % medium_no] = medium['position']
            if 'name' in medium:
                self.b["mediums.%s.name" % medium_no] = medium['name']
            if 'format_id' in medium:
                self.b["mediums.%s.format_id" % medium_no] = medium['format_id']

            if 'tracklist' in medium:
                tracklist_id = self.b["mediums.%s.id" % medium_no]
                request = urllib2.Request('http://musicbrainz.org/ws/js/medium/%s' % tracklist_id, headers={"Accept" : "application/json"})
                data = urllib2.urlopen(request)
                old_tracklist = json.load(data)

                edited_tracklist = []
                for trackno, old_track in enumerate(old_tracklist['tracks']):
                    new_track = medium['tracklist'][trackno]# if medium['tracklist'][trackno] is not None else {}
                    name = new_track['name'] if 'name' in new_track else old_track['name']
                    to = {
                        'name': name,
                        'length': new_track['length'] if 'length' in new_track else old_track['length'],
                        'artist_credit': new_track['artist_credit'] if 'artist_credit' in new_track else old_track['artist_credit']
                    }
                    to['edit_sha1'] = base64.b64encode( hashlib.sha1(structureToString(to)).digest() )
                    to['position'] = trackno
                    to['deleted'] = 0
                    to['number'] = new_track['number'] if 'number' in new_track else old_track['number']

                    edited_tracklist.append(to)

                self.b["mediums.%s.edits" % medium_no] = json.dumps(edited_tracklist)

        self.b.submit(name="step_editnote")
        page = self.b.response().read()
        self._select_form("/edit")
        try:
            self.b["edit_note"] = edit_note.encode('utf8')
        except mechanize.ControlNotFoundError:
            raise Exception('unable to post edit')
        self._as_auto_editor("", auto)
        self.b.submit(name="save")
        page = self.b.response().read()
        if "Release information" not in page:
            raise Exception('unable to post edit')

    def set_release_script(self, entity_id, old_script_id, new_script_id, edit_note, auto=False):
        return self._edit_release_information(entity_id, {"script_id": [[str(old_script_id)],[str(new_script_id)]]}, edit_note, auto)

    def set_release_language(self, entity_id, old_language_id, new_language_id, edit_note, auto=False):
        return self._edit_release_information(entity_id, {"language_id": [[str(old_language_id)],[str(new_language_id)]]}, edit_note, auto)

    def set_release_packaging(self, entity_id, old_packaging_id, new_packaging_id, edit_note, auto=False):
        old_packaging = [str(old_packaging_id)] if old_packaging_id is not None else None
        return self._edit_release_information(entity_id, {"packaging_id": [old_packaging ,[str(new_packaging_id)]]}, edit_note, auto)

    def set_release_medium_format(self, entity_id, medium_number, old_format_id, new_format_id, edit_note, auto=False):
        self.b.open(self.url("/release/%s/edit" % (entity_id,)))

        self._select_form("/edit")
        self.b["barcode_confirm"] = ["1"]
        self.b.submit(name="step_tracklist")

        self._select_form("/edit")
        attributes = {
            "mediums.%s.format_id" % (medium_number-1): [[str(old_format_id) if old_format_id is not None else ''], [str(new_format_id)]]
        }
        changed = False
        for k, v in attributes.items():
            if self.b[k] != v[0]:
                print " * %s has changed, aborting" % k
                return
            if self.b[k] != v[1]:
                changed = True
                self.b[k] = v[1]
        if not changed:
            print " * already set, not changing"
            return
        self.b.submit(name="step_editnote")

        page = self.b.response().read()
        if "This medium already has disc ID" in page:
            print " * has a discid => medium format can't be set to a format that can't have disc IDs"
            return

        self._select_form("/edit")
        try:
            self.b["edit_note"] = edit_note.encode('utf8')
        except mechanize.ControlNotFoundError:
            raise Exception('unable to post edit')
        self._as_auto_editor("", auto)
        self.b.submit(name="save")
        page = self.b.response().read()
        if "Release information" not in page:
            raise Exception('unable to post edit')

    def add_edit_note(self, identify, edit_note):
        '''Adds an edit note to the last (or very recently) made edit. This
        is necessary e.g. for ISRC submission via web service, as it has no
        support for edit notes. The "identify" argument is a function
            function(str, str) -> bool
        which receives the edit number as first, the raw html body of the edit
        as second argument, and determines if the note should be added to this
        edit.'''
        self.b.open(self.url("/user/%s/edits" % (self.username,)))
        page = self.b.response().read()
        self._select_form("/edit")
        edits = re.findall(r'<h2><a href="'+self.server+r'/edit/([0-9]+).*?<div class="edit-details">(.*?)</div>', page, re.S)
        for i, (edit_nr, text) in enumerate(edits):
            if identify(edit_nr, text):
                self.b['enter-vote.vote.%d.edit_note' % i] = edit_note.encode('utf8')
                break
        self.b.submit()

    def cancel_edit(self, edit_nr, edit_note=u''):
        self.b.open(self.url("/edit/%s/cancel" % (edit_nr,)))
        page = self.b.response().read()
        self._select_form("/cancel")
        if edit_note:
            self.b['confirm.edit_note'] = edit_note.encode('utf8')
        self.b.submit()

    def add_cover_art(self, release_gid, image, types=[], position=None, comment=u'', edit_note=u'', auto=False):

        # Download image if it's remotely hosted
        image_is_remote = True if image.startswith(('http://', 'https://', 'ftp://')) else False
        if image_is_remote:
            u = urllib2.urlopen(image)
            f,ext = os.path.splitext(image)
            localFile = '%s/%s%s' % (cfg.TMP_DIR, ''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(8)), ext)
            tmpfile = open(localFile, 'w')
            tmpfile.write(u.read())
            tmpfile.close()
        else:
            localFile = image

        # Determine mime type
        fmt = imghdr.what(localFile)
        if fmt in ("jpeg", "png", "gif"):
            mime_type = "image/" + fmt
        elif fmt is None:
            raise Exception('Cannot recognize image type: %s' % localFile)
        else:
            raise Exception('Unsupported image type %s: %s' % (fmt, localFile))

        self.b.open(self.url("/release/%s/add-cover-art" % (release_gid,)))
        page = self.b.response().read()

        # Generate a new cover art id, as done by mbserver
        cover_art_id = int((time.time()-1327528905)*100)

        # Step 1: Request POST fields for CAA from http://musicbrainz.org/ws/js/cover-art-upload
        request = urllib2.Request('http://musicbrainz.org/ws/js/cover-art-upload/%s?image_id=%s&mime_type=%s&redirect=true' % (release_gid, cover_art_id, mime_type), headers={"Accept" : "application/json"})
        data = urllib2.urlopen(request)
        postfields = json.load(data)['formdata']

        # Step 2: Upload cover art to CAA
        self.b.follow_link(tag="iframe")
        TRIES = 4
        DELAY = 3
        attempts = 0
        while True:
            try:
                self._select_form("archive.org")
                self.b.add_file(open(localFile))
                # Insert fields from ws/js, simulating what's done in javascript
                for key, value in postfields.iteritems():
                    self.b.new_control('hidden', key, {'value': str(value)})
                self.b.fixup()
                self.b.submit()
                break
            except (urllib2.HTTPError, urllib2.URLError):
                if attempts < TRIES:
                    attempts += 1
                    self.b.back()
                    continue
                raise
        page = self.b.response().read()
        if "parent.document.getElementById" not in page:
            raise Exception('Error uploading cover art file')

        # Step 3: Submit the edit
        self.b.back(2)
        # Will probably fail. Solution is to install patched mechanize:
        # http://stackoverflow.com/questions/9249996/mechanize-cannot-read-form-with-submitcontrol-that-is-disabled-and-has-no-value
        self._select_form("add-cover-art")
        self.b.set_all_readonly(False)
        try: self.b['add-cover-art.as_auto_editor'] = 1 if auto else 0
        except mechanize._form.ControlNotFoundError: pass
        submitted_types = []
        types_control = self.b.find_control(name='add-cover-art.type_id')
        for type in types:
            for item in types_control.get_items():
                if len(item.get_labels()) > 0 and item.get_labels()[0].text.lower() == type.lower():
                    submitted_types.append(item.name)
                    break
        self.b['add-cover-art.type_id'] = submitted_types
        if position:
            self.b['add-cover-art.position'] = position
        if comment:
            self.b['add-cover-art.comment'] = comment.encode('utf8')
        if edit_note:
            self.b['add-cover-art.edit_note'] = edit_note.encode('utf8')
        self.b['add-cover-art.mime_type'] = [mime_type]
        self.b['add-cover-art.id'] = str(cover_art_id)
        self.b.submit()

        if image_is_remote:
            os.remove(localFile)
