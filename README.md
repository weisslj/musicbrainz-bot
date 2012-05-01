# MusicBrainz Bot

This bot it indended to add various data from the internet to MusicBrainz.

## Wikipedia Links

### Artist

It goes over all artists that do not have a Wikipedia link yet, and searches for the name in a local Solr index of English Wikipedia article titles. Once it finds a match,
it will fetch the article text from Wikipedia's API and verify that the text contains at least some release or release group titles (self-titled albums are ignored), work titles or URLs.

### Release-group

In a very similar way to how linking to artist works, it will verify that the article text matches at least some track titles.

## Medium format

It goes over all releases that use just Vinyl rather than a more specific format like 7", 10" or 12", and that have only one attached Discogs link.
Information will then be retrieved from Discogs and submitted to MusicBrainz.

## Discogs Links

Script exist to determine corresponding Discogs entries for MusicBrainz release and release-group.

