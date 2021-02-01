import re
import csv
import requests
import pandas as pd
from io import StringIO

from datetime import datetime


def to_csv(string_):
    line = StringIO()
    writer = csv.writer(line)
    writer.writerow(string_)
    return line.getvalue().rstrip()


class BookList:

    def __init__(self, url, source_key, cache=None):
        self.url = url
        self.source_key = source_key
        self.cache = cache

    def _get(self):
        raise NotImplementedError

    def get(self, *args, **kwargs):
        if self.cache is not None:
            try:
                df = self._from_cache(self.source_key)
            except KeyError:
                df = self._get(*args, **kwargs)
                self._to_cache(self.source_key, df)
            return df
        else:
            return self._get(*args, **kwargs)

    def _get_page(self, url):
        # Make a GET request to fetch the raw HTML content
        response = requests.get(
            url,
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
        )
        # Check the response status code
        if response.status_code == requests.codes.ok:
            return response
        else:
            response.raise_for_status()

    def _tag_dataset(self, df, source):
        df['source'] = source
        df['date_accessed'] = datetime.now()
        return df

    def _clean_book_titles(self, df, title_column, fixes=None):
        # Drop rows with no book title
        df = df.dropna(subset=[title_column])
        # Split books by : or ; to get title without subtitle
        # and replace -'s with spaces
        df['base_title'] = df[title_column].apply(
            lambda x: re.split(r"[:;]+", x)[0].lower().replace("-", " ")
        )
        # Remove non-alphanumeric chars
        df['base_title'] = df['base_title'].apply(
            lambda x: re.sub(r'[^A-Za-z0-9\s]+', '', x)
        )
        # Remove 'the ' prefix (as it is not consistently used across lists)
        df['base_title'] = df['base_title'].apply(
            lambda text: text[text.startswith('the ') and len('the '):]
        )
        # Apply manual fixes previously determined (using fuzzy-matching)
        # Update fixes if duplicates are found!
        if fixes:
            df['base_title'] = df['base_title'].apply(
                lambda x: fixes[x] if x in fixes.keys() else x
            )
        # Remove trailing whitespace
        df['base_title'] = df['base_title'].apply(
            lambda x: x.rstrip()
        )
        return df

    def _to_cache(self, key, value):
        self.cache[key] = value

    def _from_cache(self, key):
        return self.cache[key]
