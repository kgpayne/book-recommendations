import re
import json
import requests
import pandas as pd
from bs4 import BeautifulSoup

from .base import BookList
from .book_corrections import DS_TITLE_CORRECTIONS

columns = [
    'title', 'title_without_series', 'author_name', 'read_at', 'date_added',
    'isbn', 'isbn13', 'goodreads_book_id', 'TGN_review_articleURL'
]


class GatesNotes(BookList):

    def __init__(self, cache=None):
        self.url = "https://www.gatesnotes.com/Books#BooksRead"
        super().__init__(
            url=self.url,
            source_key='gates-notes',
            cache=cache
        )

    def _extract_book_recommendations(self, soup):
        regex = r"var html = template\((.*?)\);"
        scripts  = soup.find_all("script")
        found = []
        for script in scripts:
            matches = re.finditer(regex, script.text, re.MULTILINE)
            for _, match in enumerate(matches, start=1):
                found.append(match.group(1))
        found = [
            json.loads(match) for match in found
            if match and not match == 'data'
        ]
        books = []
        for match in found:
            books.extend(
                match.get('GoodReadsBookItemsList', [])
            )
        df = pd.DataFrame(books)
        return df[columns]

    def _get_gates_book(self, url, base='https://www.gatesnotes.com/'):

        if url:

            url = base + url
            page = self._get_page(url)
            soup = BeautifulSoup(page.text, "lxml")
            book = soup.find('div', class_="TGN_site_Article_body")
            text = book.find_all(text=True)

            output = ''
            blacklist = [
                '[document]',
                'noscript',
                'header',
                'html',
                'meta',
                'head',
                'input',
                'script',
                'style'
            ]

            for t in text:
                if t.parent.name not in blacklist:
                    output += '{} '.format(t)

            return output

    def _get_book_notes(self, df):
        df['review_text'] = df['TGN_review_articleURL']\
            .apply(lambda x: self._get_gates_book(x))
        return df

    def _get(self, get_book_notes=False):
        page = self._get_page(self.url)
        soup = BeautifulSoup(page.text, "lxml")
        df = self._extract_book_recommendations(soup)
        if get_book_notes:
            df = self._get_book_notes(df)
        return self._tag_dataset(df, 'gates-notes')
