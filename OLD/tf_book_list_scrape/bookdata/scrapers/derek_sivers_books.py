import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

from .base import BookList
from .book_corrections import DS_TITLE_CORRECTIONS


class DerekSiversBooks(BookList):

    def __init__(self, cache=None):
        self.url = "https://sivers.org/book"
        super().__init__(
            url=self.url,
            source_key='derek-sivers-books',
            cache=cache
        )

    def _extract_book_notes(self, soup):
        allbooks = soup.find('section', id="allbooks")
        books = []
        for book in allbooks.find_all('div', class_='abook'):
            books.append(
                {
                    'date_read': book['data-date'],
                    'rating': book['data-rating'],
                    'title': book['data-title']
                }

            )
        return pd.DataFrame(books)

    def _extract_authors(self, df):
        df['author'] = df['title'].apply(lambda x: x.split('- by ')[-1])
        return df

    def _get(self, fixes=DS_TITLE_CORRECTIONS):
        page = self._get_page(self.url)
        soup = BeautifulSoup(page.text, "lxml")
        df = self._extract_book_notes(soup)
        df['base_title'] = df['title'].apply(lambda x: ''.join(x.split(' - ')[:-1]))
        df = self._clean_book_titles(df, 'base_title', fixes)
        df = self._extract_authors(df)
        return self._tag_dataset(df, 'derek-sivers-books')
