import requests
import pandas as pd
from bs4 import BeautifulSoup

from .base import BookList


class ThreeBooks(BookList):

    def __init__(self, cache=None):
        self.url = "https://www.3books.co/the-top-1000"
        super().__init__(
            url=self.url,
            source_key='three-books',
            cache=cache
        )

    def _extract_book_recommendations(self, soup):
        books = []
        allbooks = soup.find('div', class_="sqs-block-content")
        for book in allbooks.find_all('h3'):
            links = book.find_all('a')
            if len(links) > 1:
                title = links[0].get_text(strip=True)
                chapter = links[1].get_text(strip=True)
            else:
                title = links[0].get_text(strip=True)
                chapter = None

            author = book.get_text(strip=True).split(title)[-1].lower().replace('by ', '')
            if chapter:
                author = author.replace(f'{chapter}', '')
            books.append(
                {
                    'full_entry':book.text,
                    'title':title.lower(),
                    'blog_chapter':chapter,
                    'author':author
                }
            )
        return books

    def _get(self):
        page = self._get_page(self.url)
        soup = BeautifulSoup(page.text, "lxml")
        books = self._extract_book_recommendations(soup=soup)
        df = pd.DataFrame(books)
        return self._tag_dataset(df, '3-books')
