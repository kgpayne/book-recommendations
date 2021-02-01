import re
import pandas as pd
from bs4 import BeautifulSoup

from .base import BookList
from .book_corrections import TF_TITLE_CORRECTIONS


class TimFerrissShow(BookList):

    def __init__(self, cache=None):
        self.url = "https://www.booksoftitans.com/list"
        super().__init__(
            url=self.url,
            source_key='tim-ferriss-show',
            cache=cache
        )

    def _extract_book_recommendations(self, soup):
        table = soup.find('table', id="tablepress-3")
        dfs = pd.read_html(str(table))
        if len(dfs) == 1:
            return dfs[0]
        else:
            raise ValueError(
                "Unexpected number of tables found in Soup. Website "
                "may have changed formatting/structure."
            )

    def _flatten_duplicate_recommendations(self, df):
        flat = []
        for base_title, grp in df.groupby('base_title'):
            flat.append(
                {
                    'base_title': base_title,
                    'title': list(grp['Book'].unique()),
                    'author': list(grp['Author'].unique()),
                    'recommended_by': list(grp['Recommended By'].unique()),
                    'podcast_no': list(grp['Podcast #'].unique())
                }
            )
        return pd.DataFrame(flat)

    def _get(self, fixes=TF_TITLE_CORRECTIONS):
        page = self._get_page(self.url)
        soup = BeautifulSoup(page.text, "lxml")
        df = self._extract_book_recommendations(soup)
        df = self._clean_book_titles(df, 'Book', fixes)
        df = self._flatten_duplicate_recommendations(df)
        return self._tag_dataset(df, self.source_key)
