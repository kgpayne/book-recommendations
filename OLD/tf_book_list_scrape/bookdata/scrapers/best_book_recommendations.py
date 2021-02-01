import json
import requests
import xmltodict
import pandas as pd

from .base import BookList
from datetime import datetime


class BestBookRecommendations(BookList):

    def __init__(self, cache=None):
        source_key = 'best-book-recommendations'
        self.url = "https://www.mostrecommendedbooks.com/sitemap.xml"
        self.base_json_url = "https://www.mostrecommendedbooks.com/page-data/{key}/page-data.json"
        super().__init__(
            url=self.url,
            source_key=source_key,
            cache=cache
        )

    def _extract_json_path_keys(self, sitemap_xml):
        sitemap = xmltodict.parse(
            sitemap_xml
        )
        # Split key from sitemap blog URL
        return [
            site['loc'].split('/')[-1]
            for site in sitemap['urlset']['url']
            if site['loc'].split('/')[-1]
        ]

    def _extract_book_recommendations(self, json_response):
        return json_response['result']['data']['recommenderBooks']['recommenderBooks']

    def _get_raw_json_data(self, keys, base_url):
        recommendations, errors = [], []
        for key in keys:
            try:
                page = self._get_page(
                    base_url.format(key=key)
                ).json()
                rec = self._extract_book_recommendations(page)
                recommendations.extend(rec)
            except (requests.exceptions.HTTPError, KeyError, TypeError):
                errors.append(key)
        return recommendations, errors

    def _remove_duplicates(self, df):
        df['recommenders'] = df['recommenders'].apply(lambda x: [y['name'] for y in x])
        df = df.groupby(['title', 'subtitle', 'author'])\
            .agg({'recommenders': 'sum'}).reset_index()
        df['recommenders'] = df['recommenders'].apply(lambda x: list(set(x)))
        df['recommendations'] = df['recommenders'].apply(lambda x: len(x))
        df.sort_values('recommendations', ascending=False, inplace=True)
        return df

    def _get(self):
        sitemap_xml = self._get_page(self.url).text
        keys = self._extract_json_path_keys(sitemap_xml)
        books, errors = self._get_raw_json_data(keys, self.base_json_url)
        print(errors)
        df = pd.DataFrame(books)
        df = self._remove_duplicates(df)
        return self._tag_dataset(df, self.source_key)
