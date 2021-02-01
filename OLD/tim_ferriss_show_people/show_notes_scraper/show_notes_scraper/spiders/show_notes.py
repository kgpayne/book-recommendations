# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class ShowNotesSpider(CrawlSpider):
    name = 'show_notes'
    allowed_domains = ['tim.blog']
    start_urls = ['https://tim.blog/category/the-tim-ferriss-show/']

    def parse(self, response):
        show_note_links = response.xpath("//h2[@class='entry-title']/a/@href").getall()
        for url in show_note_links:
            yield scrapy.Request(url, callback=self.parse_show_notes)

        next_page = response.xpath("//nav[@class='navigation pagination']/div[@class='nav-links']/a[contains(@class, 'next')]/@href").get()
        if next_page is not None:
            yield scrapy.Request(next_page, callback=self.parse)

    def parse_show_notes(self, response):
        item = {}
        item['date_published'] = response.xpath("//time[@class='entry-date published']/@datetime").get()
        item['title'] = response.xpath("//h1[@class='entry-title']/text()").get()
        item['people_mentioned'] = [
            self.parse_person_mentioned(mention)
            for mention in
            # Use `translate()` for case-insensitive contains.
            response.xpath(
                "//h3[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'people mentioned')]/following-sibling::ul[1]/li/a"
            )
        ]
        item['url'] = response.url
        return item

    def parse_person_mentioned(self, mentioned):
        mention = {}
        mention['name'] = mentioned.xpath('text()').get() or mentioned.xpath('span/text()').get()
        mention['url'] = mentioned.xpath('@href').get()
        return mention
