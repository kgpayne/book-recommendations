# Book Recommendations

Scraping book recommendations for fun and profit.

In general, this follows the process in Peter Christens' excellent book
[Data Matching: Concepts and Techniques for Record Linkage, Entity Resolution and Duplicate Detection](https://www.google.co.uk/books/edition/Data_Matching/LZrT6eWf9NMC) for both deduplication and linkage.

## Package Overview

### book-tools

A collection of tools for use across sources and projects.

### tim-ferriss-show

Book recommendations from [The Tim Ferriss Show](https://tim.blog/podcast/),
helpfully collated by [The Books of Titans Project](https://www.booksoftitans.com/).

- **tap-books-of-titans** is a scraper for collecting the full list of recommendations.
- **clean-books-of-titans** contains notebooks for deduplicating the raw list.

### record-linkage

An attempt at joining Books of Titans data with Goodreads bookshelf data.
