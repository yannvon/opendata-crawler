import scrapy
from scrapy.http import Request
import os


class QuotesSpider(scrapy.Spider):
    name = "opendata"
    # allowed_domains = ["www.opendata.swiss"]

    def start_requests(self):
        base_url = 'https://opendata.swiss/en/dataset?res_format=CSV&page='
        urls = [base_url + str(i) for i in range(1, 44)]

        alt_url = [
            'https://opendata.swiss/en/dataset?res_format=CSV&page=1',
            'https://opendata.swiss/en/dataset?res_format=CSV&page=2',
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for href in response.css('h3.dataset-heading a::attr(href)').extract():
            print("Href found: %s", href)
            yield Request(
                url=response.urljoin(href),
                callback=self.parse_article
            )

    def parse_article(self, response):
        filter_title = ''.join(filter(lambda s: s != '/n', title))

        for href in response.css('li.resource-item a[href$=".csv"]::attr(href)').extract():
            print("Article found: %s", href)
            yield Request(
                url=response.urljoin(href),
                callback=self.save_csv, meta={'title': filter_title}
            )

    def save_csv(self, response):
        path = response.url.split('/')[-1]
        rel_path = 'data/' + response.meta.get('title') + '/' + path
        print("CSV found: %s", path)
        self.logger.info('Saving CSV %s', path)
        os.makedirs(os.path.dirname(rel_path), exist_ok=True)
        with open(rel_path, 'wb') as f:
            f.write(response.body)



