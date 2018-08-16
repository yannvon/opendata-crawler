import scrapy
from scrapy.http import Request
import os


class QuotesSpider(scrapy.Spider):
    name = "classified"
    # allowed_domains = ["www.opendata.swiss"]

    def start_requests(self):
        base_url = 'https://opendata.swiss/en/dataset?res_format=CSV&page='
        urls = [base_url + str(i) for i in range(1, 44)]

        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        for href in response.css('h3.dataset-heading a::attr(href)').extract():
            print("Href found: ", href)
            yield Request(
                url=response.urljoin(href),
                callback=self.parse_article
            )

    def parse_article(self, response):
        # categories = response.css('a[href$="*/en/group/*"]::text').extract()
        categories = response.css('a[href^="/en/group/"]::attr(href)').extract()
        print("Categories found: ", categories)

        for href in response.css('li.resource-item a[href$=".csv"]::attr(href)').extract():
            print("Article found: ", href)
            yield Request(
                url=response.urljoin(href),
                callback=self.save_csv, meta={'cat':  categories}
            )

    def save_csv(self, response):
        path = response.url.split('/')[-1]
        print("CSV found: ", path)

        for dir in response.meta.get('cat'):
            rel_path = 'data/' + dir.split('/')[-1] + '/' + path
            self.logger.info('Saving CSV %s', path)
            os.makedirs(os.path.dirname(rel_path), exist_ok=True)
            with open(rel_path, 'wb') as f:
                f.write(response.body)



