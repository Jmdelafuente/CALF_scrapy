import scrapy
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.utils.project import get_project_settings
from spiders.cortes_programados import CortesProgramadosSpider
from twisted.internet import reactor


def run_crawl():
    runner = CrawlerRunner(get_project_settings())
    deferred = runner.crawl(CortesProgramadosSpider)
    deferred.addCallback(reactor.callLater, 21600, run_crawl)
    return deferred

if __name__ == "__main__":
    configure_logging({'LOG_FORMAT': '%(levelname)s: %(message)s'})
    run_crawl()
    reactor.run()