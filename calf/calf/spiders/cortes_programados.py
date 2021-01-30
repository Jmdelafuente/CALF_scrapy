import blosc
import numpy as np
import scrapy
import urllib.request

class CortesProgramadosSpider(scrapy.Spider):
    name = 'cortes_programados'
    allowed_domains = ['cooperativacalf.com.ar/category/cortes-programados']
    start_urls = ['http://www.cooperativacalf.com.ar/category/cortes-programados/']

    def parse(self, response):
        for article in response.css("article.post"):
            for image in article.css("img.attachment-post-thumbnail"):
                with urllib.request.urlopen(image.xpath('@src').get()) as img_res:
                    image_array = np.asarray(
                        bytearray(img_res.read()), dtype=np.uint8)
                    compressed_img = blosc.pack_array(image_array)
                yield {
                    'id': article.attrib['id'],
                    'src': image.xpath('@src').get(),
                    'img': compressed_img
                }
