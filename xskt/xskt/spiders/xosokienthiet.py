import scrapy
import calendar
import datetime

from scrapy.spiders import CrawlSpider
from ..items import XsktItem

def get_total_date_month(year, month):
    now = datetime.datetime.now()
    total_date = calendar.monthrange(year, month)[1]

    if year == now.year and month == now.month and now.day < total_date:
        return now.day

    return total_date

class SoxokienthietSpider(CrawlSpider):
    name = 'xosokienthiet'
    allowed_domains = ['xskt.com.vn']

    start_urls = []

    month_to_scrap = 5
    year_to_scrap = 2018
    total_date = get_total_date_month(year_to_scrap, month_to_scrap)

    for i in range(1, total_date):
        start_urls.append('http://xskt.com.vn/ket-qua-xo-so-theo-ngay/mien-nam-xsmn/'
                          '{0}-{1}-{2}.html'.format(i, month_to_scrap, year_to_scrap))

    def parse(self, response):
        xs_item = XsktItem()
        tmp_data = {}
        data_resp = scrapy.Selector(response)

        xs_item['xs_info'] = [
            # Thứ
            data_resp.xpath("//table[@id='MN0']/tr/th[1]/a/text()").extract_first(),
            # Ngày tháng
            data_resp.xpath("//table[@id='MN0']/tr/th[1]/text()").extract_first(),
            self.year_to_scrap
        ]

        for i in range(2, 5):
            # Các tỉnh trong bảng xổ số
            tmp_location = data_resp.xpath("//table[@id='MN0']/tr/th[{0}]/a/text()".format(i)).extract_first()
            tmp_data[tmp_location] = {}

            for j in range(2, 11):
                # Cột các giải từ giải 8 đến giải đặc biệt
                tmp_giai = data_resp.xpath("//table[@id='MN0']/tr[{0}]/td[1]/text()".format(j)).extract_first()
                # Các số trúng thưởng trong cột theo tỉnh
                tmp_number = data_resp.xpath("//table[@id='MN0']/tr[{0}]/td[{1}]//text()".format(j, i)).extract()
                tmp_data[tmp_location][tmp_giai] = ", ".join(tmp_number)

        xs_item['xs_data'] = tmp_data

        yield xs_item
