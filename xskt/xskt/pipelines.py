# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from sqlite3 import dbapi2 as sqlite

class XsktPipeline(object):
    def __init__(self):
        self.connection = sqlite.connect('./xs_database.db')
        self.cursor = self.connection.cursor()
        self.cursor.execute('CREATE TABLE IF NOT EXISTS kq_xs '
                            '(id INTEGER PRIMARY KEY, xs_thu VARCHAR(80),'
                            'xs_ngay_thang VARCHAR(80), xs_nam VARCHAR(80), xs_data TEXT)')

    def process_item(self, item, spider):
        self.cursor.execute("select * from kq_xs where xs_thu=? and xs_ngay_thang=? and xs_nam=?", (item['xs_info'][0], item['xs_info'][1], item['xs_info'][2]))

        result = self.cursor.fetchone()
        if not result:
            self.cursor.execute(
                "insert into kq_xs (xs_thu, xs_ngay_thang, xs_nam, xs_data) values (?, ?, ?, ?)",
                (item['xs_info'][0], item['xs_info'][1], item['xs_info'][2], str(item['xs_data'])))

            self.connection.commit()

        return item
