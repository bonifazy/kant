import sqlite3
import os.path
from pathlib import Path

from settings import SHOPS, DB_NAME, RATING, DEBUG


class SQLite:
    """
    Class for communication with database by main.py, parser.py, tests.py modules
    Names manifest:
    Methods startswith (examples):
    -- to_... : to add new data to tables (only write);
    -- get_... : to get the values/ data (only read);
    -- update_... : to update already recorded data (only rewrite);
    -- test_... : to test structure and consistency tables and data (only read);
    -- export_... serialized and export items card description to json, xml, csv
    """

    # protection from double connectors to database. sqlite3 module don't support 2 and more parallel connections
    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(SQLite, cls).__new__(cls)

        return cls.instance

    def __init__(self):
        # to real path to 'db.sqlite3' file. If database file should be a parent dir,
        # set parent_dir = Path(__file__).resolve().parent.parent
        parent_dir = Path(__file__).resolve().parent
        self.db = os.path.join(parent_dir, DB_NAME)  # path + file with any OS
        if os.path.isfile(self.db):
            self.conn = sqlite3.connect(self.db)
            self.cur = self.conn.cursor()
            if DEBUG:
                print(self.db, '\nDatabase is working.')
        else:
            self.db = None  # re- forward to main.Main(). Look functionality on Main() class
            if DEBUG:
                print('No access to database. :-(\nEdit DB_NAME in settings.py or edit SQLite.parent_dir correctly.')
        self.brand = None  # may be this parameter will be replaced later from main.Main.__init__()
        # if self.brand was recieved from main.Main(brand='your_working_brand_name'), in each methods
        # from this SQLite() class, request to database will be only apply
        # to a part of all data from db, within one brand name

    def __del__(self):

        self.close()

    def close(self):

        if hasattr(self, 'cur') and hasattr(self, 'conn'):
            self.cur.close()
            self.conn.close()

            if DEBUG:
                print('Close database.')

    def to_products(self, products: list):
        """
        Append main items description that will not change in the future (except 'rating' column for not in stock items)
        type of values:             (int,   str,  str,   str, str, str, str,    int,  str, str,      str, str, int, str)
        """

        sql = "INSERT INTO products (code, brand, model, url, img, age, gender, year, use, pronation, article, " \
              "season, rating, timestamp) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
        self.cur.executemany(sql, products)
        self.conn.commit()
        return self.cur.rowcount

    def to_prices(self, prices: list):
        """
        Update price if existing items or add new item price to 'prices' table to database
        """

        sql = "INSERT INTO prices (code_id, price, timestamp, rating) VALUES (?,?,?,?);"
        self.cur.executemany(sql, prices)
        self.conn.commit()
        return self.cur.rowcount

    def to_instock(self, shop, instock: list):
        """
        Update each size (its count availability and its update) of each item ('code_id' column) from
        'instock_nagornaya' table
        """

        table = ''
        if shop == SHOPS[0]:
            table = 'instock_nagornaya'
        elif shop == SHOPS[1]:
            table = 'instock_timiryazevskaya'
        elif shop == SHOPS[2]:
            table = 'instock_teply_stan'
        elif shop == SHOPS[3]:
            table = 'instock_altufevo'

        if table:
            sql = "INSERT INTO '{}' (code_id, size, count, timestamp, rating) VALUES (?,?,?,?,?);".format(table)
        self.cur.executemany(sql, instock)
        self.conn.commit()
        return self.cur.rowcount

    def get_products_urls_rating_below_normal(self):
        """
        Return urls of not in stock items or has just appeared in stock item (after its rating=1 before)
        """

        if self.brand is not None:
            sql = "SELECT url FROM products WHERE brand='{}' AND rating < {};".format(self.brand, RATING)
        else:
            sql = "SELECT url FROM products WHERE rating < {};".format(RATING)
        self.cur.execute(sql)
        urls = [i[0] for i in self.cur.fetchall()]
        return urls

    def get_products_codes_for_urls(self, urls):
        """
        Get the products code from its link
        """

        self.cur.execute("SELECT code FROM products WHERE url IN ('{}');".format("','".join(urls)))
        codes = self.cur.fetchall()
        return codes

    def get_products_code_url(self):
        """
        Get pairs code (unic), link (unic) to operate (update or add new) 'prices' and 'instock_nagornaya' tables
        """

        if self.brand is not None:
            sql = "SELECT code, url FROM products WHERE brand = '{}';".format(self.brand)
        else:
            sql = "SELECT code, url FROM products;"
        self.cur.execute(sql)
        codes = self.cur.fetchall()
        return codes

    def get_products_urls(self):
        """
        Get the product link
        Need to operate 'products' table changes of items
        """

        if self.brand is not None:
            sql = "SELECT url FROM products WHERE brand='{}' AND rating >= {};".format(self.brand, RATING)
        else:
            sql = "SELECT url FROM products WHERE rating >= {};".format(RATING)
        self.cur.execute(sql)
        urls = [i[0] for i in self.cur.fetchall()]
        return urls

    def get_last_update_prices(self):
        """
        Get last update actual price of the product by its code
        """

        if self.brand is not None:
            sql = "SELECT prod.code, p.price, p.timestamp, p.rating " \
                "FROM prices AS p, products AS prod " \
                "ON prod.code = p.code_id " \
                "WHERE prod.brand = '{}' " \
                "GROUP BY p.code_id " \
                "ORDER BY -max(p.rating);".format(self.brand)
        else:
            sql = "SELECT code_id, price, timestamp, rating FROM prices GROUP BY code_id ORDER BY -max(rating);"
        self.cur.execute(sql)
        return self.cur.fetchall()

    def get_instock_last_update(self, shop):
        """
        Get actual info on the size and availability of the products
        """

        table = ''
        if shop == SHOPS[0]:
            table = 'instock_nagornaya'
        elif shop == SHOPS[1]:
            table = 'instock_timiryazevskaya'
        elif shop == SHOPS[2]:
            table = 'instock_teply_stan'
        elif shop == SHOPS[3]:
            table = 'instock_altufevo'

        if self.brand is not None and table:
            sql = "SELECT p.code, i.size, i.count, i.timestamp, i.rating " \
                  "FROM '{}' AS i, products AS p " \
                  "ON p.code=i.code_id " \
                  "WHERE p.brand = '{}' AND i.rating >= {} " \
                  "GROUP BY i.code_id, i.size " \
                  "ORDER BY -MAX(i.rating);".format(table, self.brand, RATING)
        elif self.brand is None and table:
            sql = "SELECT code_id, size, count, timestamp, rating " \
                  "FROM '{}' " \
                  "WHERE rating >= {} " \
                  "GROUP BY code_id, size " \
                  "ORDER BY -MAX(rating);".format(table, RATING)
        self.cur.execute(sql)
        return self.cur.fetchall()

    def get_instock_codes_with_0_count(self, shop):
        """
        Get products that are not in stock
        """

        table = ''
        if shop == SHOPS[0]:
            table = 'instock_nagornaya'
        elif shop == SHOPS[1]:
            table = 'instock_timiryazevskaya'
        elif shop == SHOPS[2]:
            table = 'instock_teply_stan'
        elif shop == SHOPS[3]:
            table = 'instock_altufevo'

        if self.brand is not None and table:
            sql = "SELECT i.code_id " \
                  "FROM products AS p, '{}' AS i " \
                  "WHERE p.brand = '{}' AND i.count = 0 " \
                  "GROUP BY i.code_id;".format(
                table, self.brand)
        elif self.brand is None and table:
            sql = "SELECT code_id FROM '{}' WHERE count = 0 GROUP BY code_id;".format(table)
        self.cur.execute(sql)

        return self.cur.fetchall()

    def update_products_rating_to_0(self, urls):
        """
        Sets low rating for items that is not in stock
        """

        sql = "UPDATE products SET rating = 0 WHERE url IN ('{}');".format("','".join(urls))
        self.cur.execute(sql)
        self.conn.commit()
        return self.cur.fetchall()

    def update_products_rating_to_normal(self, urls):
        """
        Set normal rating for items that have become available again
        """

        sql = "UPDATE products SET rating = '{}' WHERE url IN ('{}');".format(RATING, "','".join(urls))
        self.cur.execute(sql)
        self.conn.commit()
        return self.cur.fetchall()

    def exe(self, sql):
        self.cur.execute(sql)
        self.conn.commit()
        return self.cur.fetchall()

    def test_products(self):
        sql = "SELECT code, brand, model, url, img, age, gender, year, use, pronation, article, season, timestamp, " \
              "rating FROM products WHERE rating = {} LIMIT 1;".format(RATING)
        self.cur.execute(sql)
        return self.cur.fetchone()

    def test_prices(self):
        self.cur.execute("SELECT code_id, price, timestamp, rating FROM prices WHERE rating={} LIMIT 1;".format(RATING))
        return self.cur.fetchone()

    def test_instock_nagornaya(self):
        sql = "SELECT code_id, size, count, timestamp, rating FROM instock_nagornaya WHERE rating={} LIMIT 1;".\
            format(RATING)
        self.cur.execute(sql)
        return self.cur.fetchone()

    def export_card_and_price(self, code=None):
        """
        Export to json, xml, csv formats. Used by Main.export()
        """

        # single card description by code of item
        if code is not None:
            sql = "SELECT p.code, p.model, p.brand, pri.price, p.url, p.img, p.age, p.gender, p.year, p.use, " \
                  "p.pronation, p.article, p.season " \
                  "FROM products AS p, prices AS pri " \
                  "ON p.code=pri.code_id " \
                  "WHERE code='{}' " \
                  "GROUP BY  pri.code_id " \
                  "HAVING MAX(pri.rating);".format(code)
            self.cur.execute(sql)

            return self.cur.fetchall()

        # multiple card description by brand
        if self.brand is not None:
            sql = "SELECT p.code, p.model, p.brand, pri.price, p.url, p.img, p.age, p.gender, p.year, p.use, " \
                  "p.pronation, p.article, p.season " \
                  "FROM products AS p, prices AS pri " \
                  "ON p.code=pri.code_id " \
                  "WHERE brand='{}' " \
                  "GROUP BY  pri.code_id " \
                  "HAVING MAX(pri.rating);".format(self.brand)
        else:
            # multiple card description of full database
            sql = "SELECT p.code, p.model, p.brand, pri.price, p.url, p.img, p.age, p.gender, p.year, p.use, " \
                    "p.pronation, p.article, p.season " \
                    "FROM products AS p, prices AS pri " \
                    "ON p.code=pri.code_id " \
                    "GROUP BY  pri.code_id " \
                    "HAVING MAX(pri.rating);"
        self.cur.execute(sql)
        return self.cur.fetchall()

    def export_available(self, code):
        """
        Additional description of product (by code) availability
        """

        available = dict()
        for shop in SHOPS:
            table = ''
            if shop == SHOPS[0]:
                table = 'instock_nagornaya'
            elif shop == SHOPS[1]:
                table = 'instock_timiryazevskaya'
            elif shop == SHOPS[2]:
                table = 'instock_teply_stan'
            elif shop == SHOPS[3]:
                table = 'instock_altufevo'

            sql = "SELECT size, count " \
                    "FROM {} " \
                    "WHERE code_id={} " \
                    "GROUP BY size " \
                    "HAVING MAX(rating) AND count <> 0;".format(table, code)
            self.cur.execute(sql)
            response = self.cur.fetchall()
            if response:
                available['_comment'] = 'format: offline_Moscow_store: {size_in_stock: its_count, next_size: its_count}'
                instock = {size: count for size, count in response}
                available[shop] = instock

            if not available:
                return None

        return available


if __name__ == '__main__':
    database = SQLite()
    if hasattr(database, 'conn'):
        print('Connection is open.')
    else:
        print('Problem to database connection.')  # close any third- party working SQLite clients and retry!
