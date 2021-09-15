#!/usr/bin/env python

import time
import asyncio
import sys
from aiohttp import ClientConnectionError

from db import SQLite
from parser import Parser
from settings import DEBUG, RATING, BRANDS_URLS, SHOPS


# support print to testing full app functionality, include 'db' и 'parser' modules
if DEBUG:
    tic = lambda: time.time()
    now = tic()
    tac = lambda: '{:.2f}sec'.format(time.time() - now)
    print('Debug is on.')


class Main:
    """
    Starting class to operate constistency, updating and filling of the database by monitoring kant.ru on availability
    of running shoes items and its prices changes.
    __init__() connect to database and configures partial work (an optional) to add and change functionality for working
    methods.
    update_products_table() fills the 'products' table from db and monitors its consistency
    update_prices_table() fills and monitors 'prices' table
    update_instock_table() fills and monitors 'instock_nagornaya' table
    """

    def __init__(self, brand=None):

        self.url_list = BRANDS_URLS  # used all running brands (urls) to parsing
        self.from_parse_main = list()  # cached, if disconnect cases is often
        self.max_pagination = 30  # max pagination of each brand
        self.brand_ = brand  # partial working with db, don't used other data from db to correct full data consistency

        self.loop = asyncio.get_event_loop()  # start async event loop
        self.db = SQLite()  # connect to db

        self.set_brand_parameter(brand)  # see next

    def brand_getter(self):

        return self.brand_

    def brand_setter(self, name):

        self.set_brand_parameter(name)

    def brand_deleter(self):

        del self.db  # close database
        self.brand_, self.brand = None, None

    brand = property(brand_getter, brand_setter)

    def set_brand_parameter(self, brand):
        """
        Forwarding 'brand' argument from main.Main() to db.SQLite() to operate part of data from database.
        'brand' need to test part of database (don't using full data) with full work process, operate by all methods
        For example:
        page = Main(brand='Adidas')  # from __init__()
        page.brand = 'Adidas'        # or from attribute
        page.update_products_table()
        To full functionality for testing and updating any Main() methods with only one brand 'Adidas',
        without using full data.
        """

        if brand is not None:
            self.brand_ = brand  # set double parameters: self.brand_ and self.brand (property)
            # forward naming to SQLite().brand
            if self.db is not None:
                self.db.brand = brand
            # working only with one brand (not full running shoes urls)
            name = brand.lower()
            # find unic url by one unic brand name
            # use split() by '/' and '-' (from url string) to use correct url by brand name
            self.url_list = [url for url in self.url_list if
                             list(filter(
                                 lambda x: x == name,
                                 [i for i in url.split('/') if '-' not in i] + \
                                 [i.partition('-')[2] for i in url.split('/') if '-' in i]
                             ))]

    def update_products_table(self):
        """
        Create new items to 'products' table to database and update rating to items, which doesn't in stock
        """

        if not self.db:  # if not db connection
            return None

        # support to develop, if True
        if DEBUG:
            now = tic()
            tac = lambda: '{:.2f}sec'.format(time.time() - now)
            print('\r\n> Start update_products_table..')

        # load urls from www.kant.ru
        if not self.from_parse_main:  # if not cached from internet re- connection (mobile connection, as usual)
            self.from_parse_main = self.loop.run_until_complete(Parser.parse_main(self.url_list, self.max_pagination))
        unic_urls = set(self.from_parse_main)  # unic urls, exclude doubles items from list
        url_from_db = set(self.db.get_products_urls())  # get urls to check its availability
        check_urls = unic_urls - url_from_db  # check urls, not in stock from 'products' table
        urls_not_instock = url_from_db - unic_urls # out of stock urls
        url_from_db_small_rate = set(self.db.get_products_urls_rating_below_normal())  # get only small rate
        urls_to_normal_rate = url_from_db_small_rate & check_urls  # update to normal rate: RATING
        new_urls = list(check_urls - url_from_db_small_rate)  # set new rate: RATING
        new = list() # products from new_urls
        if urls_not_instock:  # change rating to 1 for not in stock items
            self.db.update_products_rating_to_1(urls_not_instock)
        if urls_to_normal_rate:  # change rating to normal (settings.RATING) if item is available again
            self.db.update_products_rating_to_normal(urls_to_normal_rate)
        if new_urls:  # add to 'products' new items
            new = self.loop.run_until_complete(Parser.parse_details(new_urls))  # item description by it urls
            if new:
                self.db.to_products(new)
            else:
                if DEBUG:
                    print('without exec Parser.parse_details')
        if DEBUG:
            print('\tfrom db, rate {}: {}'.format(RATING, len(url_from_db)))
            print('\tfrom kant.ru: ', len(unic_urls))
            print('\tNot in stock:', len(urls_not_instock), urls_not_instock)
            print('\tUpdate rate 1 to normal:', len(urls_to_normal_rate), urls_to_normal_rate)
            print('\tNew:', len(new), new)
            print('> End update_product_table {}.'.format(tac()))

        return True  # if all ok

    def update_prices_table(self):
        """
        set new prices to new items in 'prices' from new items in 'products' and update prices 'prices' if chanched
        """

        if not self.db:  # if not db connection
            return None

        # support dev
        if DEBUG:
            now = tic()
            tac = lambda: '{:.2f}sec'.format(time.time() - now)
            print('\r\n> Start update_prices_table..')

        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())  # time to update stamp
        products = self.db.get_products_code_url()  # get pairs code and url from 'products'
        if not products:
            if DEBUG: print('No items in products table!')
            return False
        prod_codes = [code for code, url in products]  # only codes
        # get prices by codes in 'products' from 'prices' table with max rate
        prices_from_db = self.db.get_last_update_prices()  # [(code, price, timestamp, rating), (code, ...]
        prices_codes = [code for (code, price, time_, rating) in prices_from_db]  # only codes

        # new codes set to 'prices': code, price, timestamp, RATING
        new = set(prod_codes) - set(prices_codes)  # new shoes, prices not define, need parse
        if new:
            new_codes_urls = [(code, url) for (code, url) in products if code in new]  # get pairs code: url for parsing
            new_codes_prices = self.loop.run_until_complete(Parser.parse_price(new_codes_urls))  # code: price for items
            # if price == 0, set rate == 1.
            # starting rate for new normal price == RATING
            solution_new_list = [(code, price, timestamp, (lambda i: RATING if i > 0 else 1)(price))
                                 for (code, price) in new_codes_prices
                                 ]
            if solution_new_list:
                self.db.to_prices(solution_new_list)
                if DEBUG: print('new prices to db: ', len(solution_new_list), *solution_new_list, timestamp)

        # if item was not in stock and now update yet, rate 1+1 = 2
        old = set(prices_codes) & set(prod_codes)  # check existing or update for new price find, increment rate +1
        if old:
            old_codes_urls = [(code, url) for (code, url) in products if code in old]
            updated_codes_prices = self.loop.run_until_complete(Parser.parse_price(old_codes_urls))
            solution_old_list = list()
            for upd_code, upd_price in updated_codes_prices:  # iterate for loaded data from site
                for db_code, db_price, time_, rating in prices_from_db:   # check equal prices from db and site
                    if upd_code == db_code and upd_price != db_price:  # code is equal. Prices is updated?
                        if upd_price != 0:  # item in stock and price real is update
                            solution_old_list.append((upd_code, upd_price, timestamp, rating+1))  # set new price toitem
                        else:  # price is 0 or price column not found in prices card
                            solution_old_list.append((upd_code, upd_price, timestamp, 1))  # set not in stock price
                        break
            if solution_old_list:  # set new price and rate conditions-- update existing items
                self.db.to_prices(solution_old_list)
                if DEBUG: print('\tupdate prices in db: ', len(solution_old_list), *solution_old_list, timestamp)

        if DEBUG: print('> End update_prices_table on {}.'.format(tac()))

        return True  # if all ok

    def update_instock_table(self):
        """
        Set new instock availability of each size of each item, update existing availability and set to 0 not in stock
        items.
        Working table: 'instock_nagornaya'
        """

        if not self.db:  # if not db connection
            return None

        if DEBUG:
            now = tic()
            tac = lambda: '{:.2f}sec'.format(time.time() - now)
            print('\r\n> Start update_instock_tables..')

        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())  # now time to update db timestamps
        codes_urls = self.db.get_products_code_url()
        if not codes_urls:  # table is empty. First run 'Main.update_products_table()'
            if DEBUG: print('No items in products table!')
            return False
        codes = [i[0] for i in codes_urls]
        instock_codes = [int(i[1].split('/')[5]) for i in codes_urls]  # unic code from url
        pair_codes = list(zip(codes, instock_codes))  # code, unic_code_from_url

        # load from kant.ru and set availability (size and its quantity) to loaded_instock, for example:
        #   shop            code    size, count,    time,         rating
        # {'nagornaya':
        #               {12345678:
        #                           (11.5, 3, 2021-06-21 23:59:00, 4) }}
        loaded = self.loop.run_until_complete(Parser.parse_available(pair_codes))  # load from www.kant.ru
        loaded_instock = dict()
        shop = SHOPS[0]
        loaded_instock[shop] = dict()
        for code, instock in loaded:
            if shop in instock.keys():
                loaded_instock[shop][code] = list()
                for sizes in instock[shop]:
                    loaded_instock[shop][code].append((float(sizes[0]), sizes[1], timestamp, RATING))

        # load from db and set  availability (size and its quantity) to last_update_instock, for example:
        #   shop            code    size, count,    time,         rating
        # {'nagornaya':
        #               {12345678:
        #                           (11.5, 3, 2021-06-21 23:59:00, 4) }}
        last_update_instock = dict()
        last_update_instock[shop] = dict()
        for code, size, count, time_, rate in self.db.get_instock_nagornaya_last_update():
            if code not in last_update_instock[shop].keys():
                last_update_instock[shop][code] = list()
            last_update_instock[shop][code].append((float(size), count, time_, rate))

        # New items add to table istantly without any check
        new = list()
        for code in loaded_instock[shop].keys():
            if code not in last_update_instock[shop].keys():
                new.extend([(code, *i) for i in loaded_instock[shop][code]])

        # Check items for consistency already available
        updated = list()
        not_instock = list()
        for code in last_update_instock[shop].keys():
            if code in loaded_instock[shop].keys():  # if codes from kant.ru and database matched
                for size, count, timestmp, rate in last_update_instock[shop][code]:  # check database
                    for size_, count_, timestmp_, rate_ in loaded_instock[shop][code]:  # check kant.ru
                        if size == size_ and count != count_:  # if sizes matched and count is updated (not matched)
                            updated.append((code, size_, count_, timestamp, rate + 1))
                            break
                # needs lambda to equal types to correct working with values in future
                last_update_sizes = set(map(lambda x: float(x[0]), last_update_instock[shop][code]))  # unic sizes if item from database
                loaded_sizes = set(map(lambda x: float(x[0]), loaded_instock[shop][code]))  # unic sizes of item from kant.ru
                not_instock_sizes = last_update_sizes - loaded_sizes
                new_sizes = loaded_sizes - last_update_sizes
                not_instock.extend([(code, value[0], 0, timestamp, value[3]+1)
                                    for value in last_update_instock[shop][code] if value[0] in not_instock_sizes])
                new.extend([(code, value[0], value[1], timestamp, RATING)
                            for value in loaded_instock[shop][code] if value[0] in new_sizes])

        # items codes with not in stock from database
        not_instock_codes = [i[0] for i in self.db.get_instock_codes_with_0_count()]
        # unic items codes not in stock from kant.ru, which not in database
        not_instock = [item for item in not_instock if item[0] not in not_instock_codes]
        if new:  # add to db new items
            self.db.to_instock_nagornaya(new)
        if updated:  # add updated items to db
            self.db.to_instock_nagornaya(updated)
        if not_instock:  # add not in stock (count=0 available) items to db
            self.db.to_instock_nagornaya(not_instock)

        if DEBUG:
            print('\tnew: ', len(new), *new)
            print('\tupdated: ', len(updated), *updated)
            print('\tnot in stock: ', len(not_instock), *not_instock)
            print('> End update_instock_tables on {}.'.format(tac()))

        return True  # if that's all ok


def manager(load_prods=False, load_prices=False, load_instock=False):
    """
    Manager to operate updating 3 functionality for tables by 3 methods
    Main.update_products_table, Main.update_prices_table, Main.update_instock_table
    with call command line: python main.py with sys.args
    """

    try_count = 3  # how many attempts to load page to parse
    load_prods = load_prods
    load_prices = load_prices
    load_instock = load_instock
    args = sys.argv

    if len(args) > 1:
        for argv in args:
            argv = argv.lower()
            if argv == 'products':
                load_prods = True
            if argv == 'prices':
                load_prices = True
            if argv == 'instock':
                load_instock = True

    page = Main()

    if hasattr(page, 'db'):  # normal connect to db
        for i in range(try_count):
            try:
                if load_prods:
                    load_prods = not page.update_products_table()
                if load_prices:
                    load_prices = not page.update_prices_table()
                if load_instock:
                    load_instock = not page.update_instock_table()
            except ClientConnectionError as err:  # as usual may be on mobile connect, local testing, not production
                print('ConnectionError. Reconnect..')
                time.sleep(20)


if __name__ == "__main__":

    if DEBUG:
        now = tic()

    manager()

    # manager(True)  # update 'products' table to database
    # manager(False, True)  # update 'prices'
    # manager(False, False, True)  # update 'instock_nagornaya'
    # manager(True, True, True)  # update all working tables

    if DEBUG:
        print(tac(), 'worked app.')
