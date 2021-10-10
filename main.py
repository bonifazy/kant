#!/usr/bin/env python

import time
import asyncio
import sys
import os.path
from aiohttp import ClientConnectionError
from pathlib import Path

from db import SQLite
from parser import Parser
from settings import DEBUG, RATING, BRANDS_URLS, SHOPS


# support print to testing full app functionality, include 'db' and 'parser' modules
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
    update_instock_table() fills and monitors all 'instock_...' tables
    """

    def __init__(self, brand=None):

        self.url_list = BRANDS_URLS  # used all running brands (links) to parsing
        self.from_parse_main = list()  # cached, if disconnect cases is often
        self.max_pagination = 30  # max pagination of each brand
        self._brand = brand  # uses partial working with db without affecting all data to correct data consistency

        self.loop = asyncio.get_event_loop()  # start async event loop
        self.db = SQLite()  # connect to db

        self.set_brand_parameter(brand)  # see next

    def brand_getter(self):

        return self._brand

    def brand_setter(self, name):

        self.set_brand_parameter(name)

    def brand_deleter(self):

        del self.db  # close database
        self._brand, self.brand = None, None  # for use full url list: self.url_list

    brand = property(brand_getter, brand_setter, brand_deleter)  # convenient use partial working with db

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
            self._brand = brand  # set double parameters: self._brand and self.brand (property)
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
        if urls_not_instock:  # change rating to 0 for not in stock items
            self.db.update_products_rating_to_0(urls_not_instock)
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
            if urls_not_instock:
                print('\tNot in stock:', len(urls_not_instock), urls_not_instock)
            if urls_to_normal_rate:
                print('\tUpdate rate 1 to normal:', len(urls_to_normal_rate), urls_to_normal_rate)
            if new:
                print('\tNew:', len(new), new)
            print('> End update_product_table {}.'.format(tac()))

        return True  # if all ok

    def update_prices_table(self):
        """
        set new prices to new items in 'prices' from new items in 'products' and update prices 'prices', if chanched
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
            if DEBUG:
                print('No items in products table!')
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
            # starting rate for new normal price == RATING
            solution_new_list = [(code, price, timestamp, RATING) for (code, price) in new_codes_prices]
            if solution_new_list:
                self.db.to_prices(solution_new_list)
                if DEBUG:
                    print('new prices to db: ', len(solution_new_list), *solution_new_list)

        # update existing items if prices has been updated, increment rate + 1
        exist = set(prices_codes) & set(prod_codes)
        if exist:
            old_codes_urls = [(code, url) for (code, url) in products if code in exist]
            updated_codes_prices = self.loop.run_until_complete(Parser.parse_price(old_codes_urls))
            to_update = list()
            for upd_code, upd_price in updated_codes_prices:  # iterate for loaded data from kant.ru
                for db_code, db_price, _time, rating in prices_from_db:  # check equal prices from db and site
                    if upd_code == db_code and upd_price != db_price:  # code is equal. Prices is updated?
                        # item in stock and price real is update
                        to_update.append((upd_code, upd_price, timestamp, rating+1))  # set new price to item
                        break
            if to_update:  # set new price and rate conditions-- update existing items
                self.db.to_prices(to_update)
                if DEBUG:
                    print('\tupdate prices in db: ', len(to_update), *to_update)

        if DEBUG:
            print('> End update_prices_table on {}.'.format(tac()))

        return True  # if all ok

    def update_instock_table(self):
        """
        Set new instock availability of each size of each item, update existing availability and set to 0 not in stock
        items.
        Working tables: 'instock_nagornaya', 'instock_altufevo', 'instock_teply_stan', 'instock_timiryazevskaya'
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
        instock_codes = [int(i[1].split('/')[5]) for i in codes_urls]  # unic code from url, get numeric set from link
        pair_codes = list(zip(codes, instock_codes))  # pair: code, unic_code_from_url

        # load from kant.ru and set availability (size and its quantity) to loaded_instock, for example:
        #   shop            code    size, count,    time,         rating
        # {'nagornaya':
        #               {12345678:
        #                           (11.5, 3, 2021-06-21 23:59:00, 4) }}
        loaded = self.loop.run_until_complete(Parser.parse_available(pair_codes))  # load from www.kant.ru
        loaded_instock = {shop: dict() for shop in SHOPS}  # serialized to analyse with last_update_instock

        for code, instock in loaded:
            for shop in SHOPS:
                if shop in instock.keys():
                    loaded_instock[shop][code] = list()
                    for sizes in instock[shop]:
                        loaded_instock[shop][code].append((float(sizes[0]), sizes[1], timestamp, RATING))

        # load from db availability (size and its quantity) to last_update_instock, for example:
        #   shop            code    size, count,    time,         rating
        # {'nagornaya':
        #               {12345678:
        #                           (11.5, 3, 2021-06-21 23:59:00, 4) }}
        last_update_instock = {shop: dict() for shop in SHOPS}
        for shop in SHOPS:
            for code, size, count, _time, rate in self.db.get_instock_last_update(shop):  # load from database
                if code not in last_update_instock[shop].keys():
                    last_update_instock[shop][code] = list()
                last_update_instock[shop][code].append((float(size), count, _time, rate))

        # New items (new code_id, which dooes not in 'instock_...' db) add to table istantly without any check
        absolutely_new = {shop: list() for shop in SHOPS}
        for shop in SHOPS:
            for code in loaded_instock[shop].keys():
                if code not in last_update_instock[shop].keys():
                    absolutely_new[shop].extend([(code, *i) for i in loaded_instock[shop][code]])

        # Check items for consistency already available
        new = {shop: list() for shop in SHOPS}  # new available sizes with existing items in the selected store
        updated = {shop: list() for shop in SHOPS}
        not_instock = {shop: list() for shop in SHOPS}

        for shop in SHOPS:
            for code in last_update_instock[shop].keys():  # from database
                if code in loaded_instock[shop].keys():  # if codes from kant.ru and database matched
                    for size, count, timestmp, rate in last_update_instock[shop][code]:  # check database
                        for size_, count_, timestmp_, rate_ in loaded_instock[shop][code]:  # check kant.ru
                            if size == size_ and count != count_:  # if sizes matched and count is updated (not matched)
                                updated[shop].append((code, size_, count_, timestamp, rate + 1))
                                break
                    # needs lambda to equal types to correct working with values in future
                    # unic sizes if item from database
                    last_update_sizes = set(map(lambda x: float(x[0]), last_update_instock[shop][code]))
                    # unic sizes of item from kant.ru
                    loaded_sizes = set(map(lambda x: float(x[0]), loaded_instock[shop][code]))
                    not_instock_sizes = last_update_sizes - loaded_sizes
                    new_sizes = loaded_sizes - last_update_sizes
                    # product was available in stock, but it dropped out now
                    not_instock[shop].extend([(code, value[0], 0, timestamp, value[3]+1)
                                        for value in last_update_instock[shop][code]
                                              if (value[0] in not_instock_sizes and value[1] != 0)])
                    new[shop].extend([(code, value[0], value[1], timestamp, RATING)
                                for value in loaded_instock[shop][code] if value[0] in new_sizes])
                else:  # if product was in db, but dropped out of the store completely
                    # add to not_instock dropped out items, but not rewrite no longer exists items
                    not_instock[shop].extend([(code, item[0], 0, timestamp, item[3]+1)
                                              for item in last_update_instock[shop][code] if item[1] != 0])

        for i, data in enumerate([absolutely_new, new, updated, not_instock]):
            if i == 0:
                group = 'Absolutely new items'
            elif i == 1:
                group = 'New'
            elif i == 2:
                group = 'Update'
            elif i == 3:
                group = 'Not in stock'
            is_not_empty = [bool(i) for i in data.values() if i]
            if is_not_empty:  # add to db new items
                for shop in SHOPS:
                    if data[shop]:
                        recorded_lines = self.db.to_instock(shop, data[shop])
                        if DEBUG:
                            print('Shop:', shop)
                            print("{} sizes, {}: {}".format(group, len(data[shop]), data[shop]))
                            print('Recorded lines to database:', recorded_lines, '\n')

        if DEBUG:
            print('> End update_instock_tables on {}.'.format(tac()))

        return True  # if that's all ok

    def export(self, to='csv'):
        """
        Serialized and export to file for connect to marketplace API and retail services ('InSales', example).
        'to' parameter may be:
            'json'-- export to json file
            'xml'-- export to xml file
            'csv'-- export to csv file
        """

        # real path to json file. If json file should be a parent dir,
        # set parent_dir = Path(__file__).resolve().parent.parent
        parent_dir = Path(__file__).resolve().parent

        if to == 'json':

            import json
            from settings import JSON_FILE
            file_name = os.path.join(parent_dir, JSON_FILE)  # path + file with any OS

            card_description = dict()
            for card in self.db.export_card_and_price():
                code = card[0]
                card_description[code] = dict()
                item_available = self.db.export_available(code)
                if item_available:
                    card_description[code]['code'] = card[0]
                    card_description[code]['model'] = card[1]
                    card_description[code]['brand'] = card[2]
                    card_description[code]['price'] = card[3]
                    card_description[code]['url'] = card[4]
                    card_description[code]['img'] = card[5]
                    card_description[code]['age'] = card[6]
                    card_description[code]['gender'] = card[7]
                    card_description[code]['year'] = card[8]
                    card_description[code]['use'] = card[9]
                    card_description[code]['pronation'] = card[10]
                    card_description[code]['article'] = card[11]
                    card_description[code]['season'] = card[12]
                    card_description[code]['instock'] = item_available
                else:
                    del card_description[code]  # if empty item availability

            if card_description:
                with open(file_name, 'w') as f:
                    json.dump(card_description, f)
                    if DEBUG:
                        print('JSON file is updated!')
                    return True
            else:
                print("Database is empty or no database file. Run Main.update_...() methods to filling database, "
                      "then use this method to export data.")
                return False

        elif to == 'xml':
            # TODO make xml export to file
            pass

        elif to == 'csv':

            import csv
            from settings import CSV_FILE
            file_name = os.path.join(parent_dir, CSV_FILE)  # path + file with any OS

            card_fields = ('Код', 'Модель', 'Бренд', 'Стоимость', 'Ссылка', 'Картинка', 'Возраст', 'Пол', 'Год',
                'Назначение', 'Пронация', 'Артикул', 'Сезон')
            card_description = self.db.export_card_and_price()
            if card_description:
                with open(file_name, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow(card_fields)
                    writer.writerows(card_description)
                if DEBUG:
                    print('CSV file is updated!')
                return True
            else:
                print("Database is empty or no database file. Run Main.update_...() methods to filling database, "
                      "then use this method to export data.")

                return False

        else:
            if DEBUG:
                print("Check 'to' parameter on 'Main.export()' method")

        return False


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

    # First method to work with project
    # comment line below to use Second working method
    manager()  # command line use
    #
    # manager(True, False, False)  # update 'products' table to database
    # manager(False, True, False)  # update 'prices'
    # manager(False, False, True)  # update 'instock_nagornaya', 'instock_altufevo', ... instock tables
    # manager(True, True, True)  # update all working tables

    #
    # Second method to work with project:
    #
    # Main() -- is main connector to parser and to database and syncronizer between them
    # all actual working brands in settings.BRANDS, as optional.
    # uncomment line below to work only this brand from www.kant.ru
    # page = Main('Adidas')
    #
    # or uncomment this line below to work with full running shoes items from www.kant.ru
    # page = Main()
    #
    # page.update_products_table()  # uncomment to update 'products' table
    # page.update_prices_table()  # uncomment to update 'prices' table
    # page.update_instock_table()  # uncomment to update 'instock_nagornaya', 'instock_altufevo', ... instock tables
    # uncomment 3 strings above to update all tables immediately
    #
    # TODO complete xml export
    # Export/ serialized to json/ xml/ csv
    # page.export(to='csv')

    if DEBUG:
        print(tac(), 'worked app.')
