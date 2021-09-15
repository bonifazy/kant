import time
import asyncio
import aiohttp
from lxml import html as lxml_html

from settings import DEBUG, RATING, CHUNK, TIMEOUT, AVAILABLE, BRANDS, SHOPS

if DEBUG:
    tic = lambda: time.time()


class Parser:

    @staticmethod
    async def parse_main(urls: list, finish: int) -> list:

        async def main_page_urls(url_: str, params_: int) -> list:

            urls = list()
            async with aiohttp.ClientSession() as session:
                async with session.get(url_, params={'PAGEN_1': params_}) as response:
                    html = await response.text()
            if "kant__catalog__item" in html:  # find urls from all shoes items on page
                tree = lxml_html.fromstring(html)
                a_tags = tree.xpath("//div[@class='kant__catalog__item']//a")  # links for smth items
                for item in a_tags:
                    name = item.values()[1].lower()
                    if 'кроссовки' in name or 'марафонки' in name:
                        # get useful urls to solution
                        item_url = 'https://www.kant.ru{}'.format(item.values()[0])
                        urls.append(item_url)
            else:  # its not page with running shoes. Stop coroutines!
                return list()

            return urls

        # Check correct input data
        if type(urls) is not list:
            raise TypeError('Set list of str urls, not more')
        for i in urls:
            if not (i.startswith('https://www.kant.ru') or i.startswith('http://www.kant.ru')):
                raise ValueError('Set correct value to urls, starts with: http(s)://www.kant.ru')
        if type(finish) is not int:
            raise TypeError('Set correct value to max pagination: from 1 to 30')
        if finish < 1 or finish > 30:
            raise ValueError('Set correct value to max pagination: from 1 to 30')
        if DEBUG:
            now = tic()
            tac = lambda: "{:.2f}sec".format(time.time() - now)
            print('\r\n>>> Start parse_main at ', time.strftime('%H:%M:%S', time.localtime()))

        # start parsing urls by chunk of tasks
        chunk = CHUNK
        solution_urls = list()
        all_urls = len(urls)
        for i, page_url in enumerate(urls):
            tasks = list()
            items_urls = list()
            do_search = True
            for pagination in range(1, finish+1):  # go for pages of each item url
                if DEBUG:
                    # progress bar
                    print('\r{}, {}/ {} progress. Now {} page of {}\r'.format(
                        tac(), i+1, all_urls, pagination, page_url), end='')
                tasks.append(asyncio.create_task(main_page_urls(page_url, pagination)))
                if len(tasks) == chunk or pagination == finish:
                    new_urls = await asyncio.gather(*tasks)
                    for urls in new_urls:
                        # if links is finded and first element is not repeated in the set of the same main url
                        # then keep looking
                        if urls and urls[0] not in items_urls:
                            items_urls.extend(urls)
                        # finish searching, because it is already repeated or empty task
                        else:
                            do_search = False
                    solution_urls.extend(items_urls)  # extend items set within this brand
                    tasks = list()
                    await asyncio.sleep(TIMEOUT)
                if not do_search:  # exit from pagination if empty tasks or repeat items content
                    break
        if DEBUG:
            print('>>> End parse_main on {} sec. Find {} urls.\n'.format(tac(), len(solution_urls)))
        return solution_urls

    @staticmethod
    async def parse_details(urls: list) -> list:
        """
        Parsed list urls, format 'https://www.kant.ru/catalog/product/123456(78)/'
        to full details info:
        code-- primary unic value to operate of each items from db
        brand-- item brand name, mostly commons with names from setting.BRANDS
        model-- unic item name
        url-- this func argv
        img-- url to item img, small pic
        age-- 'взрослый', 'юниор', 'детский' and may be smth else
        gender-- ''
        year-- '2020', '2021', '2020-2021', '20-2021' and may be smth else
        use-- 'грунт', 'асфальт', 'снег/ лед', may be smth else
        pronation-- 'нейтральная', 'нейтральная/ нейтральная', 'с поддержкой' and more
        article-- unic value within model name of brand
        season-- 'лето', 'демисезон', 'зима' and more
        rating-- program rate for ordering and analytics items data by changes prices of items or change availability
        timestamp-- stamp to update

        return items info by list of tuples
        """

        async def parse(url_: str, timestamp_: str) -> tuple:
            """
            Parsed url of item by aiohttp.ClientSession.get and lxml.html
            return tuple of full item info
            """
            async with aiohttp.ClientSession() as session:
                async with session.get(url_) as response:
                    html = await response.text()
            if not html:
                return None, None  # output need tuple return
            code = brand = model = img = rating = None
            age = gender = article = season = use = pronation = ''
            year = 0
            tree = lxml_html.fromstring(html)
            running = False  # Точно ли это кроссовки?
            # commons attrs from xpath objs: values, text, xpath, text_content, keys, label, items, base, attrib
            for item in tree.xpath("//div[@class='kant__product__detail-item']"):  # product description card
                column = item.xpath("span[1]/text()")[0]
                if len(item.xpath("span[2]/text()")) > 0:
                    value = item.xpath("span[2]/text()")[0]
                    if (column == 'Назначение' and 'бег' in value) or \
                            (column == 'Тип' and 'кроссовки' in value or 'марафонки' in value):
                        running = True
                    if column == 'Бренд':
                        brand = value.lower()
            if running:  # card description found!
                name = tree.xpath("//div[@id='kantMainCardProduct']/h1/text()")[0].lower()
                if brand is None:
                    if 'кроссовки' in name or 'марафонки' in name:
                        temp = [i for i in name.split() if i in [j.lower() for j in BRANDS]]  # brandname from item name
                        if temp:
                            brand = temp[0]
                model = name.partition(brand)[2].strip()
                brand = brand.title()
                code = tree.xpath("//div[@class='kant__product__code']/strong/text()")[0]
                code = int(code) if code.isdecimal() else 0
                if tree.xpath("//div[@class='kant__product__color__thumbs']//img"):
                    img = 'https://www.kant.ru' + \
                          tree.xpath("//div[@class='kant__product__color__thumbs']//img")[0].values()[0]
                else:
                    img = 'https://www.kant.ru'
                for item in tree.xpath("//div[@class='kant__product__detail-item']"):
                    column = str(item.xpath("span[1]/text()")[0])
                    if len(item.xpath("span[2]/text()")) > 0:
                        value = str(item.xpath("span[2]/text()")[0])
                        if column == 'Возраст':
                            age = value
                        if column == 'Пол':
                            gender = value
                        # sometimes year like 2021-22 (not 2021, or not 2022), produces on 2021 (earlest date)
                        # in fact. So set one first earliest date
                        if column == 'Модельный год':
                            value = value.partition('-')[0]
                            year = int(value) if value.isdecimal() else 0
                        if column == 'Покрытие':
                            use = value
                        if column == 'Пронация':
                            pronation = value
                        if column == 'Артикул':
                            article = value
                        if column == 'Сезон':
                            season = value
                # special to Saucony brand:
                if brand == 'Saucony' and model.startswith('s-'):
                    age = 'junior' if age == '' else age
                # end special Saucony
                #
                # special to Hoka brand:
                if brand == 'Hoka':
                    if model.startswith('m '):
                        gender = 'man' if gender == '' else gender
                    elif model.startswith('w '):
                        gender = 'woman' if gender == '' else gender
                    model = model[2:]
                # end special Hoka
                # TODO more specific setting for other brands or other keys or agregate more column to one in future
                rating = RATING
            return code, brand, model, url_, img, age, gender, year, use, pronation, article, season, rating, timestamp_

        # check urls content on correct with prev call func parse_main.main_page_urls
        if type(urls) is not list:
            raise TypeError
        if not urls[0].startswith('https://www.kant.ru'):
            raise ValueError
        if DEBUG:
            now = tic()
            tac = lambda: '{:.2f}sec'.format(time.time() - now)
            print('\r\n>>> Start parse_details at ', time.strftime('%H:%M:%S', time.localtime()))
        all_urls = len(urls)
        products = list()
        tasks = list()
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        for i, url in enumerate(urls):
            tasks.append(asyncio.create_task(parse(url, timestamp)))
            if len(tasks) == CHUNK or i+1 == all_urls:
                new = await asyncio.gather(*tasks)
                new = [i for i in new if i[0] is not None]  # valid parsing from async def 'parse'
                products.extend(new)
                tasks = list()
                await asyncio.sleep(TIMEOUT)
            if DEBUG:
                print('\r{} sec, {}/ {}\r'.format(tac(), i+1, all_urls), end='')  # progress bar
        if DEBUG:
            print('>>> End parse_details on {} sec. Parsed {} items.\n'.format(tac(), len(products)))

        return products  # [(code, brand, model, url, img, age..), (code, brand, model, ..), ...,]

    @staticmethod
    async def parse_price(codes_urls: list) -> list:

        async def get_and_parse(code, url: str) -> tuple:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    html = await response.text()
            if not html:
                return None, None
            tree = lxml_html.fromstring(html)
            if tree.xpath("//div[@class='kant__product__price']/span[2]/text()"):
                price = ''.join(tree.xpath("//div[@class='kant__product__price']/span[2]/text()")[0].split(' '))
                price = int(price) if price.isdecimal() else 0
            else:
                price = 0

            return code, price

        if not (type(codes_urls) is list and type(codes_urls[0]) is tuple and type(codes_urls[0][0]) is int
                and type(codes_urls[0][1])):
            raise TypeError
        if not (100_000 < codes_urls[0][0] < 9_999_999 and codes_urls[0][1].startswith('https://www.kant.ru/')):
            raise ValueError
        tasks = list()
        products = list()  # general solution list
        all_urls = len(codes_urls)
        if DEBUG:
            now = tic()
            tac = lambda: "{:.2f}sec".format(time.time() - now)
            print('\r\n>>> Start parse_price at ', time.strftime('%H:%M:%S', time.localtime()))
        for i, (code, url) in enumerate(codes_urls):
            tasks.append(asyncio.create_task(get_and_parse(code, url)))
            if len(tasks) == CHUNK or i+1 == all_urls:
                if DEBUG:
                    print('\r{} sec, {}/ {}: {}\r'.format(tac(), i+1, all_urls, url), end='')
                new = await asyncio.gather(*tasks)
                new = [i for i in new if i[0] is not None]
                products.extend(new)
                await asyncio.sleep(TIMEOUT)  # add value if you banned from kant.ru
                tasks = list()
        if DEBUG:
            print('>>> End parse_price on {} sec. Parsed {} items.\n'.format(tac(), len(products)))

        return products  # [(code, price), (code, price)...]

    @staticmethod
    async def parse_available(codes: list)-> list:

        async def parse_instock(code_: int, instock_code_: int) -> tuple:

            async with aiohttp.ClientSession() as session:
                async with session.get(AVAILABLE, params={'ID': instock_code_}) as response:
                    html = await response.text()
            if not html:
                return None, None
            tree = lxml_html.fromstring(html)
            popur_row_div = tree.xpath("//div[@data-tab='tab958']/div")  # div class = popur__row

            tables = tree.xpath("//div[@data-tab='tab958']/table")
            table_index = 0
            shops = SHOPS
            shop = None
            in_stock = dict()
            for i, div in enumerate(popur_row_div):
                div_content = div.text_content().lower()
                if 'нагорная' in div_content:
                    if 'нет в наличии' not in div_content:
                        shop = shops[0]
                    else:
                        continue
                if 'тимирязевская' in div_content:
                    if 'нет в наличии' not in div_content:
                        shop = shops[1]
                    else:
                        continue
                if 'теплый стан' in div_content:
                    if 'нет в наличии' not in div_content:
                        shop = shops[2]
                    else:
                        continue
                if 'алтуфьево' in div_content:
                    if 'нет в наличии' not in div_content:
                        shop = shops[3]
                    else:
                        break
                in_stock[shop] = list()
                table = tables[table_index]
                table_index += 1
                tr = table.xpath("tr")
                for row in tr:
                    row_list = row.text_content().split()
                    row_count = len(row_list)
                    if row_count == 2 or row_count == 3:  # size and availability found!
                        size_temp = row_list[0].lower()
                        size = None
                        # sizes to database oriented (converter) to US size
                        if size_temp.startswith('us'):
                            size = float(size_temp.split(':')[1].replace(',', '.', 1))
                        # transform to US size
                        if size_temp.startswith('uk'):
                            size = float(size_temp.split(':')[1].replace(',', '.', 1)) + 1
                        # junior size
                        elif size_temp.startswith('k'):
                            size = float(size[1:])
                        # TODO convert to US size!
                        if size_temp.startswith('eur'):
                            size = float(size_temp.split(':')[1].replace(',', '.', 1))
                        in_stock[shop].append((size, int(row_list[1]) if row_count == 2 else int(row_list[2])))

            return code_, in_stock

        # correct data, for testing
        item = codes[0]
        if not (type(item) is tuple and type(item[0]) is int and type(item[1]) is int):
            raise TypeError

        tasks = list()
        products = list()  # general solution list
        count_codes = len(codes)
        if DEBUG:
            now = tic()
            tac = lambda: "{:.2f}sec".format(time.time() - now)
            print('\r\n>>> Start parse_available at ', time.strftime('%H:%M:%S', time.localtime()))
        for i, (code, instock_code) in enumerate(codes):
            tasks.append(asyncio.create_task(parse_instock(code, instock_code)))
            if len(tasks) == CHUNK or i+1 == count_codes:
                if DEBUG:
                    print('\r{} sec, {}/ {}: {}\r'.format(tac(), i+1, count_codes, code), end='')
                new = await asyncio.gather(*tasks)
                new = [i for i in new if i[0] is not None]
                products.extend(new)
                await asyncio.sleep(TIMEOUT * 0.5)  # get tiny html, so reduce timeout, since the load is lower
                tasks = list()
        if DEBUG:
            print('>>> End parse_available on {} sec. Parsed {} items.\n'.format(tac(), len(products)))

        return products
