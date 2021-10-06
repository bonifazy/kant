from aiounittest import AsyncTestCase
from unittest import TestCase, main, skipIf

from parser import Parser
from main import Main
from db import SQLite
from settings import SHOPS, BRANDS_URLS

SKIP = False  # set False to check all tests
# more cases relevant only in oct- nov 2021


@skipIf(SKIP, 'skip parse_main')
class TestAsyncParseMain(AsyncTestCase):

    async def test_parse_main_correct_data(self):
        cases = (['http://www.kant.ru/brand/brooks/products/'], [BRANDS_URLS[-1]])
        solution_str = 'https://www.kant.ru/catalog/product'  # any solution item must be starts with this str
        for i in cases:
            with self.subTest(case=i):
                response = await Parser.parse_main(i, 1)
                self.assertEqual(response[0][:35], solution_str)

    async def test_parse_main_counts_and_values(self):
        case = ['https://www.kant.ru/catalog/shoes/running-shoes/brand-asics/']
        small_page_count = 1  # min pag pars
        approx_min_values = 24  # an usual, 24 items in one main parsing page and less, if it is last main page
        response = await Parser.parse_main(case, small_page_count)
        self.assertLessEqual(len(response), approx_min_values)

    async def test_parse_main_max_pagination(self):
        case = ['https://www.kant.ru/catalog/shoes/running-shoes/brand-asics/']
        big_page_num = 30  # max pagination parsing
        approx_count = 100  # from Asics many deep models line, if parsed all pages
        solution = 'https://www.kant.ru/catalog/product/3052137/'  # actual for jul 2021
        response = await Parser.parse_main(case, big_page_num)
        self.assertIn(solution, response)
        self.assertGreater(len(response), approx_count)

    async def test_parse_main_uncorrect_url(self):
        with self.assertRaises(ValueError):
            await Parser.parse_main(['kant.ru/catalog/shoes/running-shoes/brand-asics/'], 1)

    async def test_parse_main_bad_kant_ru_url(self):
        response = await Parser.parse_main(['http://www.kant.ru/brand/NOEXISTBRAND/products/'], 1)
        self.assertEqual(response, list())

    async def test_parse_main_uncorrect_type(self):
        case = 'http://www.kant.ru/catalog/shoes/running-shoes/brand-asics/'
        with self.assertRaises(TypeError):
            await Parser.parse_main(case, 1)  # need list of strs, send str
        with self.assertRaises(TypeError):
            await Parser.parse_main([case], 1.0)  # need int value to pagination

    async def test_parse_main_uncorrect_value(self):
        case = ['http://www.kant.ru/catalog/shoes/running-shoes/brand-asics/']
        with self.assertRaises(ValueError):
            await Parser.parse_main(case, 0)
        with self.assertRaises(ValueError):
            await Parser.parse_main(case, 40)


@skipIf(SKIP, 'skip parse_details')
class TestAsyncParseDetails(AsyncTestCase):

    async def test_parse_details_correct_data(self):
        cases = ['https://www.kant.ru/catalog/product/1693473/', 'https://www.kant.ru/catalog/product/3052137/',
                 'https://www.kant.ru/catalog/product/2906145/'
                 ]
        solution_count_positions = 14
        for i in cases:
            with self.subTest(case=i):
                response = await Parser.parse_details([i])
                solution = response[0]
                self.assertEqual(len(solution), solution_count_positions)
                self.assertEqual((type(solution[0]), type(solution[1]), type(solution[2]), type(solution[3]),
                                  type(solution[4]), type(solution[5]), type(solution[6]), type(solution[7]),
                                  type(solution[8]), type(solution[9]), type(solution[10]), type(solution[11]),
                                  type(solution[12]), type(solution[13])
                                  ),
                                 (int, str, str, str, str, str, str, int, str, str, str, str, int, str)
                                 )

    async def test_parse_details_correct_year(self):
        cases = ['https://www.kant.ru/catalog/product/1693473/', 'https://www.kant.ru/catalog/product/2783417/']
        for i in cases:
            with self.subTest(case=i):
                response = await Parser.parse_details([i])
                solution = response[0]
                self.assertIsNot(solution[7], 0)
                self.assertGreater(solution[7], 2015)
                self.assertLess(solution[7], 2050)

    async def test_parse_details_bad_url(self):
        case = 'https://www.kant.ru/catalog/product/7777777/'
        response = await Parser.parse_details([case])
        self.assertEqual(response, list())

    async def test_parse_details_uncorrect_type(self):
        case_without_list = 'https://www.kant.ru/catalog/product/3052137/'
        with self.assertRaises(TypeError):
            await Parser.parse_details(case_without_list)  # need list of strs, send str

    async def test_parse_details_uncorrect_url_format(self):
        case = ['catalog/product/2780445/']  # raw data from kant.ru without program preset
        with self.assertRaises(ValueError):
            await Parser.parse_details(case)


@skipIf(SKIP, 'skip parse_price')
class TestAsyncParsePrice(AsyncTestCase):

    async def test_parse_price_correct_data(self):
        cases = [(1626114, 'https://www.kant.ru/catalog/product/2906145/'),
                 (1599205, 'https://www.kant.ru/catalog/product/2780445/')
                 ]
        for i, (code, url) in enumerate(cases):
            with self.subTest(case=(code, url)):
                response = await Parser.parse_price([(code, url)])
                self.assertEqual(response[0][0], code)
                self.assertGreater(response[0][1], 1000)  # min price
                self.assertLess(response[0][1], 30000)  # max price

    async def test_parse_price_bad_data_type(self):
        case_reversed_code_url_values = [('https://www.kant.ru/catalog/product/2906145/', 1626114)]
        case_without_list = (1626114, 'https://www.kant.ru/catalog/product/2906145/')
        with self.assertRaises(TypeError):
            await Parser.parse_price(case_reversed_code_url_values)
        with self.assertRaises(TypeError):
            await Parser.parse_price(case_without_list)

    async def test_parse_bad_url(self):
        case = [(1626114, 'catalog/product/2906145/')]
        with self.assertRaises(ValueError):
            await Parser.parse_price(case)


@skipIf(SKIP, 'skip parse_available')
class TestAsyncParseAvailable(AsyncTestCase):

    async def test_parse_available_correct_data(self):
        cases = [(1663184, 3094643), (1648135, 3006103), (1653643, 3052219)]
        for (code, url_code) in cases:
            with self.subTest(case=(code, url_code)):
                response = await Parser.parse_available([(code, url_code)])
                response_code = response[0][0]
                response_instock = response[0][1]
                is_shops = [i for i in response_instock.keys() if i in SHOPS]
                size_value = response_instock[is_shops[0]][0]
                self.assertEqual(response_code, code)  # первый параметр-- код, тот же, что и на входе
                self.assertEqual(type(response_instock), dict)  # вывод в dict(), магазины и размерный ряд в них
                self.assertTrue(is_shops)  # хотя бы в в 1 магазине есть хотя бы 1 товар
                self.assertEqual(type(size_value), tuple)  # в dict магазинов действительно находятся пары: size, count
                self.assertEqual(type(size_value[0]), float)  # графа 'размер' содержит float значение
                self.assertEqual(type(size_value[1]), int)  # значение количества товара определенного размера в int

    async def test_parse_available_reversed_codes(self):
        case = [(3006103, 1648135)]
        response = await Parser.parse_available(case)
        self.assertEqual(response[0][1], dict())  # пустой dict, если перепутали коды местами

    async def test_parse_available_without_url_code(self):
        case = [(1648135)]  # code must be with code_url in tuple
        with self.assertRaises(TypeError):
            await Parser.parse_available(case)

    async def test_parse_available_bad_type_including_codes(self):
        case = [1648135]  # real code without his code url pair
        with self.assertRaises(TypeError):
            await Parser.parse_available(case)

    async def test_parse_available_bad_type_value(self):
        case = [('1648135', '3006103')]  # support only int, not str type
        with self.assertRaises(TypeError):
            await Parser.parse_available(case)


# True start main parsing class.
# Run after filling 'products' table from test_update_products() and test_update_prices() or uncomment these cases
@skipIf(SKIP, 'skip main page parsing')
class TestMain(TestCase):

    def test_update_products(self):
        main_page = Main('On')
        solution = main_page.update_products_table()
        self.assertTrue(solution)

    def test_update_prices(self):
        main_page = Main('Brooks')
        # uncomment the lines below if 'products' table is empty (if this brand does not exist)
        # solution = main_page.update_prices_table()
        # self.assertTrue(solution)
        solution = main_page.update_prices_table()
        self.assertTrue(solution)

    def test_update_instock(self):
        main_page = Main('Saucony')
        # uncomment the lines below if 'products' table is empty (if this brand does not exist)
        # solution = main_page.update_instock_table()
        # self.assertTrue(solution)
        solution = main_page.update_instock_table()
        self.assertTrue(solution)


@skipIf(SKIP, 'skip test db')
class TestDb(TestCase):
    """
    Test django database by ordered by logger/models.py
    model Product mapping to 'products' table in database
    model Prices mapping to 'prices' table
    model InstockNagornaya mapping to 'instock_nagornaya' table
    Use after filling all working tables. See README.txt or main.py
    """

    def __init__(self, *args, **quargs):
        super(TestDb, self).__init__(*args, **quargs)
        self.db = SQLite()

    def test_connect(self):
        self.assertIsNotNone(self.db.conn)

    def test_products_contains(self):
        solution = self.db.test_products()
        self.assertEqual(len(solution), 14)  # count of fields
        # Products 'products' fields: code, brand, model, url, img, age, gender, year, use, pronation, article,
        #   season, timestamp, rating
        self.assertEqual((type(solution[0]), type(solution[1]), type(solution[2]), type(solution[3]),
                          type(solution[4]), type(solution[5]), type(solution[6]), type(solution[7]),
                          type(solution[8]), type(solution[9]), type(solution[10]), type(solution[11]),
                          type(solution[12]), type(solution[13])
                          ),
                         (int, str, str, str, str, str, str, int, str, str, str, str, str, int)
                         )  # true ordering
        self.assertGreater(solution[0], 1_000_000)  # check correct code value
        self.assertLess(solution[0], 2_000_000)  # correct code: 1_000_000 < code < 2_000_000

    def test_prices_contains(self):
        solution = self.db.test_prices()
        self.assertEqual(len(solution), 4)  # count of fields
        # Prices 'prices' fields: code_id, price, timestamp, rating
        self.assertEqual((type(solution[0]), type(solution[1]), type(solution[2]), type(solution[3])),
                        (int, int, str, int)
                        )  # true ordering

    def test_instock_nagornaya(self):
        solution = self.db.test_instock_nagornaya()
        self.assertEqual(len(solution), 5)  # count of fields
        # InstockNagornaya 'instock_nagornaya' fields: code_id, size, count, timestamp, rating
        self.assertEqual((type(solution[0]), type(solution[2]), type(solution[3]), type(solution[4])),
                         (int, int, str, int)
                         )  # true ordering
        size_is_digit = (type(solution[1]) == float) or (type(solution[1]) == int)
        self.assertTrue(size_is_digit)


if __name__ == "__main__":
    main()
