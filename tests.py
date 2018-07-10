#!/usr/bin/env python
# _*_ coding: utf-8 _*_
import main
import unittest
import datetime

class ParseTest(unittest.TestCase):

    def test_proper_parse(self):
        ''' parse_sms should give known result with known input '''

        sms = "VISA1234 21.12.16 22:12 зачисление зарплаты 12345.57р Баланс: 16063.28р"

        self.assertEqual('VISA1234', main.parse_sms(sms)['card'])
        self.assertEqual(datetime.datetime(2016,12,21,22,12), main.parse_sms(sms)['datetime'])
        self.assertEqual(12345.57, main.parse_sms(sms)['amount'])


    def test_failed_parse(self):
        ''' parse_sms should fail with AttributeError on bad input'''
        sms = 'dasfdsf'
        self.assertRaises(AttributeError, main.parse_sms, sms)


class DBTest(unittest.TestCase):

    def test_read(self):
        main.datatable_init()
        print(main.table_data())
        main.db.close()

if __name__ == '__main__':
    unittest.main()
