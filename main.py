#!/usr/bin/env python
# _*_ coding: utf-8 _*_
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters
from functools import wraps
import telegram, logging
import configparser, sqlite3, pathlib, re, datetime, signal, os

# logging initialize
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)

# config initializing
config = configparser.ConfigParser()
config.read('config.ini')

# telegram bot initializing
updater = Updater(token=config['DEFAULT']['token'])
admin_list = [int(x) for x in config['DEFAULT']['admin_list'].split(',')]
dispatcher = updater.dispatcher

# SQLite initializing
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "data.db")
p = pathlib.Path(db_path)

# Generic functions


def parse_sms(text: str) -> dict:
    """
    parses SMS using re, example of a valid SMS (only sberbank is supported ATM):
    VISA1234 21.12.16 22:12
    зачисление зарплаты 12345.57р
    Баланс: 16063.28р
    """

    sms_data = {}
    sms_pattern = re.compile(r'''
    # matching anywhere in string
    ([A-Z]{4}\d{4})                        # matching card
    \ +                                    # separator is one space or more
    (\d{2}\.\d{2}\.\d{2}\ \d{2}\:\d{2})    # matching date and time
    \D*                       # optional separator is any number of non digits
    (\d+\.\d+)                             # matching amount
    ''', re.VERBOSE)

    parsed_sms = sms_pattern.search(text).groups()
    sms_data['card'] = parsed_sms[0]
    sms_data['datetime'] = datetime.datetime.strptime(parsed_sms[1], "%d.%m.%y %H:%M")
    sms_data['amount'] = float(parsed_sms[2])

    return sms_data


# SQLite retrieval functions

def datatable_init():
    """
    creates if necessary and opens data.db SQLite database
    """
    with sqlite3.connect(db_path, check_same_thread=False) as db:
        cursor = db.cursor()
        if not p.is_file():
            cursor.execute('''CREATE TABLE data (
                       chat_id INTEGER,
                       name TEXT,
                       card TEXT,
                       date_time DATETIME,
                       amount REAL
                       )''')
            db.commit()


def insert_transaction(chat_id: int, username: str, sms_data: dict):
    """
    takes in chat_id of the convo and parsed sms dictionary, writes data into DB
    """
    with db:
        cursor.execute("INSERT INTO data VALUES (:id, :name, :card, :datetime, :amount)",
                       {'id': chat_id, 'name': username, 'card': sms_data['card'],
                        'datetime': sms_data['datetime'], 'amount': sms_data['amount']})


def table_data() -> list:
    """
    returns ALL available data, list of tuples
    """
    cursor.execute("SELECT * FROM data")
    return cursor.fetchall()


def user_data(chat_id: str) -> list:
    """
    returns user data, list of tuples
    """
    cursor.execute("SELECT * FROM data WHERE chat_id=:id", {'id': chat_id})
    return cursor.fetchall()


def user_records(chat_id: str) -> list:
    """
    returns number of relevant user records
    """
    cursor.execute("SELECT COUNT(*) FROM data WHERE chat_id=:id", {'id': chat_id})
    return cursor.fetchone()[0]


def test_sum(chat_id: str) -> list:
    cursor.execute("SELECT SUM(amount) FROM data WHERE chat_id=:id", {'id': chat_id})
    return cursor.fetchone()


# Bot handlers

def restricted(func):
    @wraps(func)
    def wrapped(bot, update, *args, **kwargs):
        user_id = update.effective_user.id
        if user_id not in admin_list:
            bot.send_message(chat_id=update.message.chat_id,
                         text="You don't have access to this command.")
            return
        return func(bot, update, *args, **kwargs)
    return wrapped


def start(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
                     text='This bot parses and stores info about your monthly earnings!')
    bot.send_message(chat_id=update.message.chat_id, text='Please, send SMS from the bank')


def sms(bot, update):
    """
    on receiving SMS, tries to parse it and add to DB, otherwise reprompts user
    """
    try:
        sms = parse_sms(update.message.text)
        insert_transaction(update.message.chat_id, update.message.from_user['username'], sms)
        bot.send_message(chat_id=update.message.chat_id,
                         text='Transaction added successfully')
    except Exception as e:
        print(e)
        bot.send_message(chat_id=update.message.chat_id,
                         text='Unable to parse. Please, send valid SMS!')


@restricted
def all_data(bot, update):
    bot.send_message(chat_id=update.message.chat_id, text=str(table_data()))


@restricted
def dump_db(bot, update):
    bot.send_document(chat_id=update.message.chat_id,
                          document=open(db_path, 'rb'))


def user_data(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
                     text=str(user_data(update.message.chat_id)))


def user_info(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
                 text="Your chat_id is {}, we have {} records concerning you".\
                 format(update.message.chat_id, user_records(update.message.chat_id)))


# MAIN LOOP HERE

def main():
    start_handler = CommandHandler('start', start)
    alldata_handler = CommandHandler('alldata', all_data)
    userdata_handler = CommandHandler('userdata', user_data)
    dumpdb_handler = CommandHandler('dumpdb', dump_db)
    userinfo_handler = CommandHandler('userinfo', user_info)
    sms_handler = MessageHandler(Filters.text, sms)

    dispatcher.add_handler(start_handler)
    dispatcher.add_handler(alldata_handler)
    dispatcher.add_handler(userdata_handler)
    dispatcher.add_handler(dumpdb_handler)
    dispatcher.add_handler(userinfo_handler)
    dispatcher.add_handler(sms_handler)

    updater.start_polling()


if __name__ == '__main__':
    datatable_init()
    main()
