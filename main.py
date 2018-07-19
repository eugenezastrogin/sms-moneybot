#!/usr/bin/env python
import configparser
import datetime
import io
import logging
import os
import re
import signal
import sqlite3

from functools import wraps

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ChatAction
from telegram.ext import (Updater, CommandHandler, MessageHandler, Filters,
                          CallbackQueryHandler )

# logging initialize
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logger = logging.getLogger(__name__)

# config initializing
config = configparser.ConfigParser()
config.read('config.ini')
separator_date = int(config['DEFAULT']['separator_date'])

# telegram bot initializing
updater = Updater(token=config['DEFAULT']['token'])
admin_list = [int(x) for x in config['DEFAULT']['admin_list'].split(',')]
dispatcher = updater.dispatcher

# SQLite initializing
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "data.db")

# Generic functions


def parse_sms(text: str) -> dict:
    """
    Parses SMS using re, example of a valid SMS (only sberbank is supported ATM):
    VISA1234 21.12.16 22:12
    зачисление зарплаты 12345.57р
    Баланс: 16063.28р
    """

    sms_data = {}
    sms_pattern = re.compile(r'''
    # matching anywhere in string
    ([A-Z]{4}\d{4})                        # matching card
    \ +                                    # separator is one space or more
    (\d{2}\.\d{2}\.\d{2}\ \d{2}:\d{2})     # matching date and time
    \D*                       # optional separator is any number of non digits
    зарплаты
    \D*                       # optional separator is any number of non digits
    (\d+\.*\d*)                            # matching amount
    ''', re.VERBOSE)

    parsed_sms = sms_pattern.search(text).groups()
    sms_data['card'] = parsed_sms[0]
    sms_data['datetime'] = datetime.datetime.strptime(parsed_sms[1],
                                                      "%d.%m.%y %H:%M")
    sms_data['amount'] = float(parsed_sms[2])

    return sms_data


def valid_card(text: str) -> bool:
    """
    Parses card number, returns True if proper format
    """

    card_pattern = '''
    ^                          # matching from the start of the string
    [A-Z]{4}\d{4}              # matching card
    $                          # end of the string follows immediately
    '''
    return True if re.search(card_pattern, text, re.VERBOSE) else False

# SQLite retrieval functions


def datatable_init():
    """
    Creates if necessary data.db SQLite database
    """
    cursor.execute('''CREATE TABLE IF NOT EXISTS data (
               chat_id INTEGER,
               name TEXT,
               card TEXT,
               date_time DATETIME,
               amount REAL,
               UNIQUE(chat_id, name, card, date_time, amount)
               )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
               chat_id INTEGER,
               ignore_card TEXT,
               notify INTEGER,
               UNIQUE(chat_id, ignore_card, notify)
               )''')
    db.commit()


def insert_transaction(chat_id: int, username: str, sms_data: dict):
    """
    Takes in chat_id of the convo and parsed sms dictionary, writes data into DB
    """
    with db:
        cursor.execute("INSERT OR IGNORE INTO data \
            VALUES (:id, :name, :card, :datetime, :amount)",
            {'id': chat_id, 'name': username, 'card': sms_data['card'],
             'datetime': sms_data['datetime'], 'amount': sms_data['amount']})


def insert_ignored_card(chat_id: int, card: str):
    """
    Takes in chat_id of the convo and a card to ignore, keeps the data in DB
    """
    with db:
        cursor.execute("INSERT OR IGNORE INTO users VALUES (:id, :card)",
                       {'id': chat_id, 'card': card})


def remove_ignored_card(chat_id: int, card: str):
    with db:
        cursor.execute("DELETE FROM users WHERE chat_id=:id and ignore_card=:card",
                       {'id': chat_id, 'card': card})


def show_ignored_cards(chat_id: int) -> tuple:
    with db:
        cursor.execute("SELECT ignore_card FROM users WHERE chat_id=:id",
                       {'id': chat_id})
    return sum(cursor.fetchall(), ())


def to_notify(chat_id: int) -> tuple:
    with db:
        cursor.execute("SELECT notify FROM users WHERE chat_id=:id",
                       {'id': chat_id})
    return sum(cursor.fetchall(), ())


def table_data() -> list:
    """
    Returns ALL available data, list of tuples
    """
    cursor.execute("SELECT * FROM data")
    return cursor.fetchall()


def user_data(chat_id: str) -> list:
    """
    Returns user data, list of tuples
    """
    cursor.execute("SELECT * FROM data WHERE chat_id=:id", {'id': chat_id})
    return cursor.fetchall()


def new_card(chat_id: str, card: str) -> bool:
    """
    Checks whether this card type and number already in DB for this user
    """
    cursor.execute("SELECT COUNT(*) FROM data WHERE chat_id=:id AND card=:card",
        {'id': chat_id, 'card': card})
    return False if cursor.fetchone()[0] > 0 else True


def user_records(chat_id: str) -> list:
    """
    Returns number of relevant user records
    """
    cursor.execute("SELECT COUNT(*) FROM data WHERE chat_id=:id",
                   {'id': chat_id})
    return cursor.fetchone()[0]


def wage_calc(chat_id: str, start_date, end_date) -> list:
    cursor.execute('''SELECT SUM(amount)
                      FROM data WHERE chat_id=:id
                      AND date_time BETWEEN :sd and :ed''',
                   {'id': chat_id, 'sd': start_date, 'ed': end_date})
    return cursor.fetchone()[0]


def purge_all():
    with db:
        cursor.execute("DELETE FROM data")


def purge_user(user: int):
    with db:
        cursor.execute("DELETE FROM data WHERE chat_id=:id", {'id': user})


# Bot handlers


def restricted(func):
    @wraps(func)
    def wrapped(bot, update, *args, **kwargs):
        user_id = update.effective_user.id
        if user_id not in admin_list:
            bot.send_message(chat_id=update.message.chat_id,
                             text="You don't have access to this command.")
            return None
        return func(bot, update, *args, **kwargs)
    return wrapped


def start(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
        text='This bot parses and stores info about your monthly earnings!')
    bot.send_message(chat_id=update.message.chat_id,
        text='Please, send SMS from the bank')


def sms(bot, update):
    """
    On receiving SMS, tries to parse it and add to DB, otherwise reprompts user.
    Also shows accumulative wage in a month that SMS was from.
    If to_notify is not empty, sends an info message to all users in a notify
    list.
    """
    try:
        sms_p = parse_sms(update.message.text)
    except AttributeError:
        bot.send_message(chat_id=update.message.chat_id,
                         text='Unable to parse. Please, send valid SMS!')
        return None

    if sms_p['card'] in show_ignored_cards(update.message.chat_id):
        bot.send_message(chat_id=update.message.chat_id,
            text='You are trying to add a transaction from ignored card number.')

    insert_transaction(update.message.chat_id,
                       update.message.from_user['username'], sms_p)
    bot.send_message(chat_id=update.message.chat_id,
                     text='Transaction added successfully!')

    for recipient in to_notify:
        bot.send_message(chat_id=recipient,
            text='{} just added the following message: \n {}'.
            format(update.message.from_user['username'], update.message.text))

    if sms['datetime'].date() == datetime.datetime.now().date():
        now = datetime.datetime.now()
        month = now.month if now.day > separator_date else now.month - 1
        start_date = datetime.datetime(now.year, month-1, separator_date)
        end_date = datetime.datetime(now.year, month, separator_date)
        wage = wage_calc(update.message.chat_id, start_date, end_date)
        bot.send_message(chat_id=update.message.chat_id,
                         text="Your last month's wage is {} so far".format(wage))
    else:
        now = sms['datetime']
        month = now.month if now.day > separator_date else now.month - 1
        start_date = datetime.datetime(now.year, month-1, separator_date)
        end_date = datetime.datetime(now.year, month, separator_date)
        wage = wage_calc(update.message.chat_id, start_date, end_date)
        bot.send_message(chat_id=update.message.chat_id,
            text="Your wage in that month is {} so far".format(wage))


def csv_parse(bot, update):
    if update.message.document.file_name[-4:] != '.csv':
        bot.send_message(chat_id=update.message.chat_id,
                         text="Only CSV is allowed!")
        return None
    else:
        bot.send_message(chat_id=update.message.chat_id,
                         text="CSV file found, commencing download")

    # downloading CSV file into memory as a StringIO
    csvf = bot.getFile(update.message.document.file_id)
    csv_file_bin = io.BytesIO()
    csvf.download(out=csv_file_bin)
    csv_file_bin.seek(0)

    bot.send_message(chat_id=update.message.chat_id,
                     text="Download complete, commencing parsing")
    bot.send_chat_action(chat_id=update.message.chat_id,
                         action=ChatAction.TYPING)

    i,j,k = 0, 0, 0
    for line in csv_file_bin:
        try:
            parsed = parse_sms(line.decode('UTF-8').split(',')[-1])
            if parsed['card'] not in show_ignored_cards(update.message.chat_id):
                insert_transaction(update.message.chat_id,
                                   update.message.from_user['username'], parsed)
            else:
                k += 1
            i += 1
        except AttributeError:
            pass
        finally:
            j += 1

    bot.send_message(chat_id=update.message.chat_id,
        text="{} lines total, {} of them were parsed and added, {} were ignored."
        .format(j, i, k))


@restricted
def purge_db(bot, update):
    keyboard = [[InlineKeyboardButton("Yes, drop DB!", callback_data="DROPDB")],
                [InlineKeyboardButton("No, wait!", callback_data="NODROP")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text("Are you sure you want to purge an entire database?",
                              reply_markup=reply_markup)
    return PURGE_DB


def purgeuser(bot, update):
    keyboard = [[InlineKeyboardButton("Yes, drop me!", callback_data="DROPUSER")],
                [InlineKeyboardButton("No, wait!", callback_data="NODROP")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text("Are you sure you want to purge all your records?",
                              reply_markup=reply_markup)
    return PURGE_USER


@restricted
def dump_db(bot, update):
    bot.send_document(chat_id=update.message.chat_id,
                      document=open(db_path, 'rb'))


def user_data(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
                     text=str(user_data(update.message.chat_id)))


def wage_request(bot, update, args):
    """
    Takes arguments, tries to parse them and return wage for the mentioned
    period. For now: 06 2017
    """
    def is_month(text: str) -> bool:
        try:
            return 0 < int(text) < 32
        except ValueError:
            return False

    def is_year(text: str) -> bool:
        try:
            return 1999 < int(text) < 2051
        except ValueError:
            return False

    # testing for proper input
    if len(args) == 2 and is_month(args[0]) and is_year(args[1]):
        pass
    elif len(args) == 1 and (is_month(args[0]) or is_year(args[0])):
        pass
    else:
        bot.send_message(chat_id=update.message.chat_id,
                         text="Incorrect format")
        return None

    if len(args) == 2:
        month = int(args[0])
        year = int(args[1])
        start_date = datetime.datetime(year, month-1, separator_date)
        end_date = datetime.datetime(year, month, separator_date)
    elif len(args) == 1:
        if is_month(args[0]):
            month = int(args[0])
            year = datetime.datetime.now().year
            start_date = datetime.datetime(year, month-1, separator_date)
            end_date = datetime.datetime(year, month, separator_date)
        elif is_year(args[0]):
            # entire year
            year = int(args[0])
            start_date = datetime.datetime(year-1, 12, separator_date)
            end_date = datetime.datetime(year, 12, separator_date)
    else:
        now = datetime.datetime.now()
        month = now.month if now.day > separator_date else now.month - 1
        start_date = datetime.datetime(now.year, month-1, separator_date)
        end_date = datetime.datetime(now.year, month, separator_date)

    wage = wage_calc(update.message.chat_id, start_date, end_date)

    if wage is not None:
        bot.send_message(chat_id=update.message.chat_id,
                text="Your wage in that period was {:.2f}".format(wage))
    else:
        bot.send_message(chat_id=update.message.chat_id,
                         text="Sorry, we do not have any data for that period")


def modify_ignore(bot, update, args):
    """ /modignore add|remove CARD """
    if len(args) != 2 or not valid_card(args[1]):
        bot.send_message(chat_id=update.message.chat_id,
                         text="Incorrect format")
        if show_ignored_cards(update.message.chat_id):
            bot.send_message(chat_id=update.message.chat_id,
                    text="Ignored cards are:")
            bot.send_message(chat_id=update.message.chat_id,
                    text=show_ignored_cards(update.message.chat_id))
            return None
        else:
            bot.send_message(chat_id=update.message.chat_id,
                    text="No ignored cards for this user in database.")
            return None

    if args[0] == "add":
        bot.send_message(chat_id=update.message.chat_id,
            text="Adding {} to the list of ignored cards.".format(args[1]))
        insert_ignored_card(update.message.chat_id, args[1])
    elif args[0] == "remove":
        bot.send_message(chat_id=update.message.chat_id,
                         text="Removing {} from the list of ignored cards.".
                         format(args[1]))
        remove_ignored_card(update.message.chat_id, args[1])
    else:
        bot.send_message(chat_id=update.message.chat_id,
                         text="Incorrect format")


def user_info(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
        text="Your chat_id is {}, we have {} records concerning you".
        format(update.message.chat_id, user_records(update.message.chat_id)))


def purgedb_commence(bot, update):
    query = update.callback_query

    if query.data == "DROPDB":
        bot.send_message(chat_id=query.message.chat_id,
                         text="Well, you asked for it, PURGING DB")
        purge_all()


def purgeuser_commence(bot, update):
    query = update.callback_query

    if query.data == "DROPUSER":
        purge_user(query.message.chat_id)
        bot.send_message(chat_id=query.message.chat_id,
                         text="Purged your info")


def cancel(bot, update):
    query = update.callback_query

    bot.send_message(chat_id=query.message.chat_id, text="Action cancelled!")


def unknown(bot, update):
    bot.send_message(chat_id=updater.message.chat_id,
                     text="Unknown command.")


def error(bot, update, error):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, error)

# MAIN LOOP HERE


def main():
    start_handler = CommandHandler('start', start)
    userdata_handler = CommandHandler('userdata', user_data)
    dumpdb_handler = CommandHandler('dumpdb', dump_db)
    userinfo_handler = CommandHandler('userinfo', user_info)
    purgedb_handler = CommandHandler('purgedb', purge_db)
    purgeuser_handler = CommandHandler('purgeuser', purgeuser)
    wagerequest_handler = CommandHandler('wage', wage_request, pass_args=True)
    modifyignore_handler = CommandHandler('modignore', modify_ignore,
                                          pass_args=True)
    sms_handler = MessageHandler(Filters.text, sms)
    csv_handler = MessageHandler(Filters.document, csv_parse)
    purgedbcommence_handler = CallbackQueryHandler(purgedb_commence,
                                                   pattern='DROPDB')
    purgeusercommence_handler = CallbackQueryHandler(purgeuser_commence,
                                                     pattern='DROPUSER')
    cancel_handler = CallbackQueryHandler(cancel)
    unknown_handler = MessageHandler(Filters.command, unknown)

#   conv_handler = ConversationHandler(
#       entry_points=[purgedb_handler, purgeuser_handler],
#       states={
#           PURGE_USER: [CallbackQueryHandler(purgeuser_commence)],
#           PURGE_DB: [CallbackQueryHandler(purgedb_commence)]
#       },
#       fallbacks=[CommandHandler('cancel', cancel)]
#   )

    dispatcher.add_handler(start_handler)
    dispatcher.add_handler(userdata_handler)
    dispatcher.add_handler(dumpdb_handler)
    dispatcher.add_handler(userinfo_handler)
    dispatcher.add_handler(purgedb_handler)
    dispatcher.add_handler(purgeuser_handler)
    dispatcher.add_handler(sms_handler)
    dispatcher.add_handler(csv_handler)
    dispatcher.add_handler(wagerequest_handler)
    dispatcher.add_handler(modifyignore_handler)
    dispatcher.add_handler(purgeusercommence_handler)
    dispatcher.add_handler(purgedbcommence_handler)
    dispatcher.add_handler(cancel_handler)
#   dispatcher.add_handler(conv_handler)
    dispatcher.add_error_handler(error)
    # Unknown handler should go last!
    dispatcher.add_handler(unknown_handler)

    updater.start_polling()


if __name__ == '__main__':
    with sqlite3.connect(db_path, check_same_thread=False) as db:
        cursor = db.cursor()
    datatable_init()
    main()
else:
    # Don't mess with main DB if imported as a module
    db_path = os.path.join(BASE_DIR, "test.db")
    with sqlite3.connect(db_path, check_same_thread=False) as db:
        cursor = db.cursor()
    datatable_init()
