#!/usr/bin/env python
import configparser
import datetime
import io
import logging
import os
import re
import csv
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
print("Reading config...")
config.read('config.ini')
try:
    separator_date = int(config['DEFAULT']['separator_date'])
    updater = Updater(token=config['DEFAULT']['token'])
    admin_list = [int(x) for x in config['DEFAULT']['admin_list'].split(',')]
    print("Configuration initialized!")
except KeyError:
    print("Make sure you copied sample config.ini and replaced TOKEN in it")
    print("Exiting...")
    exit()


# telegram bot initializing
dispatcher = updater.dispatcher

# SQLite initializing
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "data.db")

# {{{ Generic functions


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

# }}}

# {{{ SQLite retrieval functions


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
    cursor.execute('''CREATE TABLE IF NOT EXISTS ignored_cards (
               chat_id INTEGER,
               ignore_card TEXT,
               UNIQUE(chat_id, ignore_card)
               )''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS notified (
               chat_id INTEGER,
               notify INTEGER,
               UNIQUE(chat_id, notify)
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


def new_transaction(chat_id: int, username: str, sms_data: dict):
    """
    Checks whether this exact transaction is already in the database
    """
    cursor.execute("SELECT COUNT(*) FROM data \
            WHERE chat_id=:id, name=:name, card=:card,\
            date_time=:datetime, amount=:amount",
        {'id': chat_id, 'name': username, 'card': sms_data['card'],
         'datetime': sms_data['datetime'], 'amount': sms_data['amount']})
    return False if cursor.fetchone()[0] > 0 else True


def insert_ignored_card(chat_id: int, card: str):
    """
    Takes in chat_id of the convo and a card to ignore, keeps the data in DB
    """
    with db:
        cursor.execute("INSERT OR IGNORE INTO ignored_cards VALUES (:id, :card)",
                       {'id': chat_id, 'card': card})


def remove_ignored_card(chat_id: int, card: str):
    with db:
        cursor.execute("DELETE FROM ignored_cards \
                        WHERE chat_id=:id and ignore_card=:card",
                       {'id': chat_id, 'card': card})


def insert_notify(chat_id: int, notified: int):
    with db:
        cursor.execute("INSERT OR IGNORE INTO notified VALUES (:id, :notified)",
                       {'id': chat_id, 'notified': notified})


def show_ignored_cards(chat_id: int) -> tuple:
    cursor.execute("SELECT ignore_card FROM ignored_cards WHERE chat_id=:id",
                   {'id': chat_id})
    return sum(cursor.fetchall(), ())


def remove_notify(chat_id: int, notified: int):
    with db:
        cursor.execute("DELETE FROM notified WHERE chat_id=:id and notify=:card",
                       {'id': chat_id, 'notified': notified})


def to_notify(chat_id: int) -> tuple:
    cursor.execute("SELECT notify FROM notified WHERE chat_id=:id",
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
    sum_ = cursor.fetchone()[0]
    return sum_ if sum_ else 0


def purge_all():
    with db:
        cursor.execute("DELETE FROM data")


def purge_user(user: int):
    with db:
        cursor.execute("DELETE FROM data WHERE chat_id=:id", {'id': user})


def chatid_from_name(user: str) -> int:
    cursor.execute("SELECT chat_id FROM data WHERE name=:name LIMIT 1",
                  {'name': user})
    try:
        return cursor.fetchone()[0]
    except IndexError:
        return None


def name_from_chatid(chatid: int) -> str:
    cursor.execute("SELECT name FROM data WHERE chat_id=:id LIMIT 1",
                  {'id': chatid})
    try:
        return cursor.fetchone()[0][0]
    except IndexError:
        return None


# }}}

# {{{ Bot handlers


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

    if not new_transaction(update.message.chat_id,
                           update.message.from_user['username'], sms_p):
        bot.send_message(chat_id=update.message.chat_id,
            text='Record of this transaction already exists.')
        return None

    insert_transaction(update.message.chat_id,
                       update.message.from_user['username'], sms_p)
    bot.send_message(chat_id=update.message.chat_id,
                     text='Transaction added successfully!')

    for recipient in to_notify(update.message.chat_id):
        bot.send_message(chat_id=recipient,
            text='{} just added the following message: \n {}'.
            format(update.message.from_user['username'], update.message.text))

    if sms_p['datetime'].date() == datetime.datetime.now().date():
        now = datetime.datetime.now()
        month = now.month if now.day > separator_date else now.month - 1
        start_date = datetime.datetime(now.year, month-1, separator_date)
        end_date = datetime.datetime(now.year, month, separator_date)
        wage = wage_calc(update.message.chat_id, start_date, end_date)
        bot.send_message(chat_id=update.message.chat_id,
                text=f"Your last month's wage is {wage:.2f} so far")
    else:
        now = sms_p['datetime']
        month = now.month if now.day > separator_date else now.month - 1
        start_date = datetime.datetime(now.year, month-1, separator_date)
        end_date = datetime.datetime(now.year, month, separator_date)
        wage = wage_calc(update.message.chat_id, start_date, end_date)
        bot.send_message(chat_id=update.message.chat_id,
                text=f"Your wage in that month is {wage:.2f} so far")


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

    i, j, k = 0, 0, 0
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
        text=f"{j} lines total, {i} of them were parsed and added, {k} were ignored.")


def form_csv(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
                     text="Preparing CSV file...")
    bot.send_chat_action(chat_id=update.message.chat_id,
                         action=ChatAction.TYPING)
    csv_file = io.StringIO()
    csvb_file = io.BytesIO()
    csv_writer = csv.writer(csv_file)
    for row in user_data(update.message.chat_id):
        csv_writer.writerow(row)

    bot.send_message(chat_id=update.message.chat_id,
                     text="Data gathered...")

    csvb_file.write(csv_file.getvalue().encode())

    bot.send_message(chat_id=update.message.chat_id,
                     text="Data encoded...")
    csvb_file.seek(0)

    bot.send_document(chat_id=update.message.chat_id,
        filename=f"{datetime.datetime.now():%Y_%m_%d_%H.%M}-\
                   {update.message.from_user['username']}.csv",
        document=csvb_file)


@restricted
def purge_db(bot, update):
    keyboard = [[InlineKeyboardButton("Yes, drop DB!", callback_data="DROPDB")],
                [InlineKeyboardButton("No, wait!", callback_data="NODROP")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text("Are you sure you want to purge an entire database?",
                              reply_markup=reply_markup)


def purgeuser(bot, update):
    keyboard = [[InlineKeyboardButton("Yes, drop me!", callback_data="DROPUSER")],
                [InlineKeyboardButton("No, wait!", callback_data="NODROP")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text("Are you sure you want to purge all your records?",
                              reply_markup=reply_markup)


@restricted
def dump_db(bot, update):
    bot.send_document(chat_id=update.message.chat_id,
                      document=open(db_path, 'rb'))


def wage_template(bot, update, args, user=0):
    """
    Takes arguments, tries to parse them and return wage for the mentioned
    period. Example input: 06 2017
    """
    if user == 0:
        user = update.message.chat_id

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
    elif len(args) != 0:
        bot.send_message(chat_id=update.message.chat_id,
                text="Incorrect format. Should be MM YYYY, both are optional.\
                      Example: 07 2017")
        return None

    if len(args) == 2:
        month = int(args[0])
        year = int(args[1])
        if month == 12:
            start_date = datetime.datetime(year, month, separator_date)
            end_date = datetime.datetime(year+1, 1, separator_date)
        else:
            start_date = datetime.datetime(year, month-1, separator_date)
            end_date = datetime.datetime(year, month, separator_date)
    elif len(args) == 1:
        if is_month(args[0]):
            month = int(args[0])
            year = datetime.datetime.now().year
            if month == 12:
                start_date = datetime.datetime(year, month, separator_date)
                end_date = datetime.datetime(year+1, 1, separator_date)
            else:
                start_date = datetime.datetime(year, month-1, separator_date)
                end_date = datetime.datetime(year, month, separator_date)

        elif is_year(args[0]):
            # wage by month
            acc = []
            year = int(args[0])
            for month in range(1,12):
                start_date = datetime.datetime(year, month, separator_date)
                end_date = datetime.datetime(year, month+1, separator_date)
                acc.append(wage_calc(user, start_date, end_date))
            start_date = datetime.datetime(year, 12, separator_date)
            end_date = datetime.datetime(year+1, 1, separator_date)
            acc.append(wage_calc(user, start_date, end_date))
            acc = [int(x) for x in acc]
            bot.send_message(chat_id=update.message.chat_id, text=acc)
            # Total wage
            start_date = datetime.datetime(year, 1, separator_date)
            end_date = datetime.datetime(year+1, 1, separator_date)
            wage = wage_calc(user, start_date, end_date)
            bot.send_message(chat_id=update.message.chat_id,
                         text=f"Wage in that period was {wage:.2f}")
            return None
    else:
        now = datetime.datetime.now()
        year = datetime.datetime.now().year
        month = now.month if now.day > separator_date else now.month - 1
        if month == 1:
            start_date = datetime.datetime(year-1, 12, separator_date)
            end_date = datetime.datetime(year, month, separator_date)
        else:
            start_date = datetime.datetime(year, month-1, separator_date)
            end_date = datetime.datetime(year, month, separator_date)

    wage = wage_calc(user, start_date, end_date)

    if wage is not None:
        bot.send_message(chat_id=update.message.chat_id,
                         text=f"Wage in that period was {wage:.2f}")
    else:
        bot.send_message(chat_id=update.message.chat_id,
                         text="Sorry, we do not have any data for that period")


def wage_request(bot, update, args):
    """ Returns wage data for current user """
    return wage_template(bot, update, args)


@restricted
def wage_admingrequest(bot, update, args):
    """ Returns wage data for arbitrary user. Available to admins only """
    return wage_template(bot, update, args=args[1:],
                         user=chatid_from_name(args[0]))


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
        if args[1] in show_ignored_cards(update.message.chat_id):
            bot.send_message(chat_id=update.message.chat_id,
                text=f"Card {args[1]} already in the list.")
            return None
        bot.send_message(chat_id=update.message.chat_id,
            text=f"Adding {args[1]} to the list of ignored cards.")
        insert_ignored_card(update.message.chat_id, args[1])
    elif args[0] == "remove":
        if args[1] not in show_ignored_cards(update.message.chat_id):
            bot.send_message(chat_id=update.message.chat_id,
                text=f"Card {args[1]} not in the list.")
            return None
        remove_ignored_card(update.message.chat_id, args[1])
        bot.send_message(chat_id=update.message.chat_id,
            text=f"Removed {args[1]} from the list of ignored cards.")
    else:
        bot.send_message(chat_id=update.message.chat_id,
                         text="Incorrect format")


def modify_notify(bot, update, args):
    """ /modnotify add|remove notified(username or chat_id) """
    names = []
    if len(args) != 2:
        bot.send_message(chat_id=update.message.chat_id,
                         text="Incorrect format")
        if to_notify(update.message.chat_id):
            bot.send_message(chat_id=update.message.chat_id,
                    text="chat_id's of the notified are:")
            bot.send_message(chat_id=update.message.chat_id,
                    text=to_notify(update.message.chat_id))

            for id_ in to_notify(update.message.chat_id):
                names.append(name_from_chatid(id_))

            bot.send_message(chat_id=update.message.chat_id,
                    text="Usernames are:")
            bot.send_message(chat_id=update.message.chat_id,
                    text=names)
            return None
        else:
            bot.send_message(chat_id=update.message.chat_id,
                    text="Notify list is empty for this user.")
            return None

    if args[0] == "add":
        bot.send_message(chat_id=update.message.chat_id,
                         text=f"Adding {args[1]} to the list of notified.")
        if args[1].isdigit():
            insert_notify(update.message.chat_id, args[1])
        elif chatid_from_name(args[1]):
            insert_notify(update.message.chat_id, chatid_from_name(args[1]))
        else:
            bot.send_message(chat_id=update.message.chat_id,
                             text=f"User {args[1]} not found in the database")
            bot.send_message(chat_id=update.message.chat_id,
                             text=to_notify(update.message.chat_id))

    elif args[0] == "remove":
        if args[1] not in to_notify(update.message.chat_id):
            bot.send_message(chat_id=update.message.chat_id,
                             text=f"User {args[1]} not in the list of notified")
            return None

        bot.send_message(chat_id=update.message.chat_id,
                         text=f"Removing {args[1]} from the list of notified.")
        if args[1].isdigit():
            remove_notify(update.message.chat_id, args[1])
            bot.send_message(chat_id=update.message.chat_id,
                             text="Successfuly removed {args[1]}!")
        elif chatid_from_name(args[1]):
            remove_notify(update.message.chat_id, chatid_from_name(args[1]))
            bot.send_message(chat_id=update.message.chat_id,
                             text="Successfuly removed {args[1]}!")
        else:
            bot.send_message(chat_id=update.message.chat_id,
                             text=f"User {args[1]} not found in the database")

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
                         text="Well, you asked for it, purging database...")
        purge_all()
        bot.send_message(chat_id=query.message.chat_id, text="DONE!")


def purgeuser_commence(bot, update):
    query = update.callback_query

    if query.data == "DROPUSER":
        bot.send_message(chat_id=query.message.chat_id,
                         text="Purging your info from the database...")
        purge_user(query.message.chat_id)
        bot.send_message(chat_id=query.message.chat_id, text="DONE!")


def cancel(bot, update):
    query = update.callback_query

    bot.send_message(chat_id=query.message.chat_id, text="Action cancelled!")


def unknown(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
                     text="Unknown command.")


def error(bot, update, error):
    """Log Errors caused by Updates."""
    logger.warning('Update "%s" caused error "%s"', update, error)

# }}}

# ------------------------ MAIN LOOP -------------------------------


def main():
    print("Adding handlers...")
    start_handler = CommandHandler('start', start)
    userdata_handler = CommandHandler('userdata', user_data)
    dumpdb_handler = CommandHandler('dumpdb', dump_db)
    purgedb_handler = CommandHandler('purgedb', purge_db)
    purgeuser_handler = CommandHandler('purgeuser', purgeuser)
    wagerequest_handler = CommandHandler('wage', wage_request, pass_args=True)
    wageadminrequest_handler = CommandHandler('wagedb', wage_admingrequest,
                                         pass_args=True)
    modifyignore_handler = CommandHandler('modignore', modify_ignore,
                                          pass_args=True)
    modifynotify_handler = CommandHandler('modnotify', modify_notify,
                                          pass_args=True)
    formcsv_handler = CommandHandler('formcsv', form_csv)
    sms_handler = MessageHandler(Filters.text, sms)
    csv_handler = MessageHandler(Filters.document, csv_parse)
    purgedbcommence_handler = CallbackQueryHandler(purgedb_commence,
                                                   pattern='DROPDB')
    purgeusercommence_handler = CallbackQueryHandler(purgeuser_commence,
                                                     pattern='DROPUSER')
    cancel_handler = CallbackQueryHandler(cancel)
    unknown_handler = MessageHandler(Filters.command, unknown)

    dispatcher.add_handler(start_handler)
    dispatcher.add_handler(userdata_handler)
    dispatcher.add_handler(dumpdb_handler)
    dispatcher.add_handler(purgedb_handler)
    dispatcher.add_handler(purgeuser_handler)
    dispatcher.add_handler(sms_handler)
    dispatcher.add_handler(csv_handler)
    dispatcher.add_handler(wagerequest_handler)
    dispatcher.add_handler(wageadminrequest_handler)
    dispatcher.add_handler(modifyignore_handler)
    dispatcher.add_handler(modifynotify_handler)
    dispatcher.add_handler(formcsv_handler)
    dispatcher.add_handler(purgeusercommence_handler)
    dispatcher.add_handler(purgedbcommence_handler)
    dispatcher.add_handler(cancel_handler)
    dispatcher.add_error_handler(error)
    # Unknown handler should go last!
    dispatcher.add_handler(unknown_handler)

    print("Bot is ready!")
    updater.start_polling()


if __name__ == '__main__':
    with sqlite3.connect(db_path, check_same_thread=False) as db:
        cursor = db.cursor()
    print("Database initialized!")
    datatable_init()
    main()
else:
    # Don't mess with main DB if imported as a module
    db_path = os.path.join(BASE_DIR, "test.db")
    with sqlite3.connect(db_path, check_same_thread=False) as db:
        cursor = db.cursor()
    datatable_init()


# vim:foldmethod=marker
