#!/usr/bin/env python
# _*_ coding: utf-8 _*_
from telegram.ext import (Updater, CommandHandler, MessageHandler, Filters, CallbackQueryHandler)
from functools import wraps
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, ChatAction
import logging, configparser, sqlite3, re, datetime, signal, os, io

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
    зарплаты
    \D*                       # optional separator is any number of non digits
    (\d+\.*\d*)                             # matching amount
    ''', re.VERBOSE)

    parsed_sms = sms_pattern.search(text).groups()
    sms_data['card'] = parsed_sms[0]
    sms_data['datetime'] = datetime.datetime.strptime(parsed_sms[1], "%d.%m.%y %H:%M")
    sms_data['amount'] = float(parsed_sms[2])

    return sms_data

# SQLite retrieval functions


def datatable_init():
    """
    creates if necessary data.db SQLite database
    """
    cursor.execute('''CREATE TABLE IF NOT EXISTS data (
               chat_id INTEGER,
               name TEXT,
               card TEXT,
               date_time DATETIME,
               amount REAL,
               UNIQUE(chat_id, name, card, date_time, amount)
               )''')
    db.commit()


def insert_transaction(chat_id: int, username: str, sms_data: dict):
    """
    takes in chat_id of the convo and parsed sms dictionary, writes data into DB
    """
    with db:
        cursor.execute("INSERT OR IGNORE INTO data VALUES (:id, :name, :card, :datetime, :amount)",
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


def new_card(chat_id: str, card: str) -> bool:
    """
    checks whether this card type and number already in DB for this user
    """
    cursor.execute("SELECT COUNT(*) FROM data WHERE chat_id=:id AND card=:card",
            {'id': chat_id, 'card': card})
    return False if cursor.fetchone()[0] > 0 else True


def user_records(chat_id: str) -> list:
    """
    returns number of relevant user records
    """
    cursor.execute("SELECT COUNT(*) FROM data WHERE chat_id=:id", {'id': chat_id})
    return cursor.fetchone()[0]


def wage_calc(chat_id: str, start_date, end_date) -> list:
    cursor.execute("SELECT SUM(amount) FROM data WHERE chat_id=:id \
                    AND date_time BETWEEN :sd and :ed",
                  {'id': chat_id, 'sd': start_date, 'ed': end_date})
    return cursor.fetchone()[0]


def purge_all():
    with db:
        cursor.execute("DELETE FROM data")


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
    on receiving SMS, tries to parse it and add to DB, otherwise reprompts user.
    Also shows accumulative wage in a month that SMS was from
    """
    try:
        sms_p = parse_sms(update.message.text)
    except AttributeError:
        bot.send_message(chat_id=update.message.chat_id,
                         text='Unable to parse. Please, send valid SMS!')
        return None

    # EXPERIMENTAL MENU PART

#   if new_card(update.message.chat_id, sms['card']):
#       bot.send_message(chat_id=update.message.chat_id,
#            text="I don't have any records with this particular card number.")

#       button_list = [InlineKeyboardButton("Yes, add it.", callback_data="1"),
#                      InlineKeyboardButton("No, trash it!", callback_data="0")]
#       reply_markup = InlineKeyboardMarkup(button_list)

#       update.message.reply_text("Are you sure everything is correct?",
#                        reply_markup=reply_markup)

    # ENF OF MENU

    insert_transaction(update.message.chat_id, update.message.from_user['username'], sms_p)
    bot.send_message(chat_id=update.message.chat_id,
                     text='Transaction added successfully!')

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

    i,j = 0, 0
    for line in csv_file_bin:
        try:
            parsed = parse_sms(line.decode('UTF-8').split(',')[-1])
            insert_transaction(update.message.chat_id,
                               update.message.from_user['username'], parsed)
            i += 1
            j += 1
        except AttributeError:
            j += 1
    bot.send_message(chat_id=update.message.chat_id,
                     text="{} lines total, {} of them were parsed and added.".format(j, i))


@restricted
def purge_db(bot, update):
    keyboard = [[InlineKeyboardButton("Yes, drop it!", callback_data="DROP")],
                [InlineKeyboardButton("No, wait!", callback_data="0")]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    update.message.reply_text("Are you sure you want to purge an entire database?",
                              reply_markup=reply_markup)


@restricted
def dump_db(bot, update):
    bot.send_document(chat_id=update.message.chat_id,
                      document=open(db_path, 'rb'))


def user_data(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
                     text=str(user_data(update.message.chat_id)))


def wage_request(bot, update, args):
    """ takes arguments, tries to parse them and return wage for the mentioned
    period
    for now: 06 2017"""
    if len(args) == 0:
        now = datetime.datetime.now()
        month = now.month if now.day > separator_date else now.month - 1
        start_date = datetime.datetime(now.year, month-1, separator_date)
        end_date = datetime.datetime(now.year, month, separator_date)
    elif len(args) == 1:
        if len(args[0]) > 2:
            # entire year
            pass
        else:
            month = args[0]
            year = datetime.datetime.now().year
            month = now.month if now.day > separator_date else now.month - 1
            start_date = datetime.datetime(year, month-1, separator_date)
            end_date = datetime.datetime(year, month, separator_date)
    else:
        month = args[0]
        year = args[1]
        start_date = datetime.datetime(year, month-1, separator_date)
        end_date = datetime.datetime(year, month, separator_date)

    wage = wage_calc(update.message.chat_id, start_date, end_date)
    bot.send_message(chat_id=update.message.chat_id,
                     text="Your wage in that period was {}".format(wage))


def user_info(bot, update):
    bot.send_message(chat_id=update.message.chat_id,
                 text="Your chat_id is {}, we have {} records concerning you".
                 format(update.message.chat_id, user_records(update.message.chat_id)))


def button(bot, update):
    query = update.callback_query

    if query.data == "DROP":
    #   bot.send_message(chat_id=update.message.chat_id,
    #                    text="Well, you asked for it, PURGING DB")
        purge_all()


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
    wagerequest_handler = CommandHandler('wage', wage_request, pass_args=True)
    sms_handler = MessageHandler(Filters.text, sms)
    csv_handler = MessageHandler(Filters.document, csv_parse)
    button_handler = CallbackQueryHandler(button)
    unknown_handler = MessageHandler(Filters.command, unknown)

    dispatcher.add_handler(start_handler)
    dispatcher.add_handler(userdata_handler)
    dispatcher.add_handler(dumpdb_handler)
    dispatcher.add_handler(userinfo_handler)
    dispatcher.add_handler(purgedb_handler)
    dispatcher.add_handler(sms_handler)
    dispatcher.add_handler(csv_handler)
    dispatcher.add_handler(wagerequest_handler)
    dispatcher.add_handler(button_handler)
    dispatcher.add_error_handler(error)
    # unknown handler should go last!
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
