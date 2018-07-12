# sms-moneybot
SMS parsing telegram bot that keeps track of your salary

What does it do:
Keeps an SQLite DB with parsed data from SMS that you sent to the bot.

They have to be of the following type (standard SBERBANK SMS):

    VISA1234 21.12.16 22:12
    зачисление зарплаты 12345.57р
    Баланс: 16063.28р

Offers multiple commands to get statistics on your salary.

List of commands:
/userdata - returns all user db records
/userinfo - prints current chat_id and number of records in DB of a current user
/wage MM YYYY (both arguments are optional, if none given simply returns last month's data)

Admin commands:
/alldata - returns all DB records
/dumpdb - sends an ENTIRE database as a file
