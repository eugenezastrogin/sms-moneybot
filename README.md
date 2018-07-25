# sms-moneybot

SMS parsing telegram bot that keeps track of your salary

## What does it do:

Keeps an SQLite DB with parsed data from SMS that you sent to the bot.
Initial database can be formed by sending CSV file made in SMS to Text Android app.
Just send it to the bot and all legit SMS will be added to the database.

Monthly updates can be simply sent to the bot as a text message copied from the Messaging app.

They have to be of the following type (standard SBERBANK SMS):

    VISA1234 21.12.16 22:12
    зачисление зарплаты 12345.57р
    Баланс: 16063.28р

## Available commands

Offers multiple commands to get statistics on your salary.

__List of commands:__

/userdata - returns all user db records

/userinfo - prints current chat_id and number of records in DB of a current user

/wage MM YYYY (both arguments are optional, if none given simply returns last month's data)

/modignore add|remove CARD - manages list of ignored cards, records with card numbers in it won't be added through CSV import or text messages

/modnotify add|remove notify - manages list of users to notify when you add an SMS,
useful if you share a budget

/purgeuser - deletes all transaction records of user

/formcsv - sends CSV with all user records


__Admin commands:__

/wagedb user MM YYYY - returns the same output as /wage does but for arbitrary user
present in database, specified by a username

/purgedb - purges an entire database

/dumpdb - sends an ENTIRE database as a file
