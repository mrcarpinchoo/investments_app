# core modules
import random, datetime
from os import system as sys
from uuid import uuid4

# third-party modules
from cassandra.query import BatchStatement

# third-party modules
import time_uuid

def executeBatch(session, stmt, data):
    # for i in range(0, len(data), BATCH_SIZE):
    #     batch = BatchStatement()

    #     for item in data[i : i + BATCH_SIZE]: batch.add(stmt, item)
    # # end for-in

    for i in range(len(data)):
        batch = BatchStatement()

        for item in data: batch.add(stmt, [*item])
    # end for-in

    session.execute(batch)
# end def

def getRandomDate(startDate, endDate):
    deltaTime = endDate - startDate

    deltaDays = deltaTime.days

    randomNumOfDays = random.randrange(deltaDays)

    randomDate = startDate + datetime.timedelta(days = randomNumOfDays)

    return time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(randomDate))
# end def

def bulkInsert(session):
    data = []
    accounts = []
    
    accountsNum, investmentsByAccount, transactionsByAccount = 10, 100, 100 # number of records to generate

    # INSERT statements preparation
    accByUserStmt = session.prepare(INSERT_INTO_ACCOUNTS_BY_USER)
    invByAccStmt = session.prepare(INSERT_INTO_INVESTMENTS_BY_ACCOUNT)
    traByAccDateStmt = session.prepare(INSERT_INTO_TRANSACTIONS_BY_ACCOUNT_DATE)
    traByAccTypeDateStmt = session.prepare(INSERT_INTO_TRANSACTIONS_BY_ACCOUNT_TYPE_DATE)
    traByAccSymTypeDateStmt = session.prepare(INSERT_INTO_TRANSACTIONS_BY_ACCOUNT_SYMBOL_TYPE_DATE)
    traByAccSymDateStmt = session.prepare(INSERT_INTO_TRANSACTIONS_BY_ACCOUNT_SYMBOL_DATE)
   
    # generates sample accounts by user
    for i in range(accountsNum):
        (username, name) = random.choice(USERS)

        id_account = str(uuid4())
        
        balance = round(random.uniform(0.0, 100000.0), 1)

        data.append((username, id_account, name, balance)) # appends the record in a tuple format

        accounts.append(id_account)
    # end for-in

    executeBatch(session, accByUserStmt, data)
    
    # generates sample investments by account
    data = []
    id_account_symbol = {}

    for i in range(investmentsByAccount):
        while True:
            id_account = random.choice(accounts)
            symbol = random.choice(SYMBOLS)

            if id_account + '_' + symbol not in id_account_symbol:
                id_account_symbol[id_account + '_' + symbol] = True

                shares = random.randint(1, 500)

                data.append((id_account, symbol, shares))
                
                break
            # end if
        # end while
    # end for-in

    executeBatch(session, invByAccStmt, data)

    # generate sample trades by account
    data = []

    for i in range(transactionsByAccount):
        id_transaction = getRandomDate(datetime.datetime(2013, 1, 1), datetime.datetime(2022, 8, 31))
        
        id_account = random.choice(accounts)

        symbol = random.choice(SYMBOLS)
        
        transaction_type = random.choice(TRANSACTION_TYPES)

        shares = random.randint(1, 500)

        price = round(random.uniform(0.1, 100000.0), 2)
        
        total = round(shares * price, 2)

        data.append((id_account, id_transaction, transaction_type, symbol, shares, price, total))
    # end for-in
    
    executeBatch(session, traByAccDateStmt, data)
    executeBatch(session, traByAccTypeDateStmt, data)
    executeBatch(session, traByAccSymTypeDateStmt, data)
    executeBatch(session, traByAccSymDateStmt, data)

    sys("clear")

    input("Sample data has been inserted successfully! Press \"Enter\" to continue...")
# end def

# INSERT statements
# accounts_by_user
INSERT_INTO_ACCOUNTS_BY_USER = """
    INSERT INTO accounts_by_user (username, id_account, name, balance)
    VALUES (?, ?, ?, ?)
"""

# investments_by_account
INSERT_INTO_INVESTMENTS_BY_ACCOUNT = """
    INSERT INTO investments_by_account (id_account, symbol, shares)
    VALUES (?, ?, ?)
"""

# transactions_by_account_date
INSERT_INTO_TRANSACTIONS_BY_ACCOUNT_DATE = """
    INSERT INTO transactions_by_account_date (id_account, id_transaction, type, symbol, shares, price, total) VALUES(?, ?, ?, ?, ?, ?, ?)
"""

# transactions_by_account_type_date
INSERT_INTO_TRANSACTIONS_BY_ACCOUNT_TYPE_DATE = """
    INSERT INTO transactions_by_account_type_date (id_account, id_transaction, type, symbol, shares, price, total) VALUES(?, ?, ?, ?, ?, ?, ?)
"""

# transactions_by_account_symbol_type_date
INSERT_INTO_TRANSACTIONS_BY_ACCOUNT_SYMBOL_TYPE_DATE = """
    INSERT INTO transactions_by_account_symbol_type_date (id_account, id_transaction, type, symbol, shares, price, total) VALUES(?, ?, ?, ?, ?, ?, ?)
"""

# transactions_by_account_symbol_date
INSERT_INTO_TRANSACTIONS_BY_ACCOUNT_SYMBOL_DATE = """
    INSERT INTO transactions_by_account_symbol_date (id_account, id_transaction, type, symbol, shares, price, total) VALUES(?, ?, ?, ?, ?, ?, ?)
"""

# sample data
USERS = [
    ('mike', 'Michael Jones'),
    ('stacy', 'Stacy Malibu'),
    ('john', 'John Doe'),
    ('marie', 'Marie Condo'),
    ('tom', 'Tomas Train')
]

SYMBOLS = [
    'ETSY', 'PINS', 'SE', 'SHOP', 'SQ', 'MELI', 'ISRG', 'DIS', 'BRK.A', 'AMZN',
    'VOO', 'VEA', 'VGT', 'VIG', 'MBB', 'QQQ', 'SPY', 'BSV', 'BND', 'MUB',
    'VSMPX', 'VFIAX', 'FXAIX', 'VTSAX', 'SPAXX', 'VMFXX', 'FDRXX', 'FGXX'
]

# global variables
BATCH_SIZE = 10

TRANSACTION_TYPES = ['buy', 'sell']