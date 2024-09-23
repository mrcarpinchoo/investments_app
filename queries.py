# core modules
import logging
from datetime import datetime
from os import system as sys

def getUserAccounts(session, username):
    log.info(f"Retrieving {username} accounts...")

    stmt = session.prepare(SELECT_ACCOUNTS_BY_USER)

    result = session.execute(stmt, [username])

    sys("clear")

    for row in result: print(f"Account: {row.id_account} Balance: {round(row.balance, 2)}")

    print()

    input(f"Accounts for {username} have been shown successfully! Press \"Enter\" to continue...")
# end def

def selectAccount(session, username):
    stmt = session.prepare(SELECT_ACCOUNTS_BY_USER)

    result = list(session.execute(stmt, [username]))

    for i in range(len(result)): print(f"{i + 1}. {result[i].id_account}")

    print()

    opt = int(input(f"Enter account: "))

    return result[opt - 1].id_account
# end def

def getInvestmentsByAccount(session, username):
    stmt = session.prepare(SELECT_INVESTMENTS_BY_ACCOUNT)

    id_account = selectAccount(session, username)

    sys("clear")

    result = session.execute(stmt, [id_account])

    for row in result: print(f"id_account: {row.id_account}, symbol: {row.symbol}, shares: {row.shares}")

    print()

    input(f"Investments for {id_account} account from {username} have been shown successfully! Press \"Enter\" to continue...")
# end def

def displayQueryOptions():
    print("Choose one of the following options:")

    for key in QUERY_OPTIONS.keys(): print(f"{key}: {QUERY_OPTIONS[key]}")

    print()

    return int(input("Enter your choice: "))
# end def

def queryInADateRangeFn():
    sys("clear")

    opt = displayQueryOptions()

    sys("clear")

    if opt == 1: return (False, None, None)
    if opt == 2:
        print("Select your range of dates (inclusive, with the format yyyy-mm-dd):")

        fromDate = datetime.strptime(input("From: "), "%Y-%m-%d")
        toDate = datetime.strptime(input("To: "), "%Y-%m-%d")

        return (True, fromDate, toDate)
    # end if
# end def

def getTransactions(session, username):
    id_account = selectAccount(session, username)

    (queryInADateRange, fromDate, toDate) = queryInADateRangeFn()

    stmt = session.prepare(SELECT_TRANSACTIONS_BY_ACCOUNT_DATE_RANGE if queryInADateRange else SELECT_TRANSACTIONS_BY_ACCOUNT_DATE)

    sys("clear")

    result = session.execute(stmt, [id_account, fromDate, toDate] if queryInADateRange else [id_account])

    for row in result: print(f"type: {row.type}, symbol: {row.symbol}, shares: {row.shares}, price: {round(row.price, 2)}, total: {round(row.total, 2)}")

    print()

    input(f"Transactions for {id_account} account from {username} have been shown successfully! Press \"Enter\" to continue...")
# end def

def displayTypeOptions():
    print("Choose one of the following types:")

    for key in TYPE_OPTIONS.keys(): print(f"{key}: {TYPE_OPTIONS[key]}")

    print()

    return TYPE_OPTIONS[int(input("Enter your choice: "))]
# end def

def setType():
    sys("clear")

    transactionType = displayTypeOptions()

    sys("clear")

    return transactionType
# end def

def getTransactionsByType(session, username):
    id_account = selectAccount(session, username)

    transactionType = setType()

    (queryInADateRange, fromDate, toDate) = queryInADateRangeFn()

    stmt = session.prepare(SELECT_TRANSACTIONS_BY_ACCOUNT_TYPE_DATE_RANGE if queryInADateRange else SELECT_TRANSACTIONS_BY_ACCOUNT_TYPE_DATE)

    sys("clear")

    result = session.execute(stmt, [id_account, transactionType, fromDate, toDate] if queryInADateRange else [id_account, transactionType])

    for row in result: print(f"symbol: {row.symbol}, shares: {row.shares}, price: {round(row.price, 2)}, total: {round(row.total, 2)}")

    print()

    input(f"Transactions of type {transactionType} for {id_account} account from {username} have been shown successfully! Press \"Enter\" to continue...")
# end def

def displaySymbols():
    print(f"Choose one of the following symbols:\n{', '.join(SYMBOLS)}")

    print()

    return input("Enter a symbol: ")
# end def

def setSymbol():
    sys("clear")

    symbol = displaySymbols()

    sys("clear")

    return symbol
# end def

def getTransactionsBySymbolType(session, username):
    id_account = selectAccount(session, username)

    symbol = setSymbol()

    transactionType = setType()

    (queryInADateRange, fromDate, toDate) = queryInADateRangeFn()

    stmt = session.prepare(SELECT_TRANSACTIONS_BY_ACCOUNT_SYMBOL_TYPE_DATE_RANGE if queryInADateRange else SELECT_TRANSACTIONS_BY_ACCOUNT_SYMBOL_TYPE_DATE)

    sys("clear")

    result = session.execute(stmt, [id_account, symbol, transactionType, fromDate, toDate] if queryInADateRange else [id_account, symbol, transactionType])

    for row in result: print(f"shares: {row.shares}, price: {round(row.price, 2)}, total: {round(row.total, 2)}")

    print()

    input(f"Transactions of type {transactionType} with the symbol {symbol} for {id_account} account from {username} have been shown successfully! Press \"Enter\" to continue...")
# end def

def triggerQuery(session, opt, username):
    sys("clear")

    if opt == 1: getTransactions(session, username)
    if opt == 2: getTransactionsByType(session, username)
    if opt == 3: getTransactionsBySymbolType(session, username)
    # if opt == 4: getTransactionsBySymbol(session, username)

    sys("clear")
# end def

# query statements
SELECT_ACCOUNTS_BY_USER = """
    SELECT *
    FROM accounts_by_user
    WHERE username = ?;
"""

# investments_by_account
SELECT_INVESTMENTS_BY_ACCOUNT = """
    SELECT *
    FROM investments_by_account
    WHERE id_account = ?;
"""

# transactions_by_account_date
SELECT_TRANSACTIONS_BY_ACCOUNT_DATE = """
    SELECT type, symbol, shares, price, total
    FROM transactions_by_account_date
    WHERE id_account = ?;
"""

SELECT_TRANSACTIONS_BY_ACCOUNT_DATE_RANGE = """
    SELECT type, symbol, shares, price, total
    FROM transactions_by_account_date
    WHERE id_account = ?
        AND id_transaction >= maxTimeuuid(?)
        AND id_transaction <= minTimeuuid(?);
"""

# transactions_by_account_type_date
SELECT_TRANSACTIONS_BY_ACCOUNT_TYPE_DATE = """
    SELECT type, symbol, shares, price, total
    FROM transactions_by_account_type_date
    WHERE id_account = ?
        AND type = ?;
"""

SELECT_TRANSACTIONS_BY_ACCOUNT_TYPE_DATE_RANGE = """
    SELECT type, symbol, shares, price, total
    FROM transactions_by_account_type_date
    WHERE id_account = ?
        AND type = ?
        AND id_transaction >= maxTimeuuid(?)
        AND id_transaction <= minTimeuuid(?);
"""

# transactions_by_account_symbol_type_date
SELECT_TRANSACTIONS_BY_ACCOUNT_SYMBOL_TYPE_DATE = """
    SELECT type, symbol, shares, price, total
    FROM transactions_by_account_symbol_type_date
    WHERE id_account = ?
        AND symbol = ?
        AND type = ?;
"""

SELECT_TRANSACTIONS_BY_ACCOUNT_SYMBOL_TYPE_DATE_RANGE = """
    SELECT type, symbol, shares, price, total
    FROM transactions_by_account_symbol_type_date
    WHERE id_account = ?
        AND symbol = ?
        AND type = ?
        AND id_transaction >= maxTimeuuid(?)
        AND id_transaction <= minTimeuuid(?);
"""

# global variables
QUERY_OPTIONS = {
    1: "Query all records",
    2: "Query records in a given range of dates"
}

TYPE_OPTIONS = {
    1: "buy",
    2: "sell"
}

SYMBOLS = [
    'ETSY', 'PINS', 'SE', 'SHOP', 'SQ', 'MELI', 'ISRG', 'DIS', 'BRK.A', 'AMZN',
    'VOO', 'VEA', 'VGT', 'VIG', 'MBB', 'QQQ', 'SPY', 'BSV', 'BND', 'MUB',
    'VSMPX', 'VFIAX', 'FXAIX', 'VTSAX', 'SPAXX', 'VMFXX', 'FDRXX', 'FGXX'
]

# logger
log = logging.getLogger()