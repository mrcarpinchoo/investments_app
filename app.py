#!/usr/bin/env python3

# core modules
from os import getenv, system as sys
import logging, random

# third-party modules
from cassandra.cluster import Cluster

# custom modules
import models, data, queries

def setUsername():
    username = input('Enter your username: ')

    log.info(f"Username set to {username}.")

    return username
# end def

def displayMenu():
    options = {
        0: "Exit",
        1: "Show accounts",
        2: "Show investments",
        3: "Show transactions",
        4: "Populate tables",
        5: "Change username",
    }
    
    for key in options.keys(): print(f"{key}: {options[key]}")

    print()
# end def

def showTransactionsMenu():
    options = {
        0: "Exit",
        1: "All transactions. (optional date range, defaults to latest 30 days)",
        2: "Transactions by type (buy or sell). (optional date range, defaults to latest 30 days)",
        3: "Transactions by type (buy or sell) with instrument symbol. (optional date range, defaults to latest 30 days)",
        4: "Transactions by symbol. (optional date range, defaults to latest 30 days)",
    }

    for key in options.keys(): print(f"{key}: {options[key]}")

    print()
# end def

def get_instrument_value(instrument):
    instr_mock_sum = sum(bytearray(instrument, encoding='utf-8'))
    return random.uniform(1.0, instr_mock_sum)

# environment variables
CLUSTER_IP_ADDRESSES = getenv('CASSANDRA_CLUSTER_IP_ADDRESSES', 'localhost')
KEYSPACE_NAME = getenv('CASSANDRA_KEYSPACE_NAME', 'investments')
REPLICATION_FACTOR = getenv('CASSANDRA_REPLICATION_FACTOR', '1')

# logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

def main():
    sys("clear")

    log.info(f"Connecting to {CLUSTER_IP_ADDRESSES}...")

    cluster = Cluster(CLUSTER_IP_ADDRESSES.split(",")) # Cassandra cluster

    session = cluster.connect() # returns a collection of connection pools for each host in the cluster

    models.createKeyspace(session, KEYSPACE_NAME, REPLICATION_FACTOR) # creates the keyspace

    session.set_keyspace(KEYSPACE_NAME) # sets the default keyspace for the current session

    models.createSchemas(session) # creates schemas

    username = setUsername()

    sys("clear")

    while(True):
        displayMenu()

        opt = int(input("Enter your choice: "))

        sys("clear")

        if opt == 0: break
        if opt == 1: queries.getUserAccounts(session, username)
        if opt == 2: queries.getInvestmentsByAccount(session, username)
        if opt == 3:
            showTransactionsMenu()
            
            opt = int(input('Enter your choice: '))

            queries.triggerQuery(session, opt, username)
        if opt == 4: data.bulkInsert(session)
        if opt == 5: username = setUsername()

        sys("clear")
    # end while

    cluster.shutdown() # closes all sessions and connections associated with this cluster
# end def

if __name__ == '__main__': main()