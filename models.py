# core modules
import logging

def createKeyspace(session, keyspaceName, replicationFactor):
    log.info(f"Creating keyspace {keyspaceName}, with replication factor {replicationFactor}...")

    session.execute(CREATE_KEYSPACE.format(keyspaceName, replicationFactor))
# end def

def createSchemas(session):
    log.info("Creating schemas...")
    
    session.execute(CREATE_TABLE_ACCOUNTS_BY_USER)
    session.execute(CREATE_TABLE_INVESTMENTS_BY_ACCOUNT)
    session.execute(CREATE_TABLE_TRANSACTIONS_BY_ACCOUNT_DATE)
    session.execute(CREATE_TABLE_TRANSACTIONS_BY_ACCOUNT_TYPE_DATE)
    session.execute(CREATE_TABLE_TRANSACTIONS_BY_ACCOUNT_SYMBOL_TYPE_DATE)
    session.execute(CREATE_TABLE_TRANSACTIONS_BY_ACCOUNT_SYMBOL_DATE)
# end def

# keyspace
CREATE_KEYSPACE = """
    CREATE KEYSPACE IF NOT EXISTS {}
    WITH replication = {{
        'class': 'SimpleStrategy',
        'replication_factor': {} 
    }};
"""

# schemas
# accounts_by_user
CREATE_TABLE_ACCOUNTS_BY_USER = """
    CREATE TABLE IF NOT EXISTS accounts_by_user (
        username TEXT,
        id_account TEXT,
        name TEXT,
        balance DECIMAL,
        PRIMARY KEY ((username), id_account)
    );
"""

# investments_by_account
CREATE_TABLE_INVESTMENTS_BY_ACCOUNT = """
    CREATE TABLE IF NOT EXISTS investments_by_account (
        id_account TEXT,
        symbol TEXT,
        shares INT,
        PRIMARY KEY ((id_account), symbol)
    );
"""

# transactions_by_account_date
CREATE_TABLE_TRANSACTIONS_BY_ACCOUNT_DATE = """
    CREATE TABLE IF NOT EXISTS transactions_by_account_date (
        id_account TEXT,
        id_transaction TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares INT,
        price DECIMAL,
        total DECIMAL,
        PRIMARY KEY ((id_account), id_transaction)
    ) WITH CLUSTERING ORDER BY (id_transaction DESC);
"""

# transactions_by_account_type_date
CREATE_TABLE_TRANSACTIONS_BY_ACCOUNT_TYPE_DATE = """
    CREATE TABLE IF NOT EXISTS transactions_by_account_type_date (
        id_account TEXT,
        id_transaction TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares INT,
        price DECIMAL,
        total DECIMAL,
        PRIMARY KEY ((id_account), type, id_transaction)
    ) WITH CLUSTERING ORDER BY (type ASC, id_transaction DESC);
"""

# transactions_by_account_symbol_type_date
CREATE_TABLE_TRANSACTIONS_BY_ACCOUNT_SYMBOL_TYPE_DATE = """
    CREATE TABLE IF NOT EXISTS transactions_by_account_symbol_type_date (
        id_account TEXT,
        id_transaction TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares INT,
        price DECIMAL,
        total DECIMAL,
        PRIMARY KEY ((id_account), symbol, type, id_transaction)
    ) WITH CLUSTERING ORDER BY (symbol ASC, type ASC, id_transaction DESC);
"""

# transactions_by_account_symbol_date
CREATE_TABLE_TRANSACTIONS_BY_ACCOUNT_SYMBOL_DATE = """
    CREATE TABLE IF NOT EXISTS transactions_by_account_symbol_date (
        id_account TEXT,
        id_transaction TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares INT,
        price DECIMAL,
        total DECIMAL,
        PRIMARY KEY ((id_account), symbol, id_transaction)
    ) WITH CLUSTERING ORDER BY (symbol ASC, id_transaction DESC);
"""

# logger
log = logging.getLogger()