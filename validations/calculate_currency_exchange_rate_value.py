from search_api.settings import waller_session, CASSANDRA_WALLET_KEYSPACE
currency_exchange_rate = {}
waller_session.set_keyspace(CASSANDRA_WALLET_KEYSPACE)


def cal_currency_exchange_rate():
    response_casandra = waller_session.execute("""SELECT * FROM currencyExchangeRate""")
    if not response_casandra:
        pass
    else:
        for fav in response_casandra:
            new_currency = str(fav.from_currency)+"_"+str(fav.to_currency)
            try:
                currency_exchange_rate[new_currency] = float(fav.exchange_rate)
            except:
                pass

    return currency_exchange_rate