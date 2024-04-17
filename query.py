from sqlalchemy import text
from db import get_db
from collections import Counter


months = ["Tammikuu", "Helmikuu", "Maaliskuu", "Huhtikuu", "Toukokuu", "Kesäkuu", "Heinäkuu", "Elokuu", "Syyskuu", "Lokakuu", "Marraskuu", "Joulukuu"]


def query():
    while True:
        decision = int(input("[1] Lainauksien määrä [2] Tavarat\n"))
        if decision == 1:
            search_by_rents()
        if decision == 2:
            search_by_items()


def search_by_rents():
    decision = int(input("[1] Vuodelta [2] Kuukaudelta\n"))
    with get_db('olap') as _dw:
        result = []
        transactions = _get_rental_transactions_dates(_dw)

        if decision == 1:
            search_year = int(input("Vuosi:\n"))
            for transaction in transactions:
                if transaction['year'] == search_year:
                    result.append(transaction['month'])
            result.sort()
            counts = dict(Counter(result))

            for month, amount in counts.items():
                print(f"{months[month - 1]}: {amount} kpl")

        if decision == 2:
            decision = int(input("[1] Viikoittain [2] Päivittäin\n"))
            search_year = int(input("Vuosi:\n"))
            search_month = int(input("Kuukausi:\n"))
            if decision == 1:
                for transaction in transactions:
                    if transaction['year'] == search_year and transaction['month'] == search_month:
                        result.append(transaction['week'])
            result.sort()
            counts = dict(Counter(result))

            for week, amount in counts.items():
                print(f"Viikko {week}: {amount} kpl")

            if decision == 2:
                for transaction in transactions:
                    if transaction['year'] == search_year and transaction['month'] == search_month:
                        result.append(transaction['day'])
            result.sort()
            counts = dict(Counter(result))

            for day, amount in counts.items():
                print(f"Päivä {day}: {amount} kpl")


def search_by_items():
    decision = int(input("[1] Lisätyt tavarat [2] Lainatuimmat tavarat\n"))
    with get_db(cnx_type='olap') as _dw:
        result = []
        if decision == 1:
            creation_dates = _get_item_creation_dates(_dw)
            search_year = int(input("Vuosi:\n"))
            for date in creation_dates:
                if date['year'] == search_year:
                    result.append(date['month'])
            result.sort()
            counts = dict(Counter(result))
            print(counts)
            highest_month = max(zip(counts.values(), counts.keys()))[1]
            highest_month_amount = counts[highest_month]

            print(f"Vuosi {search_year} {months[highest_month - 1]}: {highest_month_amount}")

        if decision == 2:
            decision = int(input("[1] Vuodelta [2] Koko aikana\n"))
            if decision == 1:
                search_year = int(input("Vuosi:\n"))
                rented_items = _get_rented_items_by_year(_dw, search_year)
                index = 0
                printed_list = []
                # lisää index muuttujaa 1 joka iteraatiossa, jos index on 10 tai alle tulostaa indexin, joka toimii järjestyslukuna, ja tuotteen tiedot
                # lisäksi kuukausi printataan jos se ei ole printed_list listassa, samalla index palautetaan 1
                for item in rented_items:
                    index += 1
                    if item['month'] not in printed_list:
                        print()
                        print(months[item['month'] - 1])
                        print("---------------------------")
                        printed_list.append(item['month'])
                        index = 1
                    if index <= 10:
                        print(f"{index}: {item['name']}, {item['amount']} kpl")

            if decision == 2:
                rented_items = _get_rented_items_all_time(_dw)
                i = 0
                for item in rented_items:
                    i += 1
                    if i > 10:
                        break
                    print(f"{i}: {item['name']}, {item['amount']} kpl")


def _get_rental_transactions_dates(_dw):
    _query = text('SELECT rented_at_key, date_dim.year AS year, date_dim.month AS month, date_dim.week AS week, date_dim.day AS day FROM rental_transactions_fact INNER JOIN date_dim ON date_dim.date_key = rental_transactions_fact.rented_at_key')
    rows = _dw.execute(_query)
    return rows.mappings().all()


def _get_item_creation_dates(_dw):
    _query = text('SELECT created_at_key, date_dim.year AS year, date_dim.month AS month, date_dim.week AS week, date_dim.day AS day FROM rental_items_fact INNER JOIN date_dim ON date_dim.date_key = rental_items_fact.created_at_key')
    rows = _dw.execute(_query)
    return rows.mappings().all()


def _get_rented_items_by_year(_dw, year):
    _query = text('SELECT rental_items_dim.name, date_dim.month, COUNT(rental_transactions_fact.rental_items_key) AS amount FROM rental_transactions_fact INNER JOIN rental_items_dim ON rental_transactions_fact.rental_items_key = rental_items_dim.rental_items_key INNER JOIN date_dim ON rental_transactions_fact.rented_at_key = date_dim.date_key WHERE date_dim.year = :year GROUP BY rental_transactions_fact.rental_items_key ORDER BY date_dim.month, amount DESC')
    rows = _dw.execute(_query, {'year': year})
    return rows.mappings().all()


def _get_rented_items_all_time(_dw):
    _query = text('SELECT rental_items_dim.name, COUNT(rental_transactions_fact.rental_items_key) AS amount FROM rental_transactions_fact INNER JOIN rental_items_dim ON rental_transactions_fact.rental_items_key = rental_items_dim.rental_items_key GROUP BY rental_transactions_fact.rental_items_key ORDER BY amount DESC')
    rows = _dw.execute(_query)
    return rows.mappings().all()
