import mysql.connector.errors
from sqlalchemy import text
from db import get_db
from query import query


def _clear_all(_dw):
    _dw.execute(text('SET FOREIGN_KEY_CHECKS = 0'))
    _dw.execute(text('DELETE FROM rental_items_dim'))
    _dw.execute(text('DELETE FROM date_dim'))
    _dw.execute(text('DELETE FROM rental_items_fact'))
    _dw.execute(text('DELETE FROM rental_transactions_fact'))
    _dw.execute(text('SET FOREIGN_KEY_CHECKS = 1'))
    _dw.commit()


def _get_rental_items(_db):
    _query = text('SELECT rental_items.id AS rental_items_id, rental_items.name, description, serial_number, categories.id AS categories_id, categories.name AS categories_name FROM rental_items INNER JOIN categories ON categories.id = rental_items.categories_id')
    rows = _db.execute(_query)
    items = rows.mappings().all()
    return items


def rental_items_etl():
    with get_db() as _db:
        items = _get_rental_items(_db)
    with get_db(cnx_type='olap') as _dw:
        try:
            for item in items:
                _query = text('INSERT INTO rental_items_dim(rental_items_id, name, description, serial_number, categories_id, categories_name) VALUES(:rental_items_id, :name, :description, :serial_number, :categories_id, :categories_name)')
                _dw.execute(_query, {'rental_items_id': item['rental_items_id'],
                                     'name': item['name'],
                                     'description': item['description'],
                                     'serial_number': item['serial_number'],
                                     'categories_id': item['categories_id'],
                                     'categories_name': item['categories_name']})
                _dw.commit()
        except Exception as e:
            _dw.rollback()
            print(e)


def _get_rental_transactions_dates(_db):
    _query = text('SELECT DISTINCT created_at AS dt FROM rental_transactions')
    rows = _db.execute(_query)
    date_rows = rows.mappings().all()
    dates = []
    for row in date_rows:
        dates.append(row['dt'])
    return dates


def _get_rental_items_dates(_db):
    _query = text('SELECT DISTINCT created_at AS dt FROM rental_items')
    rows = _db.execute(_query)
    date_rows = rows.mappings().all()
    dates = []
    for row in date_rows:
        dates.append(row['dt'])
    return dates


def date_etl():
    with get_db() as _db:
        rental_transactions_dates = _get_rental_transactions_dates(_db)
        rental_items_dates = _get_rental_items_dates(_db)

        all_dates = rental_transactions_dates + rental_items_dates
        unique_dates_set = set(all_dates)
        unique_dates = list(unique_dates_set)

    with get_db(cnx_type='olap') as _dw:
        try:
            _query = text('INSERT INTO date_dim(year, month, week, day, hour, min, sec) VALUES(:year, :month, :week, :day, :hour, :min, :sec)')

            for date in unique_dates:
                _dw.execute(_query, {'year': date.year,
                                     'month': date.month,
                                     'week': date.isocalendar().week,
                                     'day': date.day,
                                     'hour': date.hour,
                                     'min': date.minute,
                                     'sec': date.second})
            _dw.commit()
        except Exception as e:
            _dw.rollback()
            print(e)


def _get_rental_transactions_for_fact(_db):
    _query = text('SELECT id, created_at, rental_items_id FROM rental_transactions')
    rows = _db.execute(_query)
    return rows.mappings().all()


def _get_rental_items_for_fact(_db):
    _query = text('SELECT id AS rental_items_id, created_at FROM rental_items')
    rows = _db.execute(_query)
    return rows.mappings().all()


def _get_rental_items_dims(_dw):
    _query = text('SELECT * FROM rental_items_dim')
    rows = _dw.execute(_query)
    return rows.mappings().all()


def _get_rental_items_key(oltp_item, items):
    for item in items:
        if oltp_item['rental_items_id'] == item['rental_items_id']:
            return item['rental_items_key']
    return None


def _get_date_dims(_dw):
    _query = text('SELECT * FROM date_dim')
    rows = _dw.execute(_query)
    return rows.mappings().all()


def _get_date_key(oltp_item, dates):
    date = oltp_item['created_at']
    for d in dates:
        if date.year == d['year'] and date.month == d['month'] and date.isocalendar().week == d['week'] and date.hour == d['hour'] and date.minute == d['min'] and date.second == d['sec']:
            return d['date_key']
    return None


def rental_transactions_fact_etl():
    with get_db() as _db:
        rental_transactions = _get_rental_transactions_for_fact(_db)
    with get_db(cnx_type='olap') as _dw:
        rental_items_dims = _get_rental_items_dims(_dw)
        date_dims = _get_date_dims(_dw)
        for transaction in rental_transactions:
            try:
                _date_key = _get_date_key(transaction, date_dims)
                _rental_items_key = _get_rental_items_key(transaction, rental_items_dims)
                if _date_key is None or _rental_items_key is None:
                    continue
                _query = text('INSERT INTO rental_transactions_fact(rental_items_key, rented_at_key) VALUES(:rental_items_key, :rented_at_key)')
                _dw.execute(_query, {'rental_items_key': _rental_items_key,
                                     'rented_at_key': _date_key})
                _dw.commit()
            except Exception as e:
                print(e)
                _dw.rollback()


def rental_items_fact_etl():
    with get_db() as _db:
        rental_items = _get_rental_items_for_fact(_db)
    with get_db(cnx_type='olap') as _dw:
        try:
            rental_items_dims = _get_rental_items_dims(_dw)
            date_dims = _get_date_dims(_dw)

            for item in rental_items:
                _date_key = _get_date_key(item, date_dims)
                _rental_items_key = _get_rental_items_key(item, rental_items_dims)
                if _date_key is None or _rental_items_key is None:
                    continue
                _query = text('INSERT INTO rental_items_fact(rental_items_key, created_at_key) VALUES(:rental_items_key, :created_at_key)')
                _dw.execute(_query, {'rental_items_key': _rental_items_key,
                                     'created_at_key': _date_key})
            _dw.commit()
        except Exception as e:
            print("2")
            print(e)
            _dw.rollback()


def main():
    while True:
        decision = int(input("[1] Vie data [2] Hae data [0] Lopeta\n"))
        if decision == 1:
            with get_db('olap') as _dw:
                _clear_all(_dw)
            rental_items_etl()
            date_etl()
            rental_transactions_fact_etl()
            rental_items_fact_etl()
        elif decision == 2:
            query()
        elif decision == 0:
            break


if __name__ == '__main__':
    main()