import jwt
from fastapi import FastAPI, HTTPException, Header, Depends
from sqlalchemy import text
from pydantic import BaseModel
from db import DW
from passlib.hash import pbkdf2_sha512 as pl
from typing import Annotated

app = FastAPI()


class RegisterRequest(BaseModel):
    username: str
    password: str

# in string format for demonstration purposes
SECRET_KEY = 'ojg4io2jna0921pionmmlvlkjneae9p4tjn984qvmajfm809u4q3980+09093quk0qu+09t098au+´0g8qbmn00938mn5+´09q35mb'


def require_login(dw: DW, authorization = Header(None, alias="token")):
    try:
        if authorization is not None and len(authorization.split(' ')) == 2:
            validated = jwt.decode(authorization.split(' ')[1], SECRET_KEY, algorithms=['HS512'])
            user = dw.execute(text('SELECT username FROM users WHERE id = :id'),
                              {'id': validated['id']}).mappings().first()
            if user is None:
                raise HTTPException(status_code=404, detail='user not found')
            return user
        else:
            raise HTTPException(status_code=401, detail='unauthorized')
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


LoggedInUser = Annotated[dict, Depends(require_login)]


@app.get('/api/account')
async def get_account(logged_in_user: LoggedInUser):
    return logged_in_user


@app.post('/api/login')
async def login(dw: DW, req: RegisterRequest):
    _query = text("SELECT * FROM users WHERE username = :username")
    user = dw.execute(_query, {'username': req.username}).mappings().first()
    if user is None:
        raise HTTPException(status_code=404, detail='user not found')

    password_correct = pl.verify(req.password, user['password'])
    if password_correct:
        token = jwt.encode({'id': user['id']}, SECRET_KEY, algorithm='HS512')
        return {'token': token}
    raise HTTPException(status_code=404, detail='user not found')


@app.post('/api/register')
async def register(dw: DW, req: RegisterRequest):
    try:
        _query = text("INSERT INTO users (username, password) VALUES(:username, :password)")
        user = dw.execute(_query, {'username': req.username, 'password': pl.hash(req.password)})
        dw.commit()
        return {'username': req.username, 'id': user.lastrowid}
    except Exception as e:
        dw.rollback()
        print(e)
        raise HTTPException(status_code=422, detail='error registering user')


@app.get('/api/transactions/by-month-weekly/{month}/{year}')
async def get_transactions_by_month_weekly(dw: DW, month: int, year: int, logged_in_user: LoggedInUser):
    _query = text('SELECT date_dim.week, COUNT(*) AS transaction_count FROM rental_transactions_fact '
                  ' INNER JOIN date_dim on date_dim.date_key = rental_transactions_fact.rented_at_key '
                  ' WHERE date_dim.year = :year AND date_dim.month = :month GROUP BY date_dim.week ORDER BY date_dim.week DESC')
    rows = dw.execute(_query, {'year': year, 'month': month})
    data = rows.mappings().all()
    return {'data': data}


@app.get('/api/transactions/by-month-daily/{month}/{year}')
async def get_transactions_by_month_daily(dw: DW, month: int, year: int, logged_in_user: LoggedInUser):
    _query = text('SELECT date_dim.day, COUNT(*) AS transaction_count FROM rental_transactions_fact '
                  ' INNER JOIN date_dim on date_dim.date_key = rental_transactions_fact.rented_at_key '
                  ' WHERE date_dim.year = :year AND date_dim.month = :month GROUP BY date_dim.day ORDER BY date_dim.day DESC')
    rows = dw.execute(_query, {'year': year, 'month': month})
    data = rows.mappings().all()
    return {'data': data}


@app.get('/api/transactions/by-year-monthly/{year}')
async def get_transactions_by_year_monthly(dw: DW, year: int, logged_in_user: LoggedInUser):
    _query = text('SELECT date_dim.month, COUNT(*) AS transaction_count FROM rental_transactions_fact '
                  ' INNER JOIN date_dim on date_dim.date_key = rental_transactions_fact.rented_at_key '
                  ' WHERE date_dim.year = :year GROUP BY date_dim.month ORDER BY date_dim.month DESC')
    rows = dw.execute(_query, {'year': year})
    data = rows.mappings().all()
    return {'data': data}


@app.get('/api/transactions/top-of-all-time/')
async def get_transactions_top_of_all_time(dw: DW, logged_in_user: LoggedInUser):
    _query = text('SELECT rental_items_dim.name, COUNT(*) AS transaction_count FROM rental_transactions_fact '
                  ' INNER JOIN rental_items_dim ON rental_transactions_fact.rental_items_key = rental_items_dim.rental_items_key '
                  ' GROUP BY rental_transactions_fact.rental_items_key ORDER BY transaction_count DESC LIMIT 10')
    rows = dw.execute(_query)
    data = rows.mappings().all()
    return {'data': data}


@app.get('/api/transactions/top-of-year/{year}')
async def get_transactions_top_of_year(dw: DW, year: int, logged_in_user: LoggedInUser):
    total_data = []
    for i in range(12):
        _query = text('SELECT date_dim.month, rental_items_dim.name, COUNT(*) AS transaction_count FROM rental_transactions_fact '
                      ' INNER JOIN rental_items_dim ON rental_transactions_fact.rental_items_key = rental_items_dim.rental_items_key '
                      ' INNER JOIN date_dim ON rental_transactions_fact.rented_at_key = date_dim.date_key '
                      ' WHERE date_dim.year = :year AND date_dim.month = '
                      ' ' + str(i + 1) +' GROUP BY rental_transactions_fact.rental_items_key  ORDER BY transaction_count DESC LIMIT 10')
        rows = dw.execute(_query, {'year': year})
        data = rows.mappings().all()
        total_data += data
    return {'data': total_data}


@app.get('/api/items/creations-by-year-monthly/{year}')
async def get_item_creations_by_year_monthly(dw: DW, year: int, logged_in_user: LoggedInUser):
    _query = text('SELECT date_dim.month, COUNT(*) AS creation_count FROM rental_items_fact '
                  ' INNER JOIN date_dim ON date_dim.date_key = rental_items_fact.created_at_key '
                  ' WHERE date_dim.year = :year GROUP BY date_dim.month ORDER BY creation_count DESC')
    rows = dw.execute(_query, {'year': year})
    data = rows.mappings().all()
    return {'data': data}
