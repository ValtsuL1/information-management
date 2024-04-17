from typing import Annotated
from fastapi import Depends
import contextlib
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session


@contextlib.contextmanager
def get_db(cnx_type='oltp'):
    _db = None
    try:
        if cnx_type == 'oltp':
            cnx_str = 'mysql+mysqlconnector://root:@localhost/laplanduas_rental'
        else:
            cnx_str = 'mysql+mysqlconnector://root:@localhost/laplanduas_rental_olap'
        engine = create_engine(cnx_str)
        db_session = sessionmaker(bind=engine)
        _db = db_session()
        yield _db
    except Exception as e:
        print(e)
    finally:
        if _db is not None:
            _db.close()


def get_dw():
    _dw = None
    try:
        engine = create_engine('mysql+mysqlconnector://root:@localhost/laplanduas_rental_olap')
        dw_session = sessionmaker(bind=engine)
        _dw = dw_session()
        yield _dw
    except Exception as e:
        print(e)
    finally:
        if _dw is not None:
            _dw.close()


DW = Annotated[Session, Depends(get_dw)]