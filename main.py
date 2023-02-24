import contextlib
import datetime
import random
import time

from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, String, create_engine, insert, BigInteger, DateTime
from sqlalchemy.orm import Session

from faker import Faker

Base = declarative_base()
faker = Faker()


class User(Base):
    __tablename__ = "User"
    id = Column('id', BigInteger, primary_key=True)
    name = Column('name', String)
    email = Column('email', String)
    residence = Column('residence', String)
    gender = Column('gender', String)
    birthdate = Column('birthdate', DateTime)


@contextlib.contextmanager
def sqlalchemy_session(future):
    engine = create_engine(
        'mysql+pymysql://davidxxi21:35xxxv!!!@clickstream.ccorjew9awjd.ap-northeast-2.rds.amazonaws.com:3306/clickstream',
        future=future, echo=False)
    sess = Session(
        bind=engine, future=future, autoflush=False, expire_on_commit=False
    )
    yield sess
    sess.close()






if __name__ == '__main__':
    domain = ['@gmail.com', '@naver.com', '@daum.net']
    gender = ["male", "female", "unknown"]


    for loop_iteration in range(3, 10):

        with sqlalchemy_session(future=True) as session:
            with session.bind.begin() as conn:
                t0 = time.time()
                conn.execute(
                    insert(User.__table__),
                    [{"id": i,
                      "name": faker.name(),
                      "email": faker.first_name()+str(random.randint(1,10000)) +domain[random.randint(0, 2)],
                      "residence": faker.address(),
                      "gender": gender[random.randint(0, 2)],
                      "birthdate": faker.date_between_dates(date_start=datetime.date(1950, 1, 1),
                                                            date_end=datetime.date(2005, 12, 31))} for i in
                     range(1000000 * loop_iteration, 1000000 * (loop_iteration + 1))],
                )
                conn.commit()
                print("SQLA Core", time.time() - t0)
