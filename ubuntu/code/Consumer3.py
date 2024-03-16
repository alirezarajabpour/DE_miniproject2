from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, Table
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect
from datetime import datetime
import json
from kafka import KafkaConsumer

Base = declarative_base()

class UserInfo(Base):
    __tablename__ = 'users_info'
    id = Column(String, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    gender = Column(String)
    address = Column(String)
    post_code = Column(Integer)
    email = Column(String)
    username = Column(String)
    dob = Column(DateTime)
    registered_date = Column(DateTime)
    phone = Column(String)
    picture = Column(String)
    timestamp = Column(DateTime)
    label = Column(String)

def store_data_in_postgres(data):
    db_host = 'root_db'
    db_port = '5432'
    db_name = 'root_db'
    db_user = 'postgres'
    db_password = 'password'

    try:
        engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
        if not inspect(engine).has_table(UserInfo.__tablename__):
            Base.metadata.create_all(engine)

        Session = sessionmaker(bind=engine)
        session = Session()

        timestamp_datetime = datetime.fromtimestamp(data['timestamp'])

        insert_query = UserInfo.__table__.insert().values(
            id=data['id'],
            first_name=data['first_name'],
            last_name=data['last_name'],
            gender=data['gender'],
            address=data['address'],
            post_code=data['post_code'],
            email=data['email'],
            username=data['username'],
            dob=datetime.fromisoformat(data['dob'].replace('Z', '')),
            registered_date=datetime.fromisoformat(data['registered_date'].replace('Z', '')),
            phone=data['phone'],
            picture=data['picture'],
            timestamp=timestamp_datetime,
            label=data['label']
        )
        session.execute(insert_query)
        session.commit()
        print("Data have been saved successfully.")
    except Exception as e:
        print(f"Error in connection: {e}")
    finally:
        session.close()

def consumer3():
    consumer = KafkaConsumer('users_info_timestamp_label', bootstrap_servers=['broker:29092'])
    for message in consumer:
        data = json.loads(message.value)
        store_data_in_postgres(data)
        print(data)

if __name__ == "__main__":
    consumer3()
