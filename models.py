from sqlalchemy import Column, Integer, String, DateTime, Float, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()


class User(Base):
    __tablename__ = "user_table"

    uid = Column(Integer, primary_key=True, nullable=False)
    nice_total_disk_usage = Column(String(256), nullable=False)
    username = Column(String(256), nullable=False)
    is_admin = Column(Boolean, nullable = False)
    quota_percent = Column(Float, nullable=False)
    total_disk_usage = Column(Float, nullable=False)
    purged = Column(Boolean, nullable = False)
    quota = Column(String(256), nullable=False)
    email = Column(String(256), nullable=False)
    id = Column(String(256), nullable=False)
    deleted = Column(Boolean, nullable = False)

    def __init__(self, dictionary):
        self.__dict__.update(dictionary)
    
    def update(self, dictionary):
        dictionary.pop('uid', None)
        self.__dict__.update(dictionary)

    def __repr__(self):
        return '<Galaxy User {}>'.format(self.id)


class HistoryNotification(Base):
    __tablename__ = 'history_notification_table'

    id = Column(Integer, primary_key=True, nullable=False)
    h_id = Column('history_id', String(256), ForeignKey('history_table.id'), nullable=False)
    h_date = Column(DateTime, nullable=False)
    n_id = Column('notification_id', Integer, ForeignKey('notification_table.id'), nullable=False)


class History(Base):
    __tablename__ = "history_table"

    hid = Column(Integer, primary_key=True, nullable=False)
    update_time = Column(DateTime, nullable=False)
    size = Column(Float, nullable=False)
    name = Column(String(256), nullable=False)
    id = Column(String(256), nullable=False)
    user_id = Column(String(256), ForeignKey('user_table.id'), nullable=True)
    status = Column(String(256))

    def __init__(self, dictionary):
        self.__dict__.update(dictionary)

    def update(self, dictionary):
        dictionary.pop('hid', None)
        self.__dict__.update(dictionary)

    def __repr__(self):
        return '<Galaxy History {}>'.format(self.id)


class Notification(Base):
    __tablename__ = "notification_table"

    id = Column(Integer, primary_key=True, nullable=False)
    user_id = Column(String(256), ForeignKey('user_table.id'), nullable=False)
    message_id = Column(String(256), ForeignKey('message_table.message_id'))
    sent = Column(DateTime, nullable=False)
    status = Column(String(256), nullable=False)
    type = Column(String(64), nullable=False)


class Message(Base):
    __tablename__ = "message_table"

    id = Column(Integer, primary_key=True, nullable=False)
    message_id = Column(Integer, nullable=False)
    status = Column(String(256), nullable=False)