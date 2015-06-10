from flask.ext.sqlalchemy import SQLAlchemy

db = SQLAlchemy()
DB_STRING_TYPE = db.Unicode(256)
DB_TEXT_TYPE = db.TEXT(length=1 << 20, convert_unicode=True)


def init_db(app):
    db.init_app(app)
    db.app = app
    db.create_all()


class Base(db.Model):
    __abstract__ = True
    __table_args__ = {'mysql_charset': 'utf8'}

    id = db.Column('id', db.Integer, primary_key=True, autoincrement=True)
