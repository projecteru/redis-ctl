from base import db, Base

TYPE_REDIS = 0


class ContainerImage(Base):
    __tablename__ = 'cont_image'

    type = db.Column(db.Integer, index=True, default=TYPE_REDIS,
                     nullable=False)
    name = db.Column(db.String(255), nullable=False, index=True, unique=True)
    description = db.Column(db.String(255))
    creation = db.Column(db.DateTime)


def list_redis():
    return ContainerImage.query.all()


def add_redis_image(name, description, creation):
    i = ContainerImage(name=name, description=description, creation=creation)
    db.session.add(i)
    db.session.flush()
    return i


def del_redis_image(id):
    db.session.delete(ContainerImage.query.get(id))
