class Base(object):
    def __str__(self):
        return 'Unimplemented Statistic Service'

    def write_points(self, name, fields):
        raise NotImplementedError()

    def query(self, name, fields, span, end, interval):
        raise NotImplementedError()
