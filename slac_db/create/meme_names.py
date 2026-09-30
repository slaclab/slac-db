import slac_db.directory_service

def to_directory_service_db():
    """ Build  device DB with SQLAlchemy.
    """
    return slac_db.directory_service.recreate(_Parser())

class _Parser:
    """Container for DB row data.
    """
    def __init__(self):
        self.addresses = dict()
        self._get_from_meme()

    def _get_from_meme(self):
        import meme.names
        address_list = meme.names.list_pvs("%", timeout=600)
        for a in address_list:
            part = a.split(':')
            if len(part) < 4:
                continue
            self.addresses[(part[0], part[1], part[2])] = a
