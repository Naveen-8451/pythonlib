import json


class MetaDataJsonConverter(json.JSONEncoder):
    def default(self, o):
        try:
            return o.toJSON()
        except AttributeError:
            return ''

