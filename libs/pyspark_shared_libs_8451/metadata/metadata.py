import json


class ColumnMeta(object):
    def __init__(self, col_name):
        self.columnName = col_name

    def toJSON(self):
        return json.dumps(self.columnName)


class InputMeta(object):
    def __init__(self, input_name, input_value):
        self.inputName = input_name
        self.inputValue = input_value

    def toJSON(self):
        return {'name': self.inputName, 'inputType': self.inputValue}


class OutputMeta(object):
    def __init__(self, output_name, output_value):
        self.outputName = output_name
        self.outputValue = output_value

    def toJSON(self):
        return {'name': self.outputName, 'value': self.outputValue}