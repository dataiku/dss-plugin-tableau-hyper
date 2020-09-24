class InvalidPluginParameter(Exception):
    """
    Raised when the input plugin parameter is invalid
    """
    def __init__(self, variable_name, variable_value):
        self.message = "Variable {} with value `{}` is invalid. Please refer to the documentation of this plugin."
        super().__init__(self.message.format(variable_name, variable_value))
