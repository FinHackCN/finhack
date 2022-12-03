import os

class util:
    @staticmethod
    def mypath():
        path = os.path.dirname(os.path.dirname(__file__))
        return path