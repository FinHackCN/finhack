from runtime.constant import *
import runtime.global_var as global_var
from finhack.library.class_loader import ClassLoader

import os
import importlib
import finhack.library.log as Log
import runtime.global_var as global_var
from finhack.library.mydb import mydb
from flask import Flask, send_from_directory



class DefaultTrader:
    def run(self):
        
 