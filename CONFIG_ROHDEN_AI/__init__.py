from flask import Blueprint
import os

mod_name = os.path.basename(os.path.dirname(__file__)).lower()
bp_name = f"{mod_name}_bp"
config_rohden_ai_bp = Blueprint(bp_name, __name__, template_folder='templates')

import importlib
importlib.import_module('.routes', package=__name__)
