import os

DEBUG = os.environ.get("DEBUG", "true").lower() in ['y', 'yes', 'true']
