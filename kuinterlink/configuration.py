import os

DEBUG = os.environ.get("DEBUG", "true").lower() in ['y', 'yes', 'true']

NAMESPACE = os.environ.get("NAMESPACE", "ilnk-work")
QUEUE = os.environ.get("QUEUE", "interlink")
