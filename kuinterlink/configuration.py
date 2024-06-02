import os

DEBUG = os.environ.get("DEBUG", "true").lower() in ['y', 'yes', 'true']

NAMESPACE = os.environ.get("NAMESPACE", "ilnk-work")
QUEUE = os.environ.get("QUEUE", "interlink")
CVMFS_CLAIM_NAME = os.environ.get("CVMFS_CLAIM_NAME", None)
CVMFS_HOST_PATH = os.environ.get("CVMFS_HOST_PATH", None)

if CVMFS_CLAIM_NAME and CVMFS_HOST_PATH:
    raise ValueError("CVMFS_CLAIM_NAME and CVMFS_HOST_PATH are mutually exclusive.")
