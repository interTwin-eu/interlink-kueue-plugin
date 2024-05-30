import os
from contextlib import asynccontextmanager
import traceback
import logging
import json
from typing import Literal, Optional, List

import kubernetes_asyncio as k8s
from kubernetes_asyncio.client.api_client import ApiClient
from fastapi import HTTPException


__API_GROUPS__ = dict(
    core=k8s.client.CoreV1Api,
    custom_object=k8s.client.CustomObjectsApi,
    batch=k8s.client.BatchV1Api,
)

ApiGroup = Literal['core', 'custom_object', 'batch']


def initialize_k8s():
    logger = logging.getLogger("Kubernetes")
    if os.environ.get("KUBECONFIG", "") == "":
        logger.info(f"Initialization in *incluster* mode")
        k8s.config.load_incluster_config()
    else:
        logger.info(f"Initialization in *remote* mode (with KUBECONFIG)")
        k8s.config.load_kube_config()


@asynccontextmanager
async def kubernetes_api(group: ApiGroup = 'core', ignored_statuses: Optional[List[int]] = None):
    logger = logging.getLogger("Kubernetes")
    async with ApiClient() as api_client:
        try:
            yield __API_GROUPS__[group](api_client)
        except k8s.client.exceptions.ApiException as exception:
            if ignored_statuses is not None and exception.status in ignored_statuses:
                raise exception
            try:
                body = json.loads(exception.body)
            except json.JSONDecodeError:
                logger.error("HTTP error not returning a JSON.")
                logger.error(traceback.print_exc())
                raise HTTPException(exception.status, f"({exception.reason}) {exception.body}")
            else:
                logger.error(f"Error {exception.status} ({exception.reason})")
                message = body.get("message", "Kubernetes error")
                logger.error(message)
                logger.error(traceback.print_exc())
                raise HTTPException(exception.status, message)
        except Exception as e:
            logger.error(str(e))
            logger.error(traceback.print_exc())
            raise HTTPException(500, "Unknown kubernetes error")

