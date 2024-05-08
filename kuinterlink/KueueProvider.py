import interlink
import logging

from pprint import pformat
from fastapi import HTTPException

from .kubernetes_client import initialize_k8s
from .kubernetes_client import kubernetes_api
from .parse_template import parse_template
from . import configuration as cfg


class KueueProvider(interlink.provider.Provider):
    def __init__(self):
        super().__init__(None)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.logger.info("Starting KueueProvider")
        initialize_k8s()

    async def create_pod(self,  pod: interlink.Pod) -> str:
        self.logger.info(f"Create pod {pod.pod.metadata.name}.{pod.pod.metadata.namespace} [{pod.pod.metadata.uid}]")

        parsed_request = parse_template(
            'Job',
            job=pod.pod.dict(
                exclude_none=True,
                include=dict(
                    metadata=dict(name=pod.pod.metadata.uid, namespace=cfg.NAMESPACE, queue=cfg.QUEUE)
                )
            )
        )
        logging.debug("\n\n CREATE POD: \n " + pformat(parsed_request))

        async with kubernetes_api('custom_object') as k8s:
            response = await k8s.create_namespaced_custom_object(
                group='batch',
                version='v1',
                namespace=cfg.NAMESPACE,
                plural='jobs',
                body=parsed_request
            )

        logging.debug(response)

        # async with kubernetes_api() as k8s:
        #     ret = await k8s.list_pod_for_all_namespaces()
        #     logging.debug(ret)

        return "ok"

    def delete_pod(self, pod: interlink.PodRequest) -> None:
        try:
            self.logger.info(f"Delete pod {pod}")
        except:
            raise HTTPException(status_code=404, detail="No containers found for UUID")
        return

    def get_pod_status(self, pod: interlink.PodRequest) -> interlink.PodStatus:
        self.logger.info(f"Retrieve status of pod {pod}")

    def get_pod_logs(self, pod: interlink.PodRequest) -> interlink.PodStatus:
        self.logger.info(f"Retrieve logs of pod {pod}")
