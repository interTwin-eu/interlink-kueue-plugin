import interlink
import logging

from fastapi import HTTPException

from .kubernetes_client import initialize_k8s


class KueueProvider(interlink.provider.Provider):
    def __init__(self):
        super().__init__(None)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.logger.info("Starting KueueProvider")
        initialize_k8s()

    def create_pod(self,  pod: interlink.Pod) -> str:
        self.logger.info(f"Create pod {pod.pod.metadata.name}.{pod.pod.metadata.namespace} [{pod.pod.metadata.uid}]")
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
