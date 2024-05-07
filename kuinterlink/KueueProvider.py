import interlink
import logging

from fastapi import HTTPException


class KueueProvider(interlink.provider.Provider):
    def __init__(self):
        super().__init__(None)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.logger.info("Starting KueueProvider")

    def create_pod(self,  pod: interlink.PodRequest) -> interlink.PodStatus:
        self.logger.info(f"Create pod {pod}")

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
