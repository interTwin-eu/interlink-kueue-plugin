import interlink
import logging


class KueueProvider(interlink.provider.Provider):
    def __init__(self):
        super().__init__(None)
        self.logger = logging.getLogger(self.__class__.__name__)
        
        self.logger.info("Starting KueueProvider")


    def Create(self,  pod: interlink.PodRequest) -> interlink.PodStatus:
        self.logger(f"Status {pod}")


    def Delete(self, pod: interlink.PodRequest) -> None:
        try:
          self.logger(f"DELETE {pod}")
        except:
            raise HTTPException(status_code=404, detail="No containers found for UUID")
        return


    def Status(self,  pod: interlink.PodRequest) -> interlink.PodStatus:
        self.logger(f"Status {pod}")


    def Logs(self,  pod: interlink.PodRequest) -> interlink.PodStatus:
        self.logger(f"Logs {pod}")






