import interlink
import logging
from typing import Union, Collection

from pprint import pformat
from fastapi import HTTPException

from kubernetes_asyncio.client.models import V1ContainerState
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

    @staticmethod
    def get_readable_uid(pod: Union[interlink.PodRequest, interlink.LogRequest]):
        """Internal. Return a readable unique id used to name the pod."""
        if isinstance(pod, interlink.PodRequest):
            name = pod.metadata.name
            namespace = pod.metadata.namespace
            uid = pod.metadata.uid
        elif isinstance(pod, interlink.LogRequest):
            name = pod.PodName
            namespace = pod.Namespace
            uid = pod.PodUID
        else:
            raise HTTPException(500, f"Unexpected pod or log request of type {type(pod)}")

        short_name = '-'.join((namespace, name))[:20]
        return '-'.join((short_name, uid))

    async def create_job(self,  pod: interlink.PodRequest, volumes: Collection[interlink.Volume]) -> str:
        """
        Create a kueue job containing the pod
        """
        self.logger.info(f"Create pod {pod.metadata.name}.{pod.metadata.namespace} [{pod.metadata.uid}]")

        job_desc = pod.dict(exclude_none=True)
        job_desc.update(name=self.get_readable_uid(pod), namespace=cfg.NAMESPACE, queue=cfg.QUEUE)

        parsed_request = parse_template('Job', job=job_desc)

        logging.debug("\n\n\n\n CREATE POD: \n\n\n " + pformat(parsed_request) + "\n\n\n\n")

        async with kubernetes_api('custom_object') as k8s:
            response = await k8s.create_namespaced_custom_object(
                group='batch',
                version='v1',
                namespace=cfg.NAMESPACE,
                plural='jobs',
                body=parsed_request
            )

        logging.debug(response)

        return "ok"

    def delete_pod(self, pod: interlink.PodRequest) -> None:
        try:
            self.logger.info(f"Delete pod {pod}")
        except:
            raise HTTPException(status_code=404, detail="No containers found for UUID")
        return

    @staticmethod
    def create_container_states(container_state: V1ContainerState) -> interlink.ContainerStates:
        if container_state.terminated is not None:
            reason = container_state.terminated.reason
            return interlink.ContainerStates(
                terminated=interlink.StateTerminated(
                    exitCode=container_state.terminated.exit_code,
                    reason=reason if reason is not None else "Terminated.",
                )
            )

        if container_state.running is not None:
            return interlink.ContainerStates(
                running=interlink.StateRunning(
                    startedAt=container_state.running.started_at,
                )
            )

        if container_state.waiting is not None:
            return interlink.ContainerStates(
                waiting=interlink.StateWaiting(
                    message=container_state.waiting.message,
                    reason=container_state.waiting.reason,
                )
            )

        return interlink.ContainerStates(
            waiting=interlink.StateWaiting(
                message="Pending",
                reason="Unknown",
            )
        )

    async def get_pod_status(self, pod: interlink.PodRequest) -> interlink.PodStatus:
        self.logger.info(f"Status of pod {pod.metadata.name}.{pod.metadata.namespace} [{pod.metadata.uid}]")
        async with kubernetes_api('core') as k8s:
            # job = await k8s.get_namespaced_custom_object(
            #     group='batch',
            #     version='v1',
            #     namespace=cfg.NAMESPACE,
            #     plural='jobs',
            #     name=self.get_readable_uid(pod)
            # )
            pods = await k8s.list_namespaced_pod(
                namespace=cfg.NAMESPACE,
                label_selector=f"job-name={self.get_readable_uid(pod)}"
            )

        container_statuses = sum([p.status.container_statuses for p in pods.items], [])

        return interlink.PodStatus(
            name=pod.metadata.name,
            UID=pod.metadata.uid,
            namespace=pod.metadata.namespace,
            containers=[
                interlink.ContainerStatus(
                    name=cs.name,
                    state=self.create_container_states(cs.state),
                ) for cs in container_statuses
            ]
        )

    async def get_pod_logs(self, log_request: interlink.LogRequest) -> str:
        self.logger.info(f"Log of pod {log_request.PodName}.{log_request.Namespace} [{log_request.PodUID}]")

        async with kubernetes_api('core') as k8s:
            return await k8s.read_namespaced_pod_log(
                name=self.get_readable_uid(log_request),
                namespace=cfg.NAMESPACE,
                container=log_request.ContainerName,
                tail_lines=log_request.Opts.Tail,
                limit_bytes=log_request.Opts.LimitBytes,
                timestamps=log_request.Opts.Timestamps,
                previous=log_request.Opts.Previous,
                since_seconds=log_request.Opts.SinceSeconds,
            )

