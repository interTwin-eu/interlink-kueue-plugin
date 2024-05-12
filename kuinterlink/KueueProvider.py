import uuid
import traceback

import interlink
import logging
from typing import Union, Collection

from pprint import pformat
from fastapi import HTTPException

from kubernetes_asyncio.client.models import V1ContainerState, V1Pod
from kubernetes_asyncio.client.exceptions import ApiException
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

    @staticmethod
    def generate_volume_id(volume_name: str, pod_name: str, namespace: str):
        """Internal. Return a readable unique id used to name the pod."""
        uid = str(uuid.uuid4()).replace("-", "")[:30]
        short_name = '-'.join((namespace[:10], pod_name[:10], volume_name[:10]))[:32]
        return '-'.join((short_name, uid))

    async def create_job(self,  pod: interlink.PodRequest, volumes: Collection[interlink.Volume]) -> str:
        """
        Create a kueue job containing the pod
        """
        self.logger.info(f"Create pod {pod.metadata.name}.{pod.metadata.namespace} [{pod.metadata.uid}]")

        config_map_manifests = []
        secret_manifests = []

        for volume_to_mount in pod.spec.volumes:
            if volume_to_mount.configMap is not None:
                original_name = volume_to_mount.configMap.name
                new_name = self.generate_volume_id(original_name, pod.metadata.name, pod.metadata.namespace)

                # Update the name of the volume in the pod manifest
                volume_to_mount.configMap.name = new_name
                for container in volumes:
                    if container.configMaps is not None:
                        for config_map in container.configMaps:
                            if config_map.metadata.name == original_name:
                                config_map_manifests.append(
                                    parse_template(
                                        'ConfigMap',
                                        **config_map.dict(exclude_none=True),
                                        name=new_name,
                                        namespace=cfg.NAMESPACE,
                                        job_name=self.get_readable_uid(pod),
                                    ))

            if volume_to_mount.secret is not None:
                original_name = volume_to_mount.secret.secretName
                new_name = self.generate_volume_id(original_name, pod.metadata.name, pod.metadata.namespace)

                # Update the name of the volume in the pod manifest
                volume_to_mount.secret.secretName = new_name
                for container in volumes:
                    if container.secrets is not None:
                        for secret in container.secrets:
                            if secret.metadata.name == original_name:
                                secret_manifests.append(
                                    parse_template(
                                        'Secret',
                                        **secret.dict(exclude_none=True),
                                        name=new_name,
                                        namespace=cfg.NAMESPACE,
                                        job_name=self.get_readable_uid(pod),
                                    ))

        job_desc = pod.dict(exclude_none=True)
        job_desc.update(name=self.get_readable_uid(pod), namespace=cfg.NAMESPACE, queue=cfg.QUEUE)

        parsed_request = parse_template('Job', job=job_desc)

        logging.debug("\n\n\n\n CREATE POD: \n\n\n " + pformat(parsed_request) + "\n\n\n\n")

        async with kubernetes_api('core') as k8s:
            for volume_manifest in config_map_manifests:
                logging.debug("\n\n\n\n CREATE CONFIG MAP: \n\n\n " + pformat(volume_manifest) + "\n\n\n\n")
                response = await k8s.create_namespaced_config_map(cfg.NAMESPACE, body=volume_manifest)
                logging.debug(f"Defining config_map {volume_manifest['metadata']['name']}")
                logging.debug(response)

            for volume_manifest in secret_manifests:
                logging.debug("\n\n\n\n CREATE SECRET: \n\n\n " + pformat(volume_manifest) + "\n\n\n\n")
                response = await k8s.create_namespaced_secret(cfg.NAMESPACE, body=volume_manifest)
                logging.debug(f"Defining secret {volume_manifest['metadata']['name']}")
                logging.debug(response)

        async with kubernetes_api('custom_object') as k8s:
            response = await k8s.create_namespaced_custom_object(
                group='batch',
                version='v1',
                namespace=cfg.NAMESPACE,
                plural='jobs',
                body=parsed_request
            )

        logging.debug(f"Defining job {parsed_request['metadata']['name']}")
        logging.debug(response)

        return "ok"

    async def delete_pod(self, pod: interlink.PodRequest) -> None:
        async with kubernetes_api('custom_object') as k8s:
            await k8s.delete_namespaced_custom_object(
                group="batch",
                version="v1",
                namespace=cfg.NAMESPACE,
                plural='jobs',
                name=self.get_readable_uid(pod)
            )

        async with kubernetes_api('core') as k8s:
            pods = await k8s.list_namespaced_pod(
                namespace=cfg.NAMESPACE,
                label_selector=f"job-name={self.get_readable_uid(pod)}"
            )
            pods = pods.items

            if len(pods) > 0:
                self.logger.info(
                    f"Delete pods: {', '.join([p.metadata.name for p in pods])}"
                )

            for pod_ in pods:
                await k8s.delete_namespaced_pod(
                    name=pod_.metadata.name,
                    namespace=pod_.metadata.namespace,
                )

            config_maps = await k8s.list_namespaced_config_map(
                namespace=cfg.NAMESPACE,
                label_selector=f"job-name={self.get_readable_uid(pod)}"
            )
            config_maps = config_maps.items

            if len(config_maps) > 0:
                self.logger.info(
                    f"Delete config maps: {', '.join([cm.metadata.labels['original-name'] for cm in config_maps])}"
                )

            for config_map in config_maps:
                await k8s.delete_namespaced_config_map(
                    name=config_map.metadata.name,
                    namespace=config_map.metadata.namespace,
                )

            secrets = await k8s.list_namespaced_secret(
                namespace=cfg.NAMESPACE,
                label_selector=f"job-name={self.get_readable_uid(pod)}"
            )
            secrets = secrets.items

            if len(secrets) > 0:
                self.logger.info(
                    f"Delete secrets: {', '.join([s.metadata.labels['original-name'] for s in secrets])}"
                )

            for secret in secrets:
                await k8s.delete_namespaced_secret(
                    name=secret.metadata.name,
                    namespace=secret.metadata.namespace,
                )

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
                    startedAt=container_state.running.started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                )
            )

        if container_state.waiting is not None:
            message = container_state.waiting.message
            reason = container_state.waiting.reason
            return interlink.ContainerStates(
                waiting=interlink.StateWaiting(
                    message=message if message is not None else "Pending",
                    reason=reason if reason is not None else "Unknown",
                )
            )

        return interlink.ContainerStates(
            waiting=interlink.StateWaiting(
                message="Pending",
                reason="Unknown",
            )
        )

    @staticmethod
    async def _is_job_suspended(job_name: str) -> bool:
        """
        Return True if the job.spec.suspend is true. If true, kueue scheduled the job.
        """
        async with kubernetes_api('batch') as k8s:
            job = await k8s.read_namespaced_job(
                namespace=cfg.NAMESPACE,
                name=job_name
            )

            logging.getLogger("is_job_suspended").debug(
                f"job.spec.suspend: {job.spec.suspend} (boolean: {job.spec.suspend == True})"
            )

            return job.spec.suspend

    @staticmethod
    def _pending_job_status(pod: interlink.PodRequest) -> interlink.PodStatus:
        """
        Formats a PodStatus indicating the job has not been scheduled, yet.
        """
        all_containers = pod.spec.containers
        if pod.spec.initContainers is not None:
            all_containers += pod.spec.initContainers

        return interlink.PodStatus(
            name=pod.metadata.name,
            UID=pod.metadata.uid,
            namespace=pod.metadata.namespace,
            containers=[
                interlink.ContainerStatus(
                    name=c.name,
                    state=interlink.ContainerStates(
                        waiting=interlink.StateWaiting(
                            message="Pending",
                            reason="Execution enqueued",
                        ),
                    )
                ) for c in all_containers
            ]
        )

    async def get_pod_status(self, pod: interlink.PodRequest) -> interlink.PodStatus:
        self.logger.info(f"Status of pod {pod.metadata.name}.{pod.metadata.namespace} [{pod.metadata.uid}]")
        try:
            if await self._is_job_suspended(self.get_readable_uid(pod)):
                return self._pending_job_status(pod)

            async with kubernetes_api('core') as k8s:
                pods = await k8s.list_namespaced_pod(
                    namespace=cfg.NAMESPACE,
                    label_selector=f"job-name={self.get_readable_uid(pod)}"
                )
        except ApiException as e:
            self.logger.error(traceback.format_exception(e))
            return interlink.PodStatus()


        container_statuses = (sum([p.status.container_statuses for p in pods.items], [])
                              if len(pods.items) > 0 else [])

        self.logger.debug(f"Container statuses: " + pformat(container_statuses))

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

        if await self._is_job_suspended(self.get_readable_uid(log_request)):
            return f"Job scheduled in queue `{cfg.QUEUE}`."

        async with kubernetes_api('core') as k8s:
            pods = await k8s.list_namespaced_pod(
                namespace=cfg.NAMESPACE,
                label_selector=f"job-name={self.get_readable_uid(log_request)}"
            )

            if len(pods.items) > 1:
                raise HTTPException(501, "Too many pods for a single job")

            selected_pod: V1Pod = pods.items[0]

            if selected_pod.status.phase in ['Pending']:
                return "Pod execution is scheduled, but still pending."

            return await k8s.read_namespaced_pod_log(
                name=selected_pod.metadata.name,
                namespace=cfg.NAMESPACE,
                container=log_request.ContainerName,
                # tail_lines=log_request.Opts.Tail,
                # limit_bytes=log_request.Opts.LimitBytes,
                # timestamps=log_request.Opts.Timestamps,
                # previous=log_request.Opts.Previous,
                # since_seconds=log_request.Opts.SinceSeconds,
            )

