import logging
from typing import List
import os
import signal

from fastapi import FastAPI, HTTPException, Request
import interlink

from kuinterlink import KueueProvider
from kuinterlink import configuration as cfg
from kuinterlink.kubernetes_client import kubernetes_api

# Initialize FastAPI app
app = FastAPI()

# Please Take my provider and handle the interLink REST layer for me
kueue_provider = KueueProvider()

log_format = '%(asctime)-22s %(name)-10s %(levelname)-8s %(message)-90s'
logging.basicConfig(
    format=log_format,
    level=logging.DEBUG if cfg.DEBUG else logging.INFO,
)
logging.debug("Enabled debug mode.")

@app.post("/create")
async def create_pod(pods: List[interlink.Pod]) -> str:
    for pod in pods:
        await kueue_provider.create_job(pod.pod, pod.container)
    return "Pod created\n"

@app.post("/delete")
async def delete_pod(pod: interlink.PodRequest) -> str:
    kueue_provider.delete_pod(pod)
    return "Certo, l'ho cancellato. Sì sì"

@app.get("/status")
async def get_pod_status(pods: List[interlink.PodRequest]) -> List[interlink.PodStatus]:
    logging.info(f"Requested status, number of pods: {len(pods)}")
    return [await kueue_provider.get_pod_status(pod) for pod in pods]


@app.get("/getLogs")
async def get_pod_logs(req: interlink.LogRequest) -> str:
    log = await kueue_provider.get_pod_logs(req)
    print (log)
    return log #.encode('utf-8')


@app.post("/shutdown")
async def shutdown() -> str:
    logging.info("Shutting down")
    os.kill(os.getpid(), signal.SIGTERM)
    return "Shutting down"

@app.get("/healthz")
async def healtz() -> bool:
    logging.debug("Health tested: ok.")
    # async with kubernetes_api() as k8s:
    #     ret = await k8s.list_pod_for_all_namespaces()
    #     logging.debug(ret)

    return True
