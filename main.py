from typing import List
import docker
import re
import os
import pprint
from datetime import datetime

from fastapi import FastAPI, HTTPException

from kuinterlink import KueueProvider

# Initialize FastAPI app
app = FastAPI()



# Please Take my provider and handle the interLink REST layer for me
kueue_provider = KueueProvider()

@app.post("/create")
async def create_pod(pods: List[interlink.Pod]) -> str:
    return kueue_provider.create_pod(pods)

@app.post("/delete")
async def delete_pod(pod: interlink.PodRequest) -> str:
    return kueue_provider.delete_pod(pod)

@app.get("/status")
async def status_pod(pods: List[interlink.PodRequest]) -> List[interlink.PodStatus]:
    return kueue_provider.get_status(pods)

@app.post("/getLogs")
async def get_logs(req: interlink.LogRequest) -> bytes:
    return kueue_provider.get_logs(req)




