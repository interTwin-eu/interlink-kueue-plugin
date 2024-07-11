# Image name: landerlini/fastinterlink:v0.0
FROM python:latest

RUN apt-get install git

RUN pip install \
  "uvicorn[standard]" \
  "fastapi" \
  "pyyaml" \
  "jinja2" \
  "kubernetes-asyncio" \
  "git+https://github.com/intertwin-eu/interLink.git#egg=interlink&subdirectory=example"

