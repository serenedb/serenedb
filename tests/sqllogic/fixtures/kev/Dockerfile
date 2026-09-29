FROM python:3.12-slim

ARG KEV_COMMIT=09ff745d52a0f23954e3b0f5a608bf4c7c6aebb4
ARG KEV_MODEL=kev-0.5b
ARG KEV_MODEL_URL=https://github.com/jaredpalmer/kev/releases/download/v0.1.0/kev-0.5b.tar.gz
ARG KEV_MODEL_SHA256=15639f791361258432fc954a68023bca95260dade61f731b4260eb60f06e12f8

RUN pip install --no-cache-dir --root-user-action=ignore --disable-pip-version-check uv \
    && uv pip install --system --no-cache --quiet --torch-backend=cpu \
       "kev[serve] @ https://github.com/jaredpalmer/kev/archive/${KEV_COMMIT}.tar.gz"

RUN python -c "import sys, urllib.request; urllib.request.urlretrieve(sys.argv[1], '/tmp/kev.tar.gz')" "${KEV_MODEL_URL}" \
    && echo "${KEV_MODEL_SHA256}  /tmp/kev.tar.gz" | sha256sum -c --quiet - \
    && mkdir /models \
    && tar --warning=no-unknown-keyword --exclude='._*' -xzf /tmp/kev.tar.gz -C /models \
    && rm /tmp/kev.tar.gz

ENV KEV_RUN=/models/${KEV_MODEL}
RUN HF_HUB_DISABLE_PROGRESS_BARS=1 HF_HUB_VERBOSITY=error \
    python -c "import os; from kev.checkpoint import Checkpoint; Checkpoint(os.environ['KEV_RUN']).load('cpu')"
ENV HF_HUB_OFFLINE=1

COPY serve.py /serve.py
EXPOSE 8009
ENTRYPOINT ["python", "/serve.py"]
