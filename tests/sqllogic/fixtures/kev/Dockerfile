FROM python:3.12-slim

ARG KEV_COMMIT=09ff745d52a0f23954e3b0f5a608bf4c7c6aebb4
ARG KEV_RUN=jaredpalmer/kev-0.8b

RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv \
    && git clone https://github.com/jaredpalmer/kev.git /kev \
    && git -C /kev checkout "${KEV_COMMIT}" \
    && uv pip install --system --no-cache --torch-backend=cpu -e '/kev[serve]'

ENV KEV_RUN=${KEV_RUN} HF_HOME=/hf
RUN python -c "import os; from kev.checkpoint import Checkpoint; Checkpoint(os.environ['KEV_RUN']).load('cpu')"
ENV HF_HUB_OFFLINE=1

COPY serve.py /serve.py
EXPOSE 8009
ENTRYPOINT ["python", "/serve.py"]
