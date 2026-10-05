FROM python:3.13-slim

ARG KEV_RELEASE=kev-1.0
ARG KEV_COMMIT=6b719c3c3f367295f6ef336f4f751cf5ff970abc
ARG KEV_MODEL=kev-0.5b
ARG KEV_MODEL_REPO=jaredpalmer/kev-0.5b
ARG KEV_MODEL_REVISION=2679c20e6dde32fb3c4f97ecdad2e6e92bb88a06
ARG KEV_BASE_REVISION=060db6499f32faf8b98477b0a26969ef7d8b9987

COPY --from=ghcr.io/astral-sh/uv:0.12.22 /uv /usr/local/bin/uv

RUN python -c "import sys, urllib.request; urllib.request.urlretrieve(sys.argv[1], '/tmp/kev-src.tar.gz')" \
       "https://github.com/jaredpalmer/kev/archive/${KEV_COMMIT}.tar.gz" \
    && mkdir /kev \
    && tar -xzf /tmp/kev-src.tar.gz -C /kev --strip-components=1 \
       "kev-${KEV_COMMIT}/kev" "kev-${KEV_COMMIT}/pyproject.toml" "kev-${KEV_COMMIT}/uv.lock" \
       "kev-${KEV_COMMIT}/README.md" "kev-${KEV_COMMIT}/LICENSE" \
    && uv export --project /kev --frozen --no-dev --extra serve --no-emit-project \
       --no-hashes --no-header --no-annotate \
       | grep -v -E '^(nvidia-|triton==)' >/tmp/requirements.txt \
    && uv pip install --system --no-cache --quiet --torch-backend=cpu -r /tmp/requirements.txt \
    && uv pip install --system --no-cache --quiet --no-deps /kev \
    && rm -rf /kev/build /kev/kev.egg-info /tmp/kev-src.tar.gz /tmp/requirements.txt

ENV KEV_RUN=/models/${KEV_MODEL}
RUN HF_HUB_DISABLE_PROGRESS_BARS=1 HF_HUB_VERBOSITY=error \
    python -c "import os, sys; from huggingface_hub import snapshot_download; snapshot_download(sys.argv[1], revision=sys.argv[2], local_dir=os.environ['KEV_RUN'], allow_patterns=['*.json', '*.safetensors', '*.pt', '*.txt', '*.jinja'])" \
       "${KEV_MODEL_REPO}" "${KEV_MODEL_REVISION}"

RUN python -c "import os, sys; from kev.checkpoint import read_meta, write_meta; run = os.environ['KEV_RUN']; meta = read_meta(run); assert meta.base_revision in (None, sys.argv[1]), meta.base_revision; meta.base_revision = sys.argv[1]; write_meta(run, meta)" \
       "${KEV_BASE_REVISION}" \
    && HF_HUB_DISABLE_PROGRESS_BARS=1 HF_HUB_VERBOSITY=error \
    python -c "import os; from kev.checkpoint import Checkpoint; Checkpoint(os.environ['KEV_RUN']).load('cpu')"
ENV HF_HUB_OFFLINE=1

EXPOSE 8009
ENTRYPOINT ["sh", "-c", "exec python -m kev.serve --run \"$KEV_RUN\" --host 0.0.0.0 --port 8009"]
