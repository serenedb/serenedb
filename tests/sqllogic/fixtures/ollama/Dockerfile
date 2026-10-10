ARG OLLAMA_VERSION=0.40.1

FROM ollama/ollama:${OLLAMA_VERSION}

ARG OLLAMA_MODELS="all-minilm qwen2.5:0.5b"

RUN ollama serve >/tmp/ollama.log 2>&1 & \
    pid=$!; \
    for i in $(seq 1 60); do ollama list >/dev/null 2>&1 && break; sleep 1; done; \
    for model in ${OLLAMA_MODELS}; do ollama pull "$model" || exit 1; done; \
    kill "$pid"; wait "$pid" || true
