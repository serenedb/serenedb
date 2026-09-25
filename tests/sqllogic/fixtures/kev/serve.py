import os
import sys

import uvicorn

import kev.serve

run = uvicorn.run
uvicorn.run = lambda app, host, port, **kwargs: run(app, host="0.0.0.0", port=port, **kwargs)
sys.argv = [sys.argv[0], "--run", os.environ["KEV_RUN"], "--port", "8009"]
kev.serve.main()
