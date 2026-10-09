import os
import sys

from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobServiceClient

connection_string, data_dir = sys.argv[1:3]

service = BlobServiceClient.from_connection_string(connection_string)
for name, public_access in (("testing-private", None), ("testing-public", "blob"), ("writes", None)):
    try:
        service.create_container(name, public_access=public_access)
    except ResourceExistsError:
        pass

for root, _, files in os.walk(data_dir):
    for file in files:
        path = os.path.join(root, file)
        blob = os.path.relpath(path, data_dir)
        for container in ("testing-private", "testing-public"):
            with open(path, "rb") as content:
                service.get_blob_client(container, blob).upload_blob(content, overwrite=True)
