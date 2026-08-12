#!/usr/bin/env bash
set -euo pipefail

python -m grpc_tools.protoc \
  -I protos \
  --python_out=gateway/generated \
  --grpc_python_out=gateway/generated \
  protos/demo.proto

# grpc_tools emits `import demo_pb2` — fix to a package-relative import.
python - <<'PY'
import re
path = "gateway/generated/demo_pb2_grpc.py"
with open(path) as f:
    content = f.read()
content = re.sub(r"^import demo_pb2 as demo__pb2$", "from . import demo_pb2 as demo__pb2", content, flags=re.M)
with open(path, "w") as f:
    f.write(content)
PY

echo "Generated gateway/generated/demo_pb2.py and demo_pb2_grpc.py"
