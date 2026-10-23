FROM python:3.12-slim AS build

WORKDIR /build
COPY LICENSE ./
COPY docs/schemas/dirac-1.0.json docs/schemas/dirac-1.0.json
COPY diracx-core/ diracx-core/
COPY diracx-db/ diracx-db/
COPY diracx-logic/ diracx-logic/
COPY diracx-tasks/ diracx-tasks/
# A local build version works without copying .git into the build context.
ARG VERSION=0.0.0
ENV SETUPTOOLS_SCM_PRETEND_VERSION=${VERSION}
RUN python -m venv /opt/operator && \
    /opt/operator/bin/pip install --no-cache-dir \
      ./diracx-core ./diracx-db ./diracx-logic './diracx-tasks[kubernetes]'

FROM python:3.12-slim
COPY --from=build /opt/operator /opt/operator
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
USER 10001:10001
ENTRYPOINT ["/opt/operator/bin/diracx-workgraph-operator"]
