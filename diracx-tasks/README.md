# diracx-tasks

Asynchronous task system for DiracX.

## Kubernetes WorkGraph definition operator

The optional operator defines **namespaced** `WorkGraph` resources using the
[CWL vocabulary in DX-ADR-007](../docs/adr/DX-ADR-007_cwl.md). It validates a
definition and writes only its Kubernetes status. `Validated=True`, phase
`Defined`, means **defined, not scheduled or executed**. There is no workgraph
execution backend yet: this operator does not create jobs/pods, submit tasks,
resolve configuration defaults, run feeders/hooks/actions, or implement approvals.
Kubernetes RBAC controls access to definitions; this is not DiracX VO authorization.
Do not put credentials in CWL or parameter values.

### Install and run

From the repository root, with your normal DiracX development environment:

```sh
pip install './diracx-tasks[kubernetes]'
diracx-workgraph-operator --kubeconfig "$HOME/.kube/config" --namespace default
```

Without `--kubeconfig`, authentication is **in-cluster only** (no implicit local
kubeconfig fallback). Namespace defaults to `POD_NAMESPACE`, then `default`.
The operator polls every 30 seconds; `--interval` configures the delay between
passes. Lists are paginated (100 objects/page, at most 100 pages/pass), API calls
have 5-second connect/20-second read timeouts with transport retries disabled,
and conflicts get at most three status attempts with fresh reads.
Large lists continue on the next pass; expired continuation tokens restart the scan.
Transient failures retry on the next poll; deletion skips reconciliation.
Run one replica per namespace; there is no leader election or execution finalizer.

Build a real image from this checkout (no published operator image is assumed):

```sh
docker build -f containers/workgraph-operator.Dockerfile \
  -t diracx-workgraph-operator:local .
```

For a local kind cluster, load it with
`kind load docker-image diracx-workgraph-operator:local`. For other clusters,
tag and push it to your registry, then replace the Deployment's `image` with
that reference before applying. The build defaults to package version `0.0.0`;
pass `--build-arg VERSION=...` to select a release-compatible version.

An administrator installs the CRD once; install the remaining resources into the
namespace to be managed (replace `default` consistently if desired):

```sh
kubectl apply -f deploy/workgraph-operator/crd.yaml
kubectl wait --for=condition=Established \
  crd/workgraphs.workgraphs.diracx.io --timeout=60s
kubectl apply -n default -f deploy/workgraph-operator/operator.yaml
kubectl apply -n default -f deploy/workgraph-operator/example.yaml
kubectl wait -n default --for=condition=Validated \
  workgraph/simulation-definition --timeout=120s
kubectl get -n default workgraphs
kubectl get -n default workgraph simulation-definition -o yaml
```

The service account can only list/get WorkGraphs and patch their status in its
namespace. It cannot access secrets, pods or jobs, or install CRDs. The container
runs nonroot with a read-only root filesystem, dropped capabilities and resource
limits. Definitions with errors are persisted with `Validated=False`,
`reason: InvalidDefinition`, phase `Invalid`; correct the spec to revalidate.
Status includes `observedGeneration` and transformation/dependency counts.
Unchanged definitions do not trigger patches; condition transition time changes
only when validation truth changes.

### Definition contract and validation boundary

`spec.cwl` is the document object, preserved by the CRD without pruning extension
fields; optional `spec.inputs` is an object of supplied workflow parameter values.
Both mapping and list CWL forms are supported for hints, inputs, outputs, steps
and step-input bindings. The operator checks:

- CWL `v1.2`, outer `Workflow`, the `dirac` namespace, required
  `dirac:Workgraph` / per-step `dirac:Transformation`, and parameter/run shapes;
- the complete published [Dirac hint schema](../docs/schemas/dirac-1.0.json),
  including feeder/plugin/action/hook fields (the original schema is bundled at
  build time, not maintained as a second source copy);
- duplicate ids/hint classes, declared ports and sources, `driving_input`, target
  outputs, data-management references, orphan step outputs, and cycles across
  dataflow, data management and `after`.

This is **not full CWL validation**: process bodies, CWL type compatibility,
expressions, supplied value types and referenced files are not validated by a
CWL runner. References in `run` remain opaque and are never fetched. No URL/file
resolution, evaluation or execution takes place. For graph identifiers, only
local names matching `[A-Za-z_][A-Za-z0-9_.-]{0,127}` are supported, and sources
must be `input` or `step/output` (optionally prefixed by `#`), not absolute IRIs
or document-qualified fragments. Data management can additionally reference a
workflow output; references to workflow inputs require `dirac:Feeder`. Omitted
data-management ids are assigned `data-1`, `data-2`, etc. Maximum graph size is
1000 transformations. Unsupported/invalid definitions receive a clear condition,
not a claim that they are runnable. Use a conformant CWL validator separately
when execution-level validation is needed.

Installed wheels resolve the schema using `importlib.resources`. Editable and
source-tree imports fall back to the authoritative monorepo `docs/schemas` path.
Tests can also pass `load_schema(Path("docs/schemas/dirac-1.0.json"))` explicitly
to `DefinitionValidator`. Operator tests skip when the Kubernetes extra is absent;
existing pixi environments do not need to select it.
