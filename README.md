# client-go-examples

A Kubernetes watcher CLI tool that demonstrates how to use client-go to interact programmatically with the Kubernetes API. This tool watches Kubernetes resources (pods, deployments, configmaps, secrets) and logs their events in real-time.

## Features

- Watch multiple Kubernetes resource types
- Filter resources by namespace and labels
- Real-time event monitoring (ADDED, MODIFIED, DELETED)
- Command-line interface with flexible options
- Uses native Kubernetes watchers (not informers)

## Prerequisites

- Go 1.19 or later
- Access to a Kubernetes cluster
- Valid kubeconfig file

## Installation

```bash
git clone https://github.com/vasudevchavan/client-go-examples.git
cd client-go-examples
go mod tidy
```

## Usage

### Basic Commands

```bash
# Watch all pods in all namespaces
go run main.go

# Watch pods with specific labels
go run main.go -labels="run=test"

# Watch pods in specific namespace
go run main.go -namespace="default"

# Watch deployments
go run main.go -resource="deployments"

# Watch configmaps in kube-system namespace
go run main.go -resource="configmaps" -namespace="kube-system"

# Watch secrets with labels in specific namespace
go run main.go -resource="secrets" -namespace="default" -labels="app=testing"
```

### Command-Line Flags

| Flag | Description | Default | Options |
|------|-------------|---------|---------|
| `-resource` | Resource type to watch | `pods` | `pods`, `deployments`, `configmaps`, `secrets` |
| `-namespace` | Namespace to watch | `""` (all) | Any valid namespace |
| `-labels` | Label selector | `""` (none) | e.g., `run=test`, `app=nginx` |

## Examples

### Watch Pod Events

```bash
# Create a test pod
kubectl run test-pod --image=nginx -l run=test

# Watch the pod
go run main.go -resource="pods" -labels="run=test"

# Delete the pod to see deletion event
kubectl delete pod test-pod
```

### Watch Deployment Events

```bash
# Create a deployment
kubectl create deployment nginx --image=nginx

# Watch deployments
go run main.go -resource="deployments"

# Scale the deployment
kubectl scale deployment nginx --replicas=3
```

## Project Structure

```
client-go-examples/
├── main.go                    # CLI entry point
├── pkg/
│   ├── cmd/watchers/         # Watcher implementations
│   │   ├── pods.go           # Pod watchers
│   │   ├── deployment.go     # Deployment watchers
│   │   ├── configmap.go      # ConfigMap watchers
│   │   └── secrets.go        # Secret watchers
│   ├── kubeconfig/           # Kubernetes configuration
│   │   └── kubelogin.go      # Kubeconfig loader
│   └── utils/                # Utility functions
│       └── helper.go         # Helper functions
├── go.mod                    # Go module file
└── README.md                 # This file
```

## Configuration

The tool uses your default kubeconfig file located at `~/.kube/config`. You can override this by setting the `KUBECONFIG` environment variable:

```bash
export KUBECONFIG=/path/to/your/kubeconfig
go run main.go
```

## Output Format

The tool outputs structured logs showing:
- Event type (ADDED, MODIFIED, DELETED)
- Resource name and namespace
- Labels (when using filtered watchers)
- Managers who modified the resource

Example output:
```
Watching pods in namespace 'default' with labels 'run=test'
Event type: ADDED
Labels: {"run":"test"}
pod:test-pod has been ADDED by [kubectl-run]
```

## Error Handling

The tool includes comprehensive error handling for:
- Kubernetes API connection failures
- Invalid resource types
- Network interruptions
- Type casting errors
- JSON marshaling errors

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License.

## Troubleshooting

### Common Issues

**Connection refused:**
- Ensure your Kubernetes cluster is running
- Verify kubeconfig is valid: `kubectl cluster-info`

**Permission denied:**
- Check RBAC permissions: `kubectl auth can-i watch pods`
- Ensure your user has appropriate cluster access

**No events showing:**
- Verify resources exist in the specified namespace
- Check label selectors are correct
- Ensure the watcher is running before creating/modifying resources