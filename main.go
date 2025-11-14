package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/vasudevchavan/client-go-examples/pkg/cmd/watchers"
	"github.com/vasudevchavan/client-go-examples/pkg/kubeconfig"
	klog "k8s.io/klog/v2"
)

func main() {
	var (
		resource  = flag.String("resource", "pods", "Resource to watch (pods, deployments, configmaps, secrets)")
		namespace = flag.String("namespace", "", "Namespace to watch (empty for all namespaces)")
		labels    = flag.String("labels", "", "Label selector (e.g., run=test)")
	)

	klog.InitFlags(nil)
	flag.Set("logtostderr", "true")
	flag.Parse()
	defer klog.Flush()

	_, clientset, err := kubeconfig.LoadKubeConfig()
	if err != nil {
		klog.Errorf("Unable to login to kubernetes cluster: %v", err)
		os.Exit(1)
	}
	if clientset == nil {
		klog.Error("Clientset is nil")
		os.Exit(1)
	}

	if resource == nil || namespace == nil || labels == nil {
		klog.Error("Flag pointers are nil")
		os.Exit(1)
	}

	fmt.Printf("Watching %s in namespace '%s' with labels '%s'\n", *resource, *namespace, *labels)

	switch *resource {
	case "pods":
		if *labels != "" {
			watchers.WatchFilteredPodUsingWatcher(clientset, *namespace, *labels)
		} else {
			watchers.WatchPodUsingWatcher(clientset, *namespace)
		}
	case "deployments":
		if *labels != "" {
			watchers.WatchFilteredDepUsingWatcher(clientset, *namespace, *labels)
		} else {
			watchers.WatchDepUsingWatcher(clientset, *namespace)
		}
	case "configmaps":
		if *labels != "" {
			watchers.WatchFilteredCMUsingWatcher(clientset, *namespace, *labels)
		} else {
			watchers.WatchCMUsingWatcher(clientset, *namespace)
		}
	case "secrets":
		if *labels != "" {
			watchers.WatchFilteredSecretsUsingWatcher(clientset, *namespace, *labels)
		} else {
			watchers.WatchSecretsUsingWatcher(clientset, *namespace)
		}
	default:
		fmt.Printf("Unsupported resource: %s\n", *resource)
		os.Exit(1)
	}
}
