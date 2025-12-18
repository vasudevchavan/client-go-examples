package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/vasudevchavan/client-go-examples/pkg/cmd/watchers"
	"github.com/vasudevchavan/client-go-examples/pkg/kubeconfig"
	klog "k8s.io/klog/v2"
)

func main() {
	var (
		resource          = flag.String("resource", "", "Resource to watch (pods, deployments, configmaps, secrets)")
		namespace         = flag.String("namespace", "", "Namespace to watch (empty for all namespaces)")
		labels            = flag.String("labels", "", "Label selector (e.g., run=test)")
		deleteBadImagePod = flag.Bool("delete-pods", false, "Delete pod containing images without & latest tag")
		listBadImagePod   = flag.Bool("list-bad-image-pod", false, "List pod containing images without & latest tag")
		backupPod         = flag.Bool("backup-pod", false, "Backup pod specs to JSON files")
	)

	klog.InitFlags(nil)
	flag.Set("logtostderr", "true")
	flag.Set("skip_headers", "false")
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("Watching %s in namespace '%s' with labels '%s'\n", *resource, *namespace, *labels)

	switch *resource {
	case "pods":
		fmt.Println("Calling POD")
		watchers.WatchFilteredPodUsingWatcher(ctx, clientset, *namespace, *labels)
	case "deployments":
		fmt.Println("Calling DEPLOYMENT")
		watchers.WatchFilteredDepUsingWatcher(ctx, clientset, *namespace, *labels)
	case "configmaps":
		fmt.Println("Calling CM")
		watchers.WatchFilteredCMUsingWatcher(ctx, clientset, *namespace, *labels)
	case "secrets":
		fmt.Println("Calling SECRETS")
		watchers.WatchFilteredSecretsUsingWatcher(ctx, clientset, *namespace, *labels)
	}

	switch {
	case *deleteBadImagePod:
		fmt.Println("Calling delete Bad pod")
		watchers.DeleteBadImagePodUsingWatcher(ctx, clientset, *namespace)
	case *listBadImagePod:
		fmt.Println("Calling list Bad pod")
		watchers.WatchBadImagePodUsingWatcher(ctx, clientset, *namespace)
	case *backupPod:
		fmt.Println("Calling backup Bad pod")
		watchers.BackupNewPodJson(ctx, clientset, *namespace)
	}
}

// Feature Which will delete pods without Tag or latest
// watchers.WatchImageInPodUsingWatcher(clientset, *namespace)

// Backup Pod spec to location
// watchers.BackupPodJson(client,*namespace)
