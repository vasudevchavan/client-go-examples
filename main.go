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
		resource          = flag.String("resource", "", "Resource to watch (pods, deployments, configmaps, secrets)")
		namespace         = flag.String("namespace", "", "Namespace to watch (empty for all namespaces)")
		labels            = flag.String("labels", "", "Label selector (e.g., run=test)")
		deleteBadImagePod = flag.Bool("deletePods", false, "Delete pod containing images without & latest tag")
		listBadImagePod   = flag.Bool("listbadimagepod", false, "List pod containing images without & latest tag")
		backupPod         = flag.Bool("backuppod", false, "List pod containing images without & latest tag")
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

	fmt.Printf("Watching %s in namespace '%s' with labels '%s'\n", *resource, *namespace, *labels)

	switch *resource {
	case "pods":
		fmt.Println("Calling POD")
		if *labels != "" {
			watchers.WatchFilteredPodUsingWatcher(clientset, *namespace, *labels)
		} else {
			watchers.WatchPodUsingWatcher(clientset, *namespace)
		}
	case "deployments":
		fmt.Println("Calling DEPLOYMENT")
		if *labels != "" {
			watchers.WatchFilteredDepUsingWatcher(clientset, *namespace, *labels)
		} else {
			watchers.WatchDepUsingWatcher(clientset, *namespace)
		}
	case "configmaps":
		fmt.Println("Calling CM")
		if *labels != "" {
			watchers.WatchFilteredCMUsingWatcher(clientset, *namespace, *labels)
		} else {
			watchers.WatchCMUsingWatcher(clientset, *namespace)
		}
	case "secrets":
		fmt.Println("Calling SECRETS")
		if *labels != "" {
			watchers.WatchFilteredSecretsUsingWatcher(clientset, *namespace, *labels)
		} else {
			watchers.WatchSecretsUsingWatcher(clientset, *namespace)
		}
	}

	switch {
	case *deleteBadImagePod:
		fmt.Println("Calling delete Bad pod")
		watchers.DeleteBadImagePodUsingWatcher(clientset, *namespace)
	case *listBadImagePod:
		fmt.Println("Calling list Bad pod")
		watchers.WatchBadImagePodUsingWatcher(clientset, *namespace)
	case *backupPod:
		fmt.Println("Calling backup Bad pod")
		watchers.BackupNewPodJson(clientset, *namespace)
	}
}

// Feature Which will delete pods without Tag or latest
// watchers.WatchImageInPodUsingWatcher(clientset, *namespace)

// Backup Pod spec to location
// watchers.BackupPodJson(client,*namespace)
