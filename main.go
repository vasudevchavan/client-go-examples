package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/vasudevchavan/client-go-examples/pkg/cmd/k8sinformers"
	"github.com/vasudevchavan/client-go-examples/pkg/kubeconfig"
	klog "k8s.io/klog/v2"
)

func main() {
	var (
		resource  = flag.String("resource", "", "Resource to watch (pods, deployments, configmaps, secrets)")
		namespace = flag.String("namespace", "default", "Namespace to watch (empty for all namespaces)")
		labels    = flag.String("labels", "", "Label selector (e.g., run=test)")
	// 	deleteBadImagePod = flag.Bool("delete-pods", false, "Delete pod containing images without & latest tag")
	// 	listBadImagePod   = flag.Bool("list-bad-image-pod", false, "List pod containing images without & latest tag")
	// 	backupPod         = flag.Bool("backup-pod", false, "Backup pod specs to JSON files")
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

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	fmt.Printf("Watching %s in namespace '%s' with labels '%s'\n", *resource, *namespace, *labels)

	k8sinformers.WatchPods(ctx, clientset, *namespace)
}
