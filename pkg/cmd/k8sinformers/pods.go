package k8sinformers

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	klog "k8s.io/klog/v2"
)

// WatchPods lists existing pods and watches for changes in a namespace
func WatchPods(ctx context.Context, clientset *kubernetes.Clientset, namespace string) {
	// 1️⃣ List existing pods
	pods, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to list pods in namespace %s: %v", namespace, err)
	} else {
		fmt.Printf("Existing Pods in namespace '%s':\n", namespace)
		for _, pod := range pods.Items {
			fmt.Printf("- %s (phase: %s)\n", pod.Name, pod.Status.Phase)
		}
	}

	// 2️⃣ Create informer factory
	factory := informers.NewSharedInformerFactoryWithOptions(clientset, 30*time.Second, informers.WithNamespace(namespace))
	podInformer := factory.Core().V1().Pods().Informer()

	// 3️⃣ Add event handlers for live updates
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			fmt.Printf("[ADD] Pod: %s/%s (phase: %s)\n", pod.Namespace, pod.Name, pod.Status.Phase)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			pod := newObj.(*v1.Pod)
			fmt.Printf("[UPDATE] Pod: %s/%s (phase: %s)\n", pod.Namespace, pod.Name, pod.Status.Phase)
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*v1.Pod)
			fmt.Printf("[DELETE] Pod: %s/%s\n", pod.Namespace, pod.Name)
		},
	})

	// 4️⃣ Stop channel tied to context
	stopCh := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(stopCh)
	}()

	// 5️⃣ Start the informer
	factory.Start(stopCh)

	// 6️⃣ Wait for cache to sync
	if !cache.WaitForCacheSync(stopCh, podInformer.HasSynced) {
		klog.Fatalf("Failed to sync pod informer cache")
	}

	fmt.Printf("Watching Pods in namespace '%s'...\n", namespace)

	// 7️⃣ Block until context is done
	<-ctx.Done()
	fmt.Println("Stopping pod watcher...")
}
