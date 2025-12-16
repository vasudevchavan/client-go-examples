package watchers

import (
	"context"
	"encoding/json"

	"github.com/vasudevchavan/client-go-examples/pkg/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

func WatchServicesUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string) {
	svc := clientset.CoreV1().Services(namespace)

	watch, err := svc.Watch(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to watch service: %v", err)
		return
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		// klog.Info("Event type:", event.Type)
		service, ok := event.Object.(*v1.Service)
		if !ok {
			klog.Error("Failed to cast to Service")
			continue
		}
		modifiers := utils.GetManagers(service.ManagedFields)
		klog.Infof("Service:%s has been %s by %s", service.Name, event.Type, modifiers)
	}
}

func WatchFilteredServicesUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string, labels string) {
	svc := clientset.CoreV1().Services(namespace)

	watch, err := svc.Watch(ctx, metav1.ListOptions{
		LabelSelector: labels,
	})
	if err != nil {
		klog.Errorf("Failed to watch filtered services: %v", err)
		return
	}
	defer watch.Stop()

	watchChan := watch.ResultChan()
	for {
		select {
		case <-ctx.Done():
			klog.Info("Context cancelled, stopping watcher")
			return
		case event, ok := <-watchChan:
			if !ok {
				klog.Info("Service watcher channel closed")
				return
			}
			service, ok := event.Object.(*v1.Service)
			if !ok {
				klog.Error("Failed to cast to Service")
				continue
			}
			modifiers := utils.GetManagers(service.ManagedFields)
			labelsJson, err := json.Marshal(service.GetLabels())
			if err != nil {
				klog.Errorf("Failed to marshal labels: %v", err)
				continue
			}
			klog.Infof("Labels: %s", string(labelsJson))
			klog.Infof("Service:%s has been %s by %s", service.Name, event.Type, modifiers)
		}
	}
}
