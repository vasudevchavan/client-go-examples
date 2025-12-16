package watchers

import (
	"context"
	"encoding/json"

	"github.com/vasudevchavan/client-go-examples/pkg/utils"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

func WatchDepUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string) {
	deployment := clientset.AppsV1().Deployments(namespace)

	watch, err := deployment.Watch(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to start watch on Deployments: %v", err)
		return
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		select {
		case <-ctx.Done():
			klog.Info("Context cancelled, stopping watcher")
			return
		default:
			// klog.Info("Event type:", event.Type)
			dep, ok := event.Object.(*appsv1.Deployment)
			if !ok {
				klog.Info("Failed to cast to Deployment")
				continue
			}
			modifiers := utils.GetManagers(dep.ManagedFields)
			klog.Infof("Namespace:%s | Deployment:%s | Event:%s | Owner:%s",
				dep.Namespace, dep.Name, event.Type, modifiers)
		}
	}
}

func WatchFilteredDepUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string, labels string) {
	dep := clientset.AppsV1().Deployments(namespace)

	watch, err := dep.Watch(ctx, metav1.ListOptions{
		LabelSelector: labels,
	})
	if err != nil {
		klog.Errorf("Failed to start watch on Deployments: %v", err)
		return
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		select {
		case <-ctx.Done():
			klog.Info("Context cancelled, stopping watcher")
			return
		default:
			// klog.Info("Event type:", event.Type)
			dep, ok := event.Object.(*appsv1.Deployment)
			if !ok {
				klog.Info("Failed to cast to Deployment")
				continue
			}
			modifiers := utils.GetManagers(dep.ManagedFields)
			labelsJson, err := json.Marshal(dep.GetLabels())
			if err != nil {
				klog.Errorf("Failed to marshal labels: %v", err)
				continue
			}
			klog.Infof("Labels: %s", string(labelsJson))
			klog.Infof("Namespace:%s | Deployment:%s | Event:%s | Owner:%s",
				dep.Namespace, dep.Name, event.Type, modifiers)
		}
	}
}
