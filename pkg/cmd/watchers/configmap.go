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

func WatchCMUsingWatcher(clientset *kubernetes.Clientset, namespace string) {
	cm := clientset.CoreV1().ConfigMaps(namespace)

	watch, err := cm.Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to watch configmaps: %v", err)
		return
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		klog.Info("Event type:", event.Type)
		cm, ok := event.Object.(*v1.ConfigMap)
		if !ok {
			klog.Error("Failed to cast to ConfigMap")
			continue
		}
		modifiers := utils.GetManagers(cm.ManagedFields)
		klog.Infof("ConfigMap:%s has been %s by %s", cm.Name, event.Type, modifiers)
	}
}

func WatchFilteredCMUsingWatcher(clientset *kubernetes.Clientset, namespace string, labels string) {
	cm := clientset.CoreV1().ConfigMaps(namespace)

	watch, err := cm.Watch(context.Background(), metav1.ListOptions{
		LabelSelector: labels,
	})
	if err != nil {
		klog.Errorf("Failed to watch filtered configmaps: %v", err)
		return
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		klog.Info("Event type:", event.Type)
		cm, ok := event.Object.(*v1.ConfigMap)
		if !ok {
			klog.Error("Failed to cast to ConfigMap")
			continue
		}
		modifiers := utils.GetManagers(cm.ManagedFields)
		labelsJson, err := json.Marshal(cm.GetLabels())
		if err != nil {
			klog.Errorf("Failed to marshal labels: %v", err)
			continue
		}
		klog.Infof("Labels: %s", string(labelsJson))
		klog.Infof("ConfigMap:%s has been %s by %s", cm.Name, event.Type, modifiers)
	}
}
