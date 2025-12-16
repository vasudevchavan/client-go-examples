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

func WatchSecretsUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string) {
	sec := clientset.CoreV1().Secrets(namespace)

	watch, err := sec.Watch(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to watch secrets: %v", err)
		return
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		// klog.Info("Event type:", event.Type)
		secret, ok := event.Object.(*v1.Secret)
		if !ok {
			klog.Error("Failed to cast to Secret")
			continue
		}
		modifiers := utils.GetManagers(secret.ManagedFields)
		klog.Infof("Secret:%s has been %s by %s", secret.Name, event.Type, modifiers)
	}
}

func WatchFilteredSecretsUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string, labels string) {
	sec := clientset.CoreV1().Secrets(namespace)

	watch, err := sec.Watch(ctx, metav1.ListOptions{
		LabelSelector: labels,
	})
	if err != nil {
		klog.Errorf("Failed to watch filtered secrets: %v", err)
		return
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		// klog.Info("Event type:", event.Type)
		secret, ok := event.Object.(*v1.Secret)
		if !ok {
			klog.Error("Failed to cast to Secret")
			continue
		}
		modifiers := utils.GetManagers(secret.ManagedFields)
		labelsJson, err := json.Marshal(secret.GetLabels())
		if err != nil {
			klog.Errorf("Failed to marshal labels: %v", err)
			continue
		}
		klog.Infof("Labels: %s", string(labelsJson))
		klog.Infof("Secret:%s has been %s by %s", secret.Name, event.Type, modifiers)
	}
}
