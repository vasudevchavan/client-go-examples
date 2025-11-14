package watchers

import (
	"context"
	"encoding/json"

	"github.com/vasudevchavan/client-go-examples/pkg/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"k8s.io/client-go/kubernetes"
)

func WatchPodUsingWatcher(clientset *kubernetes.Clientset, namespace string) {
	pods := clientset.CoreV1().Pods(namespace)

	watch, err := pods.Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to watch pods: %v", err)
		return
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		klog.Info("Event type:", event.Type)
		pod, ok := event.Object.(*v1.Pod)
		if !ok {
			klog.Error("Failed to cast to Pod")
			continue
		}
		modifiers := utils.GetManagers(pod.ManagedFields)
		klog.Infof("pod:%s has been %s by %s",
			pod.Name,
			event.Type,
			modifiers)
	}
}

func WatchFilteredPodUsingWatcher(clientset *kubernetes.Clientset, namespace string, labels string) {
	pods := clientset.CoreV1().Pods(namespace)
	watch, err := pods.Watch(context.Background(), metav1.ListOptions{
		LabelSelector: labels,
	})
	if err != nil {
		klog.Errorf("Failed to watch filtered pods: %v", err)
		return
	}
	defer watch.Stop()

	for event := range watch.ResultChan() {
		klog.Info("Event type:", event.Type)
		podObj, ok := event.Object.(*v1.Pod)
		if !ok {
			klog.Error("Failed to cast to Pod")
			continue
		}
		modifiers := utils.GetManagers(podObj.ManagedFields)
		labelsJson, err := json.Marshal(podObj.GetLabels())
		if err != nil {
			klog.Errorf("Failed to marshal labels: %v", err)
			continue
		}
		klog.Infof("Labels: %s", string(labelsJson))
		klog.Infof("pod:%s has been %s by %s",
			podObj.Name,
			event.Type,
			modifiers)
	}
}
