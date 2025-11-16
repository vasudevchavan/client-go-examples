package watchers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/vasudevchavan/client-go-examples/pkg/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"k8s.io/client-go/kubernetes"

	"k8s.io/apimachinery/pkg/watch"
)

func WatchPodUsingWatcher(clientset *kubernetes.Clientset, namespace string) {
	pods := clientset.CoreV1().Pods(namespace)

	podWatch, err := pods.Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to watch pods: %v", err)
		return
	}
	defer podWatch.Stop()

	for event := range podWatch.ResultChan() {
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
	podsWatch, err := pods.Watch(context.Background(), metav1.ListOptions{
		LabelSelector: labels,
	})
	if err != nil {
		klog.Errorf("Failed to watch filtered pods: %v", err)
		return
	}
	defer podsWatch.Stop()

	for event := range podsWatch.ResultChan() {
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

func WatchBadImagePodUsingWatcher(clientset *kubernetes.Clientset, namespace string) {
	pods := clientset.CoreV1().Pods(namespace)

	podsWatch, err := pods.Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to watch pods: %v", err)
		return
	}
	defer podsWatch.Stop()

	for event := range podsWatch.ResultChan() {
		klog.Info("Event type:", event.Type)
		pod, ok := event.Object.(*v1.Pod)
		if !ok {
			klog.Error("Failed to cast to Pod")
			continue
		}

		if event.Type == watch.Added || event.Type == watch.Modified {
			if !(len(pod.OwnerReferences) > 0) {
				for _, container := range pod.Spec.Containers {
					getImage := container.Image
					if !strings.Contains(getImage, ":") || strings.HasSuffix(getImage, ":latest") {
						klog.Warningf("Bad Pods without Image tag or latest %s/%s/%s", pod.Namespace, pod.Name, getImage)
					}
				}
			}
		}
	}
}

func DeleteBadImagePodUsingWatcher(clientset *kubernetes.Clientset, namespace string) {
	pods := clientset.CoreV1().Pods(namespace)

	podsWatch, err := pods.Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Failed to watch pods: %v", err)
		return
	}
	defer podsWatch.Stop()

	for event := range podsWatch.ResultChan() {
		klog.Info("Event type:", event.Type)
		pod, ok := event.Object.(*v1.Pod)
		if !ok {
			klog.Error("Failed to cast to Pod")
			continue
		}

		if event.Type == watch.Added || event.Type == watch.Modified {
			deletePod := false
			if !(len(pod.OwnerReferences) > 0) {
				for _, container := range pod.Spec.Containers {
					getImage := container.Image
					if !strings.Contains(getImage, ":") || strings.HasSuffix(getImage, ":latest") {
						deletePod = true
					}
				}
				if deletePod {
					err := clientset.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{})
					if err != nil {
						klog.Warning("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
						klog.Errorf("Failed to delete pod %s: %v", pod.Name, err)
						klog.Warning("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
					} else {
						klog.Warning("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
						klog.Infof("Pod %s deleted successfully", pod.Name)
						klog.Warning("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
					}
				}
			}
		}

		modifiers := utils.GetManagers(pod.ManagedFields)
		klog.Infof("pod:%s has been %s by %s",
			pod.Name,
			event.Type,
			modifiers)
	}
}

func BackupNewPodJson(clientset *kubernetes.Clientset, namespace string) {
	bPod := clientset.CoreV1().Pods(namespace)

	podWatcher, err := bPod.Watch(context.Background(), metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Unable to list pods %w", err)
	}
	defer podWatcher.Stop()

	backupRoot, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current directory:", err)
		return
	}
	klog.Info("Pod backup watcher started...")

	for event := range podWatcher.ResultChan() {
		if event.Type != watch.Added {
			continue
		}
		pod, ok := event.Object.(*v1.Pod)
		if !ok {
			klog.Errorf("failed to parse pod object %w", err)
		}
		latestPod, err := clientset.CoreV1().Pods(pod.Namespace).Get(context.Background(),
			pod.Name,
			metav1.GetOptions{})

		if err != nil {
			klog.Errorf("Failed to GET pod %s/%s for backup: %v", pod.Namespace, pod.Name, err)
			continue
		}
		utils.SavePodJSON(latestPod, backupRoot)
	}
}
