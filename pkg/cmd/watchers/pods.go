package watchers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/vasudevchavan/client-go-examples/pkg/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"

	"k8s.io/client-go/kubernetes"

	"k8s.io/apimachinery/pkg/watch"
)

var protectedNamespaces = map[string]bool{
	"kube-system":     true,
	"kube-public":     true,
	"kube-node-lease": true,
}

func WatchPodUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string) {
	var lastResourceVersion string

	// Validate access before starting watcher
	if !utils.CanUserAccessResource(ctx, clientset, namespace, "pods", "get", "pod") {
		klog.Errorf("Shutdown watcher for namespace %s", namespace)
		return
	}

	for {
		if ctx.Err() != nil {
			klog.Info("Shutdown signal Received. Exiting Watcher.")
			return
		}
		// initialize pod watcher
		watchPods, err := clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
			ResourceVersion: lastResourceVersion,
		})
		if err != nil {
			klog.Errorf("failed to start watcher in namespace:%s with error:%v", namespace, err)
			time.Sleep(2 * time.Second)
			continue
		}

		klog.V(2).Infof("watcher established for namespace:%s for resource version:%s", namespace, lastResourceVersion)

		watchPodChan := watchPods.ResultChan()
		restartFlag := false

		// Process events.
		for !restartFlag {
			select {
			case <-ctx.Done():
				// Immediate shutdown request
				watchPods.Stop()
				klog.Info("Shutdown Watcher.")
				return
			case event, ok := <-watchPodChan:
				if !ok {
					klog.Warning("watch pod channel closed restarted")
					restartFlag = true
					break
				}

				// Handle Watch error events
				if event.Type == watch.Error {
					status, ok := event.Object.(*metav1.Status)
					if ok && status.Code == http.StatusGone {
						klog.Warning("resourceversion expired, resetting")
						lastResourceVersion = ""
					} else if ok {
						klog.Errorf("Generic error for the event type:%s in namespace:%s", status.Message, namespace)
					} else {
						klog.Errorf("unparseable error event for namespace %s: %+v", namespace, event.Object)
					}
					restartFlag = true
					break
				}

				pod, ok := event.Object.(*v1.Pod)
				if !ok {
					klog.Warning("unexpected error")
				}

				if pod.ResourceVersion != "" {
					lastResourceVersion = pod.ResourceVersion
				}

				// Function code logic to get Pods
				modifiers := utils.GetManagers(pod.ManagedFields)
				klog.Infof("pod:%s has been %s by %s with resource version %v",
					pod.Name,
					event.Type,
					modifiers,
					lastResourceVersion,
				)

			}

		}
	}

}

func WatchFilteredPodUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string, labels string) {
	var lastResourceVersion string

	// Validate access before starting watcher
	if !utils.CanUserAccessResource(ctx, clientset, namespace, "pods", "get", "pod") {
		klog.Error("Shutdown no access")
		return
	}

	for {

		if ctx.Err() != nil {
			klog.Errorf("Shutting down watcher early : %v", ctx.Err())
			return
		}

		watchPods, err := clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
			LabelSelector:   labels,
			ResourceVersion: lastResourceVersion,
		})

		if err != nil {
			klog.Errorf("failed to start watcher %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		watchPodChan := watchPods.ResultChan()
		restartFlag := false

		for !restartFlag {
			select {
			case <-ctx.Done():
				watchPods.Stop()
				klog.Info("Shutdown Watcher.")
				return
			case event, ok := <-watchPodChan:
				if !ok {
					klog.Warning("watch pod channel closed restarted")
					restartFlag = true
					break
				}
				if event.Type == watch.Error {
					status, ok := event.Object.(*metav1.Status)
					if ok && status.Code == http.StatusGone {
						klog.Warning("resourceversion expired, resetting")
						lastResourceVersion = ""
					}
					restartFlag = true
					break
				}

				pod, ok := event.Object.(*v1.Pod)
				if !ok {
					klog.Warning("unexpected error")
				}

				modifiers := utils.GetManagers(pod.ManagedFields)
				labelsJson, err := json.Marshal(pod.GetLabels())
				if err != nil {
					klog.Errorf("Failed to marshal labels: %v", err)
					continue
				}
				klog.Infof("pod:%s has been %s by %s with labels %v",
					pod.Name,
					event.Type,
					modifiers,
					string(labelsJson),
				)

			}
		}

	}
}

func WatchBadImagePodUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string) {
	var lastResourceVersion string

	// Validate access before starting watcher
	if !utils.CanUserAccessResource(ctx, clientset, namespace, "pods", "get", "pod") {
		klog.Errorf("Shutdown no access to the resources inside namespace:%v", namespace)
		return
	}

	for {
		if ctx.Err() != nil {
			klog.Errorf("Shutting down watcher early : %v", ctx.Err())
			return
		}
		watchPods, err := clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
			ResourceVersion: lastResourceVersion,
		})
		if err != nil {
			klog.Errorf("failed to start watcher in namespace:%v with error:%v", namespace, err)
			time.Sleep(2 * time.Second)
			continue
		}
		watchPodChan := watchPods.ResultChan()
		restartFlag := false

		for !restartFlag {
			select {
			case <-ctx.Done():
				watchPods.Stop()
				klog.Info("Shutdown Watcher.")
				return
			case event, ok := <-watchPodChan:
				if !ok {
					klog.Warningf("watch pod channel closed in namespace:%vrestarted", namespace)
					restartFlag = true
					break
				}
				if event.Type == watch.Error {
					status, ok := event.Object.(*metav1.Status)
					if ok && status.Code == http.StatusGone {
						klog.Warningf("resourceversion expired in namespace:%v, resetting", namespace)
						lastResourceVersion = ""
					}
					restartFlag = true
					break
				}

				pod, ok := event.Object.(*v1.Pod)
				if !ok {
					klog.Warningf("reveived object of unexpected type:%T of event:%v in namespace:%v", event.Object, event.Type, namespace)
					continue
				}

				lastResourceVersion = pod.ResourceVersion

				if event.Type == watch.Added || event.Type == watch.Modified {
					for _, container := range pod.Spec.Containers {
						getImage := container.Image
						if !strings.Contains(getImage, ":") || strings.HasSuffix(getImage, ":latest") {
							klog.Warningf("Bad Pods without Image tag or latest %s/%s/%s", pod.Namespace, pod.Name, getImage)
						}
					}
					for _, initContainer := range pod.Spec.InitContainers {
						getImage := initContainer.Image
						if !strings.Contains(getImage, ":") || strings.HasSuffix(getImage, ":latest") {
							klog.Warningf("Bad Pods without Image tag or latest %s/%s/%s", pod.Namespace, pod.Name, getImage)
						}

					}

				}

			}
		}
	}
}

func DeleteBadImagePodUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string) {
	var lastResourceVersion string
	// Validate access before starting watcher
	if !utils.CanUserAccessResource(ctx, clientset, namespace, "pods", "get", "pod") {
		klog.Error("Shutdown no access")
		return
	}

	for {
		if ctx.Err() != nil {
			klog.Errorf("Shutting down watcher early : %v", ctx.Err())
			return
		}

		podsWatch, err := clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
			ResourceVersion: lastResourceVersion,
		})
		if err != nil {
			klog.Errorf("failed to start watcher in namespace:%v with error:%v", namespace, err)
			return
		}
		watchChan := podsWatch.ResultChan()
		restartFlag := false

		for !restartFlag {
			select {
			case <-ctx.Done():
				klog.Info("Context cancelled, stopping watcher")
				return
			case event, ok := <-watchChan:
				if !ok {
					klog.Warningf("Pod watcher channel closed for namespace:%v", namespace)
					restartFlag = true
					break
				}

				if event.Type == watch.Error {
					status, ok := event.Object.(*metav1.Status)
					if ok && status.Code == http.StatusGone {
						// Resource Version expired. We must reset and start from scratch.
						klog.Warningf("ResourceVersion expired for namespace %s. Resetting and restarting watch.", namespace)
						lastResourceVersion = ""
					} else if ok {
						// Generic API error (e.g., Permission denied, Internal server error).
						klog.Errorf("Received API error event for namespace %s: code=%d, message=%s",
							namespace, status.Code, status.Message)
					} else {
						// Unexpected error object.
						klog.Errorf("Received unparseable error event for namespace %s: %+v", namespace, event.Object)
					}
					restartFlag = true
					break
				}

				pod, ok := event.Object.(*v1.Pod)
				if !ok {
					klog.Warningf("reveived object of unexpected type:%T of event:%v in namespace:%v", event.Object, event.Type, namespace)
					continue
				}

				if protectedNamespaces[pod.Namespace] {
					klog.Infof("Skipping Pod in protected namespace: %s/%s", pod.Namespace, pod.Name)
					continue
				}

				if len(pod.OwnerReferences) != 0 {
					klog.Infof("Skipping Pod since its owned by controller: %s/%s/%s", pod.Namespace, pod.Name, pod.OwnerReferences)
					continue
				}

				if pod.ResourceVersion != "" {
					lastResourceVersion = pod.ResourceVersion
				}

				if event.Type == watch.Added || event.Type == watch.Modified {
					deletePod := false

					for _, container := range pod.Spec.Containers {
						getImage := container.Image
						if !strings.Contains(getImage, ":") || strings.HasSuffix(getImage, ":latest") {
							deletePod = true
						}
					}

					for _, initContainer := range pod.Spec.InitContainers {
						getImage := initContainer.Image
						if !strings.Contains(getImage, ":") || strings.HasSuffix(getImage, ":latest") {
							deletePod = true
						}

					}

					if deletePod {
						err := clientset.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
						if err != nil {
							klog.Errorf("Failed to delete pod %s: %v", pod.Name, err)
						} else {
							klog.Infof("Pod %s deleted successfully", pod.Name)
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
	}
}

func BackupNewPodJson(ctx context.Context, clientset *kubernetes.Clientset, namespace string) {
	bPod := clientset.CoreV1().Pods(namespace)

	podWatcher, err := bPod.Watch(ctx, metav1.ListOptions{})
	if err != nil {
		klog.Errorf("Unable to list pods %v", err)
	}
	defer podWatcher.Stop()

	backupRoot, err := os.Getwd()
	if err != nil {
		fmt.Println("Error getting current directory:", err)
		return
	}
	klog.Info("Pod backup watcher started...")

	watchChan := podWatcher.ResultChan()
	for {
		select {
		case <-ctx.Done():
			klog.Info("Context cancelled, stopping watcher")
			return
		case event, ok := <-watchChan:
			if !ok {
				klog.Info("Pod watcher channel closed")
				return
			}
			if event.Type != watch.Added {
				continue
			}
			pod, ok := event.Object.(*v1.Pod)
			if !ok {
				klog.Error("failed to parse pod object")
			}
			latestPod, err := clientset.CoreV1().Pods(pod.Namespace).Get(ctx,
				pod.Name,
				metav1.GetOptions{})

			if err != nil {
				klog.Errorf("Failed to GET pod %s/%s for backup: %v", pod.Namespace, pod.Name, err)
				continue
			}
			utils.SavePodJSON(latestPod, backupRoot)
		}
	}
}
