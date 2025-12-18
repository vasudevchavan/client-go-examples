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

var backoffTime = 1 * time.Second

func WatchFilteredPodUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string, labels string) {
	var latestResourceVersion string

	if !utils.CanUserAccessResource(ctx, clientset, namespace, "pods", "get", "pod") {
		klog.Errorf("Shutting down watcher for namespace:%s , Insufficient access to resource 'Pod'", namespace)
		return
	}

	for {
		if ctx.Err() != nil {
			klog.Infof("Shutdown watcher for Pods in namespace:%s", namespace)
			return
		}

		if latestResourceVersion == "" {
			// Create a List of Pods
			podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
				LabelSelector:   labels,
				ResourceVersion: latestResourceVersion,
			})

			if err != nil {
				klog.Errorf("Failed to list Pods in namespace:%s with error:%s", namespace, err)
				continue
			}

			latestResourceVersion = podList.ResourceVersion
			klog.Infof("Pods list sucessful namespace:%s and resource-version:%s", namespace, latestResourceVersion)

			// Create a Watcher
			WatchPod, err := clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
				LabelSelector: labels,
			})

			if err != nil {
				klog.Errorf("Failed to start watcher in namespace:%s error:%s", namespace, err)
				time.Sleep(backoffTime)
				continue
			}

			klog.Infof("Pod watcher started in namespace:%s", namespace)
			restartFlag := false

			for !restartFlag {
				select {
				case <-ctx.Done():
					klog.Infof("Shutdowning Watcher in namespace:%s", namespace)
					WatchPod.Stop()
					return
				case event, ok := <-WatchPod.ResultChan():
					if !ok {
						klog.Errorf("Watcher started failed in namespace:%s , restarting", namespace)
						restartFlag = true
						break
					}

					if event.Type == watch.Error {
						status, ok := event.Object.(*metav1.Status)
						if ok && status.Code == http.StatusGone {
							klog.Warningf("Resource version:%s expired in namespace:%s relisting",
								namespace, latestResourceVersion)
							latestResourceVersion = ""
						} else if ok {
							klog.Errorf("Watch API error (namespace:%s, code:%d error:%s)",
								namespace,
								status.Code,
								status.Message)

						} else {
							klog.Errorf("Unparsable watch error in namespace:%s error:%+v",
								namespace,
								event.Object)
						}
						restartFlag = true
						break
					}

					// Normal event
					pod, ok := event.Object.(*v1.Pod)
					if !ok {
						klog.Warningf("Unexpected object type:%T and event:%s in namespace:%s",
							event.Object, event.Type, namespace)
						continue
					}

					if pod.ResourceVersion != "" {
						latestResourceVersion = pod.ResourceVersion
					}

					podModifier := utils.GetManagers(pod.ManagedFields)
					labelsJson, err := json.Marshal(pod.GetLabels())
					if err != nil {
						klog.Errorf("Failed to marshal labels: %v", err)
						continue
					}
					klog.Infof("pod:%s has been %s by %s with labels %v in namespace:%s",
						pod.Name,
						event.Type,
						podModifier,
						string(labelsJson),
						pod.Namespace,
					)

				}
			}

		}

	}

}

func WatchBadImagePodUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string) {
	var latestResourceVersion string

	// Validate Access
	if !utils.CanUserAccessResource(ctx, clientset, namespace, "pods", "get", "pod") {
		klog.Errorf("Shutting down watcher for namespace:%s , Insufficient access to resource 'Pod'", namespace)
		return
	}

	for {
		if ctx.Err() != nil {
			klog.Infof("Shutdown watcher for Pods in namespace:%s", namespace)
			return
		}

		if latestResourceVersion == "" {
			podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
				ResourceVersion: latestResourceVersion,
			})
			if err != nil {
				klog.Errorf("Failed to list Pods in namespace:%s with error:%s", namespace, err)
				continue
			}
			latestResourceVersion = podList.ResourceVersion
			klog.Infof("Pods list sucessful namespace:%s and resource-version:%s", namespace, latestResourceVersion)

			// Watcher for Pod

			watchPod, err := clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{})

			if err != nil {
				klog.Errorf("Failed to start watcher in namespace:%s error:%s", namespace, err)
				time.Sleep(backoffTime)
				continue
			}

			klog.Infof("Pod watcher started in namespace:%s", namespace)
			restartFlag := false

			for !restartFlag {
				select {
				case <-ctx.Done():
					klog.Infof("Shutdowning Watcher in namespace:%s", namespace)
					watchPod.Stop()
					return
				case event, ok := <-watchPod.ResultChan():
					if !ok {
						klog.Errorf("Watcher started failed in namespace:%s , restarting", namespace)
						restartFlag = true
						break
					}

					if event.Type == watch.Error {
						status, ok := event.Object.(*metav1.Status)
						if ok && status.Code == http.StatusGone {
							klog.Warningf("Resource version:%s expired in namespace:%s relisting",
								namespace, latestResourceVersion)
							latestResourceVersion = ""
						} else if ok {
							klog.Errorf("Watch API error (namespace:%s, code:%d error:%s)",
								namespace,
								status.Code,
								status.Message)

						} else {
							klog.Errorf("Unparsable watch error in namespace:%s error:%+v",
								namespace,
								event.Object)
						}
						restartFlag = true
						break
					}

					// Normal event
					pod, ok := event.Object.(*v1.Pod)
					if !ok {
						klog.Warningf("Unexpected object type:%T and event:%s in namespace:%s",
							event.Object, event.Type, namespace)
						continue
					}

					if pod.ResourceVersion != "" {
						latestResourceVersion = pod.ResourceVersion
					}

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
}

func DeleteBadImagePodUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string) {
	var latestResourceVersion string

	// Validate Access
	if !utils.CanUserAccessResource(ctx, clientset, namespace, "pods", "get", "pod") {
		klog.Errorf("Shutting down watcher for namespace:%s , Insufficient access to resource 'Pod'", namespace)
		return
	}

	for {
		if ctx.Err() != nil {
			klog.Infof("Shutdown watcher for Pods in namespace:%s", namespace)
			return
		}

		if latestResourceVersion == "" {
			podList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
			if err != nil {
				klog.Errorf("Failed to list Pods in namespace:%s with error:%s", namespace, err)
				continue
			}
			latestResourceVersion = podList.ResourceVersion
			klog.Infof("Pods list sucessful namespace:%s and resource-version:%s", namespace, latestResourceVersion)

			// Watcher for Pod

			watchPod, err := clientset.CoreV1().Pods(namespace).Watch(ctx, metav1.ListOptions{
				ResourceVersion: latestResourceVersion,
			})

			if err != nil {
				klog.Errorf("Failed to start watcher in namespace:%s error:%s", namespace, err)
				time.Sleep(backoffTime)
				continue
			}

			klog.Infof("Pod watcher started in namespace:%s", namespace)
			restartFlag := false

			for !restartFlag {
				select {
				case <-ctx.Done():
					klog.Infof("Shutdowning Watcher in namespace:%s", namespace)
					watchPod.Stop()
					return
				case event, ok := <-watchPod.ResultChan():
					if !ok {
						klog.Errorf("Watcher started failed in namespace:%s , restarting", namespace)
						restartFlag = true
						time.Sleep(backoffTime)
						break
					}

					if event.Type == watch.Error {
						status, ok := event.Object.(*metav1.Status)
						if ok && status.Code == http.StatusGone {
							klog.Warningf("Resource version:%s expired in namespace:%s relisting",
								namespace, latestResourceVersion)
							latestResourceVersion = ""
						} else if ok {
							klog.Errorf("Watch API error (namespace:%s, code:%d error:%s)",
								namespace,
								status.Code,
								status.Message)

						} else {
							klog.Errorf("Unparsable watch error in namespace:%s error:%+v",
								namespace,
								event.Object)
						}
						restartFlag = true
						break
					}

					// Normal event
					pod, ok := event.Object.(*v1.Pod)
					if !ok {
						klog.Warningf("Unexpected object type:%T and event:%s in namespace:%s",
							event.Object, event.Type, namespace)
						continue
					}

					if pod.ResourceVersion != "" {
						latestResourceVersion = pod.ResourceVersion
					}

					if len(pod.OwnerReferences) > 0 {
						klog.Infof("Skipping pod %s: owned by controller", pod.Name)
						continue
					}

					// if event.Type == watch.Added || event.Type == watch.Modified {
					if event.Type == watch.Added {
						deletepod := false
						for _, container := range pod.Spec.Containers {
							getImage := container.Image
							if !strings.Contains(getImage, ":") || strings.HasSuffix(getImage, ":latest") {
								klog.Warningf("Bad Pods without Image tag or latest %s/%s/%s", pod.Namespace, pod.Name, getImage)
								deletepod = true
								break
							}
						}
						if !deletepod {
							for _, initContainer := range pod.Spec.InitContainers {
								getImage := initContainer.Image
								if !strings.Contains(getImage, ":") || strings.HasSuffix(getImage, ":latest") {
									klog.Warningf("Bad Pods without Image tag or latest %s/%s/%s", pod.Namespace, pod.Name, getImage)
									deletepod = true
									break
								}

							}
						}

						if pod.DeletionTimestamp != nil {
							continue
						}

						if deletepod {

							err := clientset.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{})
							if err != nil {
								klog.Errorf("Failed to delete pod %s: %v", pod.Name, err)
							} else {
								klog.Infof("Pod %s deleted successfully", pod.Name)
								time.Sleep(backoffTime)
							}
						}

					}

				}
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
