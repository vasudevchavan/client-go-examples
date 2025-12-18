package watchers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/vasudevchavan/client-go-examples/pkg/utils"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// var backoffTime = 1 * time.Second

func WatchFilteredDepUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string, labels string) {
	var latestResourceVersion string

	if !utils.CanUserAccessResource(ctx, clientset, namespace, "deployments", "get", "deployment") {
		klog.Errorf("Shutting down Watcher for namespace %s: insufficient access to resource 'deployment'", namespace)
		return
	}

	for {
		if ctx.Err() != nil {
			klog.Errorf("Shutdown watcher for deployment in namespace:%v", namespace)
			return
		}
		if latestResourceVersion == "" {
			// Create a List of Pods
			depList, err := clientset.AppsV1().Deployments(namespace).List(ctx, metav1.ListOptions{
				LabelSelector: labels,
			})

			if err != nil {
				klog.Errorf("Failed to list deployment in namespace:%s with error:%s", namespace, err)
				continue
			}

			latestResourceVersion = depList.ResourceVersion
			klog.Infof("Pods list sucessful namespace:%s and resource-version:%s", namespace, latestResourceVersion)

			// Create a Watcher
			watchDep, err := clientset.AppsV1().Deployments(namespace).Watch(ctx, metav1.ListOptions{
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
					watchDep.Stop()
					return
				case event, ok := <-watchDep.ResultChan():
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
					dep, ok := event.Object.(*appsv1.Deployment)
					if !ok {
						klog.Warningf("Unexpected object type:%T and event:%s in namespace:%s",
							event.Object, event.Type, namespace)
						continue
					}

					if dep.ResourceVersion != "" {
						latestResourceVersion = dep.ResourceVersion
					}

					depModifier := utils.GetManagers(dep.ManagedFields)
					labelsJson, err := json.Marshal(dep.GetLabels())
					if err != nil {
						klog.Errorf("Failed to marshal labels: %v", err)
						continue
					}
					klog.Infof("pod:%s has been %s by %s with labels %v in namespace:%s",
						dep.Name,
						event.Type,
						depModifier,
						string(labelsJson),
						dep.Namespace,
					)

				}
			}

		}

	}
}
