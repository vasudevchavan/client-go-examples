package watchers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/vasudevchavan/client-go-examples/pkg/utils"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// var backoffTime = 1 * time.Second

func WatchFilteredCMUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string, labels string) {
	var lastResourceVersion string

	if !utils.CanUserAccessResource(ctx, clientset, namespace, "configmap", "get", "configmap") {
		klog.Errorf("Shutting down Watcher for namespace %s: insufficient access to resource 'configmap'", namespace)
		return
	}

	for {
		if ctx.Err() != nil {
			klog.Errorf("Shutdown watcher for ConfigMap in namespace:%v", namespace)
			return
		}

		watchCM, err := clientset.CoreV1().ConfigMaps(namespace).Watch(ctx, metav1.ListOptions{
			LabelSelector:   labels,
			ResourceVersion: lastResourceVersion,
		})
		if err != nil {
			klog.Errorf("Failed to start watcher in namespace:%v with error:%v", namespace, err)
			time.Sleep(backoffTime)
			continue
		}
		klog.V(2).Infof("Watcher established for namespace %s, starting from ResourceVersion: %s",
			namespace, lastResourceVersion)

		watchCMChan := watchCM.ResultChan()
		restartFlag := false
		for !restartFlag {
			select {
			case <-ctx.Done():
				watchCM.Stop()
				klog.Infof("Shutdown watcher in namespace:%v", namespace)
				return
			case event, ok := <-watchCMChan:
				if !ok {
					klog.Errorf("Watcher start failed in namespace:%v", namespace)
					restartFlag = true
					break
				}

				if event.Type == watch.Error {
					status, ok := event.Object.(*metav1.Status)
					if ok && status.Code == http.StatusGone {
						klog.Warningf("ResourceVersion expired in namespace %s. Resetting and restarting.", namespace)
						lastResourceVersion = ""
					} else if ok {
						klog.Errorf("Received API error for namespace:%v code:%d message:%s", namespace, status.Code, status.Message)
					} else {
						klog.Errorf("Received unparseable error event for namespace %s: %+v", namespace, event.Object)
					}
					restartFlag = true
					break
				}

				listConfigMap, ok := event.Object.(*v1.ConfigMap)
				if !ok {
					klog.Warningf("Received object of unexpected type %T for event %s in namespace %s. Skipping.",
						event.Object, event.Type, namespace)
					continue
				}

				if listConfigMap.ResourceVersion != "" {
					lastResourceVersion = listConfigMap.ResourceVersion
				}

				modifiers := utils.GetManagers(listConfigMap.ManagedFields)
				labelsJson, err := json.Marshal(listConfigMap.GetLabels())
				if err != nil {
					klog.Errorf("Failed to marshal labels: %v", err)
					continue
				}
				klog.Infof("Labels: %s", string(labelsJson))
				klog.Infof("ConfigMap:%s has been %s by %s", listConfigMap.Name, event.Type, modifiers)

			}
		}
	}
}
