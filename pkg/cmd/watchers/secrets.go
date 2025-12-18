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

func WatchFilteredSecretsUsingWatcher(ctx context.Context, clientset *kubernetes.Clientset, namespace string, labels string) {
	var latestResourceVersion string

	if !utils.CanUserAccessResource(ctx, clientset, namespace, "secrets", "get", "secret") {
		klog.Errorf("Shutting down watcher for namespace:%s , Insufficient access to resource 'secret'", namespace)
		return
	}

	for {
		if ctx.Err() != nil {
			klog.Infof("Shutdown watcher for secret in namespace:%s", namespace)
			return
		}

		if latestResourceVersion == "" {
			// Create a List of Pods
			secList, err := clientset.CoreV1().Secrets(namespace).List(ctx, metav1.ListOptions{
				LabelSelector:   labels,
				ResourceVersion: latestResourceVersion,
			})

			if err != nil {
				klog.Errorf("Failed to list secret in namespace:%s with error:%s", namespace, err)
				continue
			}

			latestResourceVersion = secList.ResourceVersion
			klog.Infof("Pods list sucessful namespace:%s and resource-version:%s", namespace, latestResourceVersion)

			// Create a Watcher
			watchSec, err := clientset.CoreV1().Secrets(namespace).Watch(ctx, metav1.ListOptions{
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
					watchSec.Stop()
					return
				case event, ok := <-watchSec.ResultChan():
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
					sec, ok := event.Object.(*v1.Secret)
					if !ok {
						klog.Warningf("Unexpected object type:%T and event:%s in namespace:%s",
							event.Object, event.Type, namespace)
						continue
					}

					if sec.ResourceVersion != "" {
						latestResourceVersion = sec.ResourceVersion
					}

					secModifier := utils.GetManagers(sec.ManagedFields)
					labelsJson, err := json.Marshal(sec.GetLabels())
					if err != nil {
						klog.Errorf("Failed to marshal labels: %v", err)
						continue
					}
					klog.Infof("pod:%s has been %s by %s with labels %v in namespace:%s",
						sec.Name,
						event.Type,
						secModifier,
						string(labelsJson),
						sec.Namespace,
					)

				}
			}

		}

	}
}
