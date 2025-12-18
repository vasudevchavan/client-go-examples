package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

func FormatOwnerReference(owner []metav1.OwnerReference) []string {
	owners := make([]string, 0, len(owner))
	for _, o := range owner {
		owners = append(owners, fmt.Sprintf("%s/%s", o.Kind, o.Name))
	}
	return owners
}

func GetManagers(mf []metav1.ManagedFieldsEntry) []string {
	actors := make([]string, 0, len(mf))
	for _, f := range mf {
		actors = append(actors, f.Manager)
	}
	return actors
}

func SavePodJSON(pod *v1.Pod, loc string) {
	ts := time.Now().Format("20060102-150405")
	bkpDir := filepath.Join(loc, pod.Name)
	os.MkdirAll(bkpDir, 0755)

	filename := fmt.Sprintf("%s-%s.json", pod.Name, ts)
	path := filepath.Join(bkpDir, filename)

	jsonBytes, err := json.MarshalIndent(pod, "", "  ")
	if err != nil {
		klog.Errorf("unable to format pod spec to json %v", err)
		return
	}

	err = os.WriteFile(path, jsonBytes, 0644)
	if err != nil {
		klog.Errorf("Failed to write pod backup file %s: %v", path, err)
		return
	}
	klog.Infof("Backup saved: %s", path)
}

func CanUserAccessResource(ctx context.Context, clientset *kubernetes.Clientset,
	namespace string,
	resourceType string,
	verb string,
	componentName string,
) bool {
	// Access review
	accessReview := &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace: namespace,
				Verb:      verb,
				Resource:  componentName,
			},
		},
	}

	response, err := clientset.AuthorizationV1().SelfSubjectAccessReviews().Create(ctx, accessReview, metav1.CreateOptions{})
	if err != nil {
		klog.Errorf("ERROR: Failed to perform SelfSubjectAccessReview for '%s' %s in namespace %s: %v", verb, componentName, namespace, err)

		return false
	}
	if response.Status.Allowed {
		klog.Infof("ACCESS GRANTED: User is allowed to '%s' %s in namespace %s.", verb, componentName, namespace)
		return true
	} else {
		reason := response.Status.Reason
		if reason == "" {
			reason = "No matching RBAC rule found."
		}
		klog.Warningf("ACCESS DENIED: User is NOT allowed to '%s' %s in namespace %s. Reason: %s",
			verb, componentName, namespace, reason)
		return false
	}

}
