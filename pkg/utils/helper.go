package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	ts := time.Now().Format("20251229-155959")
	bkpDir := filepath.Join(loc, pod.Name)
	os.MkdirAll(bkpDir, 0755)

	filename := fmt.Sprintf("%s-%s.json", pod.Name, ts)
	path := filepath.Join(bkpDir, filename)

	jsonBytes, err := json.MarshalIndent(pod, "", "  ")
	if err != nil {
		klog.Errorf("unabelt to format pod spec to json %w", err)
		return
	}

	err = os.WriteFile(path, jsonBytes, 0644)
	if err != nil {
		klog.Errorf("Failed to write pod backup file %s: %v", path, err)
		return
	}
	klog.Infof("Backup saved: %s", path)
}
