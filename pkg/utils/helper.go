package utils

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
