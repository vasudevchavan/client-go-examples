package kubeconfig

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
)

func LoadKubeConfig() (*rest.Config, *kubernetes.Clientset, error) {
	var kubeconfig string

	if kubeConfigPath := os.Getenv("KUBECONFIG"); kubeConfigPath != "" {
		kubeconfig = kubeConfigPath
		klog.Infof("KUBECONFIG is defined:%s", kubeconfig)
	} else {
		kubeconfig = filepath.Join(
			homeDir(), ".kube", "config",
		)
		klog.Info("kubeconfig file was loaded from ~/home/.kube/config")
	}

	// Build the configuration from kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		config, err = rest.InClusterConfig()
		if err != nil {
			klog.Errorf("failed to building kubeconfig:%v", err)
			return config, nil, fmt.Errorf("failed to building kubeconfig: %v", err)

		}
	}
	// Create a new clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		klog.Error("failed to create clientset")
		return nil, nil, fmt.Errorf("error creating clientset: %w", err)
	}

	return config, clientset, nil
}

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE")
}
