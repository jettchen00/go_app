/*
说明：k8s apiServer接口的封装
创建人 jettchen
创建时间 2023/06/15
*/
package k8s

import (
	"context"

	"go_app/pkg/log"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// k8s client api库
type K8sClient struct {
	clientset *kubernetes.Clientset
	parentCtx context.Context
}

func NewClient(ctx context.Context, confFile string) (*K8sClient, error) {
	config, err := clientcmd.BuildConfigFromFlags("", confFile)
	if err != nil {
		log.Error("build config fail, err=%v", err)
		return nil, err
	}
	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Error("create client from config fail, err=%v", err)
		return nil, err
	}
	client := &K8sClient{
		clientset: clientset,
		parentCtx: ctx,
	}

	return client, nil
}

// 判断pod是否存在
func (c *K8sClient) IsPodExist(namespace string, podName string) bool {
	_, err := c.clientset.CoreV1().Pods(namespace).Get(c.parentCtx, podName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("pod is not found, namespace=%s, podName=%s, err=%v", namespace, podName, err)
			return false
		} else {
			log.Error("get pod fail, namespace=%s, podName=%s, err=%v", namespace, podName, err)
			return false
		}
	}
	return true
}
