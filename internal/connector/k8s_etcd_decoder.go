package connector

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	storagev1 "k8s.io/api/storage/v1"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

var (
	k8sScheme        = runtime.NewScheme()
	k8sDecoder       runtime.Decoder
	k8sDecoderInitOK bool
)

func init() {
	if err := initK8sScheme(); err != nil {
		k8sLog.Warn("初始化 K8s decoder 失败, 将使用轻量解码: %v", err)
		return
	}
}

func initK8sScheme() error {
	if err := corev1.AddToScheme(k8sScheme); err != nil {
		return err
	}
	if err := appsv1.AddToScheme(k8sScheme); err != nil {
		return err
	}
	if err := batchv1.AddToScheme(k8sScheme); err != nil {
		return err
	}
	if err := networkingv1.AddToScheme(k8sScheme); err != nil {
		return err
	}
	if err := rbacv1.AddToScheme(k8sScheme); err != nil {
		return err
	}
	if err := coordinationv1.AddToScheme(k8sScheme); err != nil {
		return err
	}
	if err := storagev1.AddToScheme(k8sScheme); err != nil {
		return err
	}
	if err := schedulingv1.AddToScheme(k8sScheme); err != nil {
		return err
	}
	if err := autoscalingv1.AddToScheme(k8sScheme); err != nil {
		return err
	}

	factory := serializer.NewCodecFactory(k8sScheme)
	k8sDecoder = factory.UniversalDeserializer()
	k8sDecoderInitOK = true
	return nil
}

type decodedK8sObject struct {
	Kind       string                  `json:"kind"`
	APIVersion string                  `json:"apiVersion"`
	Name       string                  `json:"name"`
	Namespace  string                  `json:"namespace,omitempty"`
	UID        string                  `json:"uid"`
	Labels     map[string]string       `json:"labels,omitempty"`
	Annos      map[string]string       `json:"annotations,omitempty"`
	RawJSON    string                  `json:"-"`
	GVK        schema.GroupVersionKind `json:"-"`
}

func decodeFullK8sObject(rawValue []byte) (*decodedK8sObject, error) {
	if !k8sDecoderInitOK {
		return nil, fmt.Errorf("K8s decoder 未初始化")
	}

	obj, gvk, err := k8sDecoder.Decode(rawValue, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("protobuf 解码失败: %w", err)
	}

	result := &decodedK8sObject{
		GVK: *gvk,
	}

	metaAccessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, fmt.Errorf("无法获取元数据: %w", err)
	}

	result.Kind = gvk.Kind
	result.APIVersion = gvk.GroupVersion().String()
	result.Name = metaAccessor.GetName()
	result.Namespace = metaAccessor.GetNamespace()
	result.UID = string(metaAccessor.GetUID())
	result.Labels = metaAccessor.GetLabels()
	result.Annos = metaAccessor.GetAnnotations()

	jsonBytes, err := json.MarshalIndent(obj, "", "  ")
	if err == nil {
		result.RawJSON = string(jsonBytes)
	}

	return result, nil
}

func formatDecodedK8sObject(meta *decodedK8sObject) (string, string) {
	var details strings.Builder

	details.WriteString(fmt.Sprintf("Kind:       %s\n", meta.Kind))
	details.WriteString(fmt.Sprintf("APIVersion: %s\n", meta.APIVersion))
	details.WriteString(fmt.Sprintf("Name:       %s\n", meta.Name))
	if meta.Namespace != "" {
		details.WriteString(fmt.Sprintf("Namespace:  %s\n", meta.Namespace))
	}
	details.WriteString(fmt.Sprintf("UID:        %s\n", meta.UID))

	if len(meta.Labels) > 0 {
		details.WriteString("Labels:\n")
		var keys []string
		for k := range meta.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			details.WriteString(fmt.Sprintf("  %s: %s\n", k, meta.Labels[k]))
		}
	}

	if len(meta.Annos) > 0 {
		details.WriteString("Annotations:\n")
		var keys []string
		for k := range meta.Annos {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			details.WriteString(fmt.Sprintf("  %s: %s\n", k, meta.Annos[k]))
		}
	}

	if meta.RawJSON != "" {
		var raw map[string]json.RawMessage
		if json.Unmarshal([]byte(meta.RawJSON), &raw) == nil {
			if specRaw, ok := raw["spec"]; ok {
				var specObj interface{}
				if json.Unmarshal(specRaw, &specObj) == nil {
					specBytes, _ := json.MarshalIndent(specObj, "  ", "  ")
					details.WriteString("Spec:\n")
					details.WriteString("  ")
					details.WriteString(strings.ReplaceAll(string(specBytes), "\n", "\n  "))
					details.WriteString("\n")
				}
			}
		}
	}

	msg := fmt.Sprintf("%s/%s", meta.Kind, meta.Name)
	return msg, details.String()
}
