package scalers

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	appsv1 "k8s.io/api/apps/v1"
	v2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

type parseCPUMemoryMetadataTestData struct {
	metricType v2.MetricTargetType
	metadata   map[string]string
	isError    bool
}

// A complete valid metadata example for reference
var validCPUMemoryMetadata = map[string]string{
	"type":            "Utilization",
	"value":           "50",
	"activationValue": "40",
}
var validContainerCPUMemoryMetadata = map[string]string{
	"type":          "Utilization",
	"value":         "50",
	"containerName": "foo",
}

var testCPUMemoryMetadata = []parseCPUMemoryMetadataTestData{
	{"", map[string]string{}, true},
	{"", validCPUMemoryMetadata, false},
	{"", validContainerCPUMemoryMetadata, false},
	{"", map[string]string{"type": "Utilization", "value": "50"}, false},
	{v2.UtilizationMetricType, map[string]string{"value": "50"}, false},
	{"", map[string]string{"type": "AverageValue", "value": "50"}, false},
	{v2.AverageValueMetricType, map[string]string{"value": "50"}, false},
	{"", map[string]string{"type": "AverageValue", "value": "50", "activationValue": "40"}, false},
	{"", map[string]string{"type": "Value", "value": "50"}, true},
	{v2.ValueMetricType, map[string]string{"value": "50"}, true},
	{"", map[string]string{"type": "AverageValue"}, true},
	{"", map[string]string{"type": "xxx", "value": "50"}, true},
}

var selectLabels = map[string]string{
	"app": "test-deployment",
}

func TestCPUMemoryParseMetadata(t *testing.T) {
	for _, testData := range testCPUMemoryMetadata {
		config := &scalersconfig.ScalerConfig{
			TriggerMetadata: testData.metadata,
			MetricType:      testData.metricType,
		}
		_, err := parseResourceMetadata(config, logr.Discard(), fake.NewFakeClient())
		if err != nil && !testData.isError {
			t.Error("Expected success but got error", err)
		}
		if testData.isError && err == nil {
			t.Error("Expected error but got success")
		}
	}
}

func TestGetMetricSpecForScaling(t *testing.T) {
	// Using trigger.metadata.type field for type
	config := &scalersconfig.ScalerConfig{
		TriggerMetadata: validCPUMemoryMetadata,
	}
	kubeClient := fake.NewFakeClient()
	scaler, _ := NewCPUMemoryScaler(v1.ResourceCPU, config, kubeClient)
	metricSpec := scaler.GetMetricSpecForScaling(context.Background())

	assert.Equal(t, metricSpec[0].Type, v2.ResourceMetricSourceType)
	assert.Equal(t, metricSpec[0].Resource.Name, v1.ResourceCPU)
	assert.Equal(t, metricSpec[0].Resource.Target.Type, v2.UtilizationMetricType)

	// Using trigger.metricType field for type
	config = &scalersconfig.ScalerConfig{
		TriggerMetadata: map[string]string{"value": "50"},
		MetricType:      v2.UtilizationMetricType,
	}
	scaler, _ = NewCPUMemoryScaler(v1.ResourceCPU, config, kubeClient)
	metricSpec = scaler.GetMetricSpecForScaling(context.Background())

	assert.Equal(t, metricSpec[0].Type, v2.ResourceMetricSourceType)
	assert.Equal(t, metricSpec[0].Resource.Name, v1.ResourceCPU)
	assert.Equal(t, metricSpec[0].Resource.Target.Type, v2.UtilizationMetricType)
}

func TestGetContainerMetricSpecForScaling(t *testing.T) {
	// Using trigger.metadata.type field for type
	config := &scalersconfig.ScalerConfig{
		TriggerMetadata: validContainerCPUMemoryMetadata,
	}
	kubeClient := fake.NewFakeClient()
	scaler, _ := NewCPUMemoryScaler(v1.ResourceCPU, config, kubeClient)
	metricSpec := scaler.GetMetricSpecForScaling(context.Background())

	assert.Equal(t, metricSpec[0].Type, v2.ContainerResourceMetricSourceType)
	assert.Equal(t, metricSpec[0].ContainerResource.Name, v1.ResourceCPU)
	assert.Equal(t, metricSpec[0].ContainerResource.Target.Type, v2.UtilizationMetricType)
	assert.Equal(t, metricSpec[0].ContainerResource.Container, validContainerCPUMemoryMetadata["containerName"])

	// Using trigger.metricType field for type
	config = &scalersconfig.ScalerConfig{
		TriggerMetadata: map[string]string{"value": "50", "containerName": "bar"},
		MetricType:      v2.UtilizationMetricType,
	}
	scaler, _ = NewCPUMemoryScaler(v1.ResourceCPU, config, kubeClient)
	metricSpec = scaler.GetMetricSpecForScaling(context.Background())

	assert.Equal(t, metricSpec[0].Type, v2.ContainerResourceMetricSourceType)
	assert.Equal(t, metricSpec[0].ContainerResource.Name, v1.ResourceCPU)
	assert.Equal(t, metricSpec[0].ContainerResource.Target.Type, v2.UtilizationMetricType)
	assert.Equal(t, metricSpec[0].ContainerResource.Container, "bar")
}

func createScaledObject() *kedav1alpha1.ScaledObject {
	maxReplicas := int32(3)
	minReplicas := int32(0)
	pollingInterval := int32(10)
	return &kedav1alpha1.ScaledObject{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "keda.sh/v1alpha1",
			Kind:       "ScaledObject",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-name",
			Namespace: "test-namespace",
		},
		Spec: kedav1alpha1.ScaledObjectSpec{
			MaxReplicaCount: &maxReplicas,
			MinReplicaCount: &minReplicas,
			PollingInterval: &pollingInterval,
			ScaleTargetRef: &kedav1alpha1.ScaleTarget{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "test-deployment",
			},
			Triggers: []kedav1alpha1.ScaleTriggers{
				{
					Type: "cpu",
					Metadata: map[string]string{
						"activationValue": "500",
						"value":           "800",
					},
					MetricType: v2.UtilizationMetricType,
				},
			},
		},
		Status: kedav1alpha1.ScaledObjectStatus{
			HpaName: "keda-hpa-test-name",
		},
	}
}

func createDeployment() *appsv1.Deployment {
	replicas := int32(1)
	deployment := &appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deployment",
			Namespace: "test-namespace",
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: selectLabels,
				},
			},
			Selector: &metav1.LabelSelector{
				MatchLabels: selectLabels,
			},
		},
	}
	return deployment
}

func createPod(cpuRequest string) *v1.Pod {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deployment-1",
			Namespace: "test-namespace",
			Labels:    selectLabels,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "test-container",
					Resources: v1.ResourceRequirements{
						Limits: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("600m"),
						},
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse(cpuRequest),
						},
					},
				},
			},
		},
		Status: v1.PodStatus{
			Phase: v1.PodRunning,
		},
	}

	return pod
}

func createPodMetrics(cpuUsage string) *metricsv1beta1.PodMetrics {
	metricsv1beta1.AddToScheme(scheme.Scheme)
	cpuQuantity, _ := resource.ParseQuantity(cpuUsage)
	return &metricsv1beta1.PodMetrics{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "metrics.k8s.io/v1beta1",
			Kind:       "PodMetrics",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-deployment-1",
			Namespace: "test-namespace",
			Labels:    selectLabels,
		},
		Containers: []metricsv1beta1.ContainerMetrics{
			{
				Name: "test-container",
				Usage: v1.ResourceList{
					v1.ResourceCPU: cpuQuantity,
				},
			},
		},
	}
}

func createHPAWithAverageUtilization(averageUtilization int32) (*v2.HorizontalPodAutoscaler, error) {
	minReplicas := int32(1)
	averageValue, err := resource.ParseQuantity("800m")
	if err != nil {
		return nil, fmt.Errorf("error parsing quantity: %s", err)
	}

	return &v2.HorizontalPodAutoscaler{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "autoscaling/v2",
			Kind:       "HorizontalPodAutoscaler",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "keda-hpa-test-name",
			Namespace: "test-namespace",
		},
		Spec: v2.HorizontalPodAutoscalerSpec{
			MaxReplicas: 3,
			MinReplicas: &minReplicas,
			Metrics: []v2.MetricSpec{

				{
					Type: v2.ResourceMetricSourceType,
					Resource: &v2.ResourceMetricSource{
						Name: v1.ResourceCPU,
						Target: v2.MetricTarget{
							AverageUtilization: &averageUtilization,
							Type:               v2.UtilizationMetricType,
						},
					},
				},
			},
		},
		Status: v2.HorizontalPodAutoscalerStatus{
			CurrentMetrics: []v2.MetricStatus{
				{
					Type: v2.ResourceMetricSourceType,
					Resource: &v2.ResourceMetricStatus{
						Name: v1.ResourceCPU,
						Current: v2.MetricValueStatus{
							AverageUtilization: &averageUtilization,
							AverageValue:       &averageValue,
						},
					},
				},
			},
		},
	}, nil
}

func TestGetMetricsAndActivity_IsActive(t *testing.T) {
	config := &scalersconfig.ScalerConfig{
		TriggerMetadata:         validCPUMemoryMetadata,
		ScalableObjectType:      "ScaledObject",
		ScalableObjectName:      "test-name",
		ScalableObjectNamespace: "test-namespace",
	}

	deployment := createDeployment()
	pod := createPod("400m")
	podMetrics := createPodMetrics("500m")

	err := kedav1alpha1.AddToScheme(scheme.Scheme)
	if err != nil {
		t.Errorf("Error adding to scheme: %s", err)
		return
	}

	kubeClient := fake.NewClientBuilder().WithObjects(deployment, pod, podMetrics, createScaledObject()).WithScheme(scheme.Scheme).Build()
	scaler, _ := NewCPUMemoryScaler(v1.ResourceCPU, config, kubeClient)

	_, isActive, _ := scaler.GetMetricsAndActivity(context.Background(), "cpu")
	assert.Equal(t, true, isActive)
}

func TestGetMetricsAndActivity_IsNotActive(t *testing.T) {
	config := &scalersconfig.ScalerConfig{
		TriggerMetadata:         validCPUMemoryMetadata,
		ScalableObjectType:      "ScaledObject",
		ScalableObjectName:      "test-name",
		ScalableObjectNamespace: "test-namespace",
	}

	deployment := createDeployment()
	pod := createPod("500m")
	podMetrics := createPodMetrics("400m")

	err := kedav1alpha1.AddToScheme(scheme.Scheme)
	if err != nil {
		t.Errorf("Error adding to scheme: %s", err)
		return
	}

	kubeClient := fake.NewClientBuilder().WithObjects(deployment, pod, podMetrics, createScaledObject()).WithScheme(scheme.Scheme).Build()
	scaler, _ := NewCPUMemoryScaler(v1.ResourceCPU, config, kubeClient)

	_, isActive, _ := scaler.GetMetricsAndActivity(context.Background(), "cpu")
	assert.Equal(t, isActive, false)
}
