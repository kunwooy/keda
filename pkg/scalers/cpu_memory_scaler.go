package scalers

import (
	"context"
	"fmt"
	"strconv"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	v2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	"k8s.io/metrics/pkg/apis/external_metrics"
	"k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kedav1alpha1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	"github.com/kedacore/keda/v2/pkg/scalers/scalersconfig"
)

type cpuMemoryScaler struct {
	metadata     *cpuMemoryMetadata
	resourceName v1.ResourceName
	logger       logr.Logger
	kubeClient   client.Client
}

type cpuMemoryMetadata struct {
	Type                         v2.MetricTargetType
	AverageValue                 *resource.Quantity
	AverageUtilization           *int32
	ContainerName                string
	ActivationAverageValue       *resource.Quantity
	ActivationAverageUtilization *int32
	ScalableObjectType           string
	Namespace                    string
	ScaleTargetName              string
	ScaleTargetKind              string
}

// NewCPUMemoryScaler creates a new cpuMemoryScaler
func NewCPUMemoryScaler(resourceName v1.ResourceName, config *scalersconfig.ScalerConfig, kubeClient client.Client) (Scaler, error) {
	logger := InitializeLogger(config, "cpu_memory_scaler")

	meta, parseErr := parseResourceMetadata(config, logger, kubeClient)
	if parseErr != nil {
		return nil, fmt.Errorf("error parsing %s metadata: %w", resourceName, parseErr)
	}

	return &cpuMemoryScaler{
		metadata:     meta,
		resourceName: resourceName,
		logger:       logger,
		kubeClient:   kubeClient,
	}, nil
}

func getScaleTarget(scalableObjectName, scalableObjectNamespace string, kubeClient client.Client) (string, string, error) {
	scaledObject := &kedav1alpha1.ScaledObject{}
	err := kubeClient.Get(context.Background(), types.NamespacedName{
		Name:      scalableObjectName,
		Namespace: scalableObjectNamespace,
	}, scaledObject)

	if err != nil {
		return "", "", err
	}

	if scaledObject.Spec.ScaleTargetRef == nil {
		return "", "", fmt.Errorf("scaled object %s has no scale target ref", scalableObjectName)
	}

	return scaledObject.Spec.ScaleTargetRef.Name, scaledObject.Spec.ScaleTargetRef.Kind, nil
}

func parseResourceMetadata(config *scalersconfig.ScalerConfig, logger logr.Logger, kubeClient client.Client) (*cpuMemoryMetadata, error) {
	meta := &cpuMemoryMetadata{}
	var value, activationValue string
	var ok bool
	value, ok = config.TriggerMetadata["type"]
	switch {
	case ok && value != "" && config.MetricType != "":
		return nil, fmt.Errorf("only one of trigger.metadata.type or trigger.metricType should be defined")
	case ok && value != "":
		logger.V(0).Info("trigger.metadata.type is deprecated in favor of trigger.metricType")
		meta.Type = v2.MetricTargetType(value)
	case config.MetricType != "":
		meta.Type = config.MetricType
	default:
		return nil, fmt.Errorf("no type given in neither trigger.metadata.type or trigger.metricType")
	}

	if value, ok = config.TriggerMetadata["value"]; !ok || value == "" {
		return nil, fmt.Errorf("no value given")
	}
	if activationValue, ok = config.TriggerMetadata["activationValue"]; !ok || activationValue == "" {
		activationValue = "0"
	}

	switch meta.Type {
	case v2.AverageValueMetricType:
		averageValueQuantity := resource.MustParse(value)
		meta.AverageValue = &averageValueQuantity

		activationValueQuantity := resource.MustParse(activationValue)
		meta.ActivationAverageValue = &activationValueQuantity
	case v2.UtilizationMetricType:
		valueNum, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			return nil, err
		}
		utilizationNum := int32(valueNum)
		meta.AverageUtilization = &utilizationNum

		valueNum, err = strconv.ParseInt(activationValue, 10, 32)
		if err != nil {
			return nil, err
		}
		activationAverageUtilization := int32(valueNum)
		meta.ActivationAverageUtilization = &activationAverageUtilization
	default:
		return nil, fmt.Errorf("unsupported metric type, allowed values are 'Utilization' or 'AverageValue'")
	}

	if value, ok = config.TriggerMetadata["containerName"]; ok && value != "" {
		meta.ContainerName = value
	}

	if config.ScalableObjectType == "ScaledObject" {
		scaleTargetName, scaleTargetKind, err := getScaleTarget(config.ScalableObjectName, config.ScalableObjectNamespace, kubeClient)
		if err != nil {
			return nil, err
		}

		meta.ScaleTargetName = scaleTargetName
		meta.ScaleTargetKind = scaleTargetKind
	}

	meta.ScalableObjectType = config.ScalableObjectType
	meta.Namespace = config.ScalableObjectNamespace

	return meta, nil
}

// Close no need for cpuMemory scaler
func (s *cpuMemoryScaler) Close(context.Context) error {
	return nil
}

// GetMetricSpecForScaling returns the metric spec for the HPA
func (s *cpuMemoryScaler) GetMetricSpecForScaling(context.Context) []v2.MetricSpec {
	var metricSpec v2.MetricSpec

	if s.metadata.ContainerName != "" {
		containerCPUMemoryMetric := &v2.ContainerResourceMetricSource{
			Name: s.resourceName,
			Target: v2.MetricTarget{
				Type:               s.metadata.Type,
				AverageUtilization: s.metadata.AverageUtilization,
				AverageValue:       s.metadata.AverageValue,
			},
			Container: s.metadata.ContainerName,
		}
		metricSpec = v2.MetricSpec{ContainerResource: containerCPUMemoryMetric, Type: v2.ContainerResourceMetricSourceType}
	} else {
		cpuMemoryMetric := &v2.ResourceMetricSource{
			Name: s.resourceName,
			Target: v2.MetricTarget{
				Type:               s.metadata.Type,
				AverageUtilization: s.metadata.AverageUtilization,
				AverageValue:       s.metadata.AverageValue,
			},
		}
		metricSpec = v2.MetricSpec{Resource: cpuMemoryMetric, Type: v2.ResourceMetricSourceType}
	}

	return []v2.MetricSpec{metricSpec}
}

func calculateAverage(total *resource.Quantity, count int64) *resource.Quantity {
	if count == 0 {
		return &resource.Quantity{}
	}

	// Convert the total to milli-units
	nanoValue := total.ScaledValue(resource.Nano)

	// Perform the division
	averageNanoValue := nanoValue / count

	// Create a new Quantity from the average milli-value
	return resource.NewScaledQuantity(averageNanoValue, resource.Nano)
}

func (s *cpuMemoryScaler) getAverageValue(ctx context.Context, metricName string) (*resource.Quantity, error) {
	podList, labelSelector, err := s.getPodList(ctx)
	if err != nil {
		return nil, err
	}

	podMetricsList, err := s.getPodMetricsList(ctx, labelSelector)
	if err != nil {
		return nil, err
	}

	totalValue := &resource.Quantity{}
	podCount := 0

	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		podMetrics := getPodMetrics(podMetricsList, pod.Name)
		if podMetrics == nil {
			continue
		}

		var metricValue *resource.Quantity
		if s.metadata.ContainerName != "" {
			containerMetrics := getContainerMetrics(podMetrics, s.metadata.ContainerName)
			if containerMetrics == nil {
				continue
			}
			metricValue = getResourceValue(containerMetrics, metricName)
		} else {
			metricValue = getPodResourceValue(podMetrics, metricName)
		}

		if metricValue == nil {
			return nil, fmt.Errorf("unsupported metric name: %s", metricName)
		}

		totalValue.Add(*metricValue)
		podCount++
	}

	if podCount == 0 {
		return nil, fmt.Errorf("no running pods found")
	}

	averageValue := calculateAverage(totalValue, int64(podCount))
	return averageValue, nil
}

func (s *cpuMemoryScaler) getAverageUtilization(ctx context.Context, metricName string) (*int32, error) {
	podList, labelSelector, err := s.getPodList(ctx)
	if err != nil {
		return nil, err
	}

	podMetricsList, err := s.getPodMetricsList(ctx, labelSelector)
	if err != nil {
		return nil, err
	}

	var totalUtilization int64
	podCount := 0

	for _, pod := range podList.Items {
		if pod.Status.Phase != corev1.PodRunning {
			continue
		}

		podMetrics := getPodMetrics(podMetricsList, pod.Name)
		if podMetrics == nil {
			continue
		}

		var metricValue, capacity int64
		if s.metadata.ContainerName != "" {
			containerMetrics := getContainerMetrics(podMetrics, s.metadata.ContainerName)
			if containerMetrics == nil {
				continue
			}
			metricValue = getResourceValueInMillis(containerMetrics, metricName)
			capacity = getContainerResourceCapacity(&pod, s.metadata.ContainerName, getResourceName(metricName))
		} else {
			metricValue = getPodResourceValueInMillis(podMetrics, metricName)
			capacity = getPodResourceCapacity(&pod, getResourceName(metricName))
		}

		if capacity == 0 {
			continue
		}

		utilization := (metricValue * 100) / capacity
		totalUtilization += utilization
		podCount++
	}

	if podCount == 0 {
		return nil, fmt.Errorf("no running pods found with non-zero capacity")
	}

	averageUtilization := int32(totalUtilization / int64(podCount))
	return &averageUtilization, nil
}

// Helper functions
func getResourceValue(containerMetrics *v1beta1.ContainerMetrics, metricName string) *resource.Quantity {
	switch metricName {
	case "cpu":
		return containerMetrics.Usage.Cpu()
	case "memory":
		return containerMetrics.Usage.Memory()
	default:
		return nil
	}
}

func getPodResourceValue(podMetrics *v1beta1.PodMetrics, metricName string) *resource.Quantity {
	var total resource.Quantity
	for _, container := range podMetrics.Containers {
		if value := getResourceValue(&container, metricName); value != nil {
			total.Add(*value)
		}
	}
	return &total
}

func getResourceValueInMillis(containerMetrics *v1beta1.ContainerMetrics, metricName string) int64 {
	switch metricName {
	case "cpu":
		return containerMetrics.Usage.Cpu().MilliValue()
	case "memory":
		return containerMetrics.Usage.Memory().Value()
	default:
		return 0
	}
}

func getPodResourceValueInMillis(podMetrics *v1beta1.PodMetrics, metricName string) int64 {
	var total int64
	for _, container := range podMetrics.Containers {
		total += getResourceValueInMillis(&container, metricName)
	}
	return total
}

func getResourceName(metricName string) corev1.ResourceName {
	switch metricName {
	case "cpu":
		return corev1.ResourceCPU
	case "memory":
		return corev1.ResourceMemory
	default:
		return ""
	}
}

func getPodResourceCapacity(pod *corev1.Pod, resourceName corev1.ResourceName) int64 {
	var total int64
	for _, container := range pod.Spec.Containers {
		if quantity, ok := container.Resources.Requests[resourceName]; ok {
			total += quantity.MilliValue()
		}
	}
	return total
}

func (s *cpuMemoryScaler) getPodList(ctx context.Context) (*corev1.PodList, labels.Selector, error) {
	var labelSelector labels.Selector

	switch s.metadata.ScaleTargetKind {
	case "Deployment":
		deployment := &appsv1.Deployment{}
		err := s.kubeClient.Get(ctx, types.NamespacedName{Namespace: s.metadata.Namespace, Name: s.metadata.ScaleTargetName}, deployment)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get deployment: %v", err)
		}
		labelSelector = labels.SelectorFromSet(deployment.Spec.Selector.MatchLabels)
	case "StatefulSet":
		statefulSet := &appsv1.StatefulSet{}
		err := s.kubeClient.Get(ctx, types.NamespacedName{Namespace: s.metadata.Namespace, Name: s.metadata.ScaleTargetName}, statefulSet)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get statefulset: %v", err)
		}
		labelSelector = labels.SelectorFromSet(statefulSet.Spec.Selector.MatchLabels)
	default:
		return nil, nil, fmt.Errorf("unsupported scalable object type: %s", s.metadata.ScalableObjectType)
	}

	podList := &corev1.PodList{}
	err := s.kubeClient.List(ctx, podList, &client.ListOptions{
		Namespace:     s.metadata.Namespace,
		LabelSelector: labelSelector,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list pods: %v", err)
	}

	return podList, labelSelector, nil
}

func (s *cpuMemoryScaler) getPodMetricsList(ctx context.Context, labelSelector labels.Selector) (*v1beta1.PodMetricsList, error) {
	//podMetricsList := &v1beta1.PodMetricsList{}
	//err := s.kubeClient.List(ctx, podMetricsList, &client.ListOptions{Namespace: s.metadata.Namespace})
	//if err != nil {
	//	return nil, fmt.Errorf("failed to get pod metrics: %v", err)
	//}
	//return podMetricsList, nil
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get in-cluster config: %v", err)
	}

	metricsClient, err := metrics.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics client: %v", err)
	}

	podsMetricsList, err := metricsClient.MetricsV1beta1().PodMetricses(s.metadata.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labelSelector.String(),
	})

	return podsMetricsList, err
}

func getPodMetrics(podMetricsList *v1beta1.PodMetricsList, podName string) *v1beta1.PodMetrics {
	for _, podMetrics := range podMetricsList.Items {
		if podMetrics.Name == podName {
			return &podMetrics
		}
	}
	return nil
}

func getContainerMetrics(podMetrics *v1beta1.PodMetrics, containerName string) *v1beta1.ContainerMetrics {
	for _, containerMetrics := range podMetrics.Containers {
		if containerMetrics.Name == containerName {
			return &containerMetrics
		}
	}
	return nil
}

func getContainerResourceCapacity(pod *corev1.Pod, containerName string, resourceName corev1.ResourceName) int64 {
	for _, container := range pod.Spec.Containers {
		if container.Name == containerName {
			if quantity, ok := container.Resources.Requests[resourceName]; ok {
				return quantity.MilliValue()
			}
		}
	}
	return 0
}

// GetMetricsAndActivity only returns the activity of the cpu/memory scaler
func (s *cpuMemoryScaler) GetMetricsAndActivity(ctx context.Context, metricName string) ([]external_metrics.ExternalMetricValue, bool, error) {
	switch s.metadata.Type {
	case v2.AverageValueMetricType:
		averageValue, err := s.getAverageValue(ctx, metricName)
		if err != nil {
			return nil, false, err
		}

		return nil, averageValue.Cmp(*s.metadata.ActivationAverageValue) == 1, nil
	case v2.UtilizationMetricType:
		averageUtilization, err := s.getAverageUtilization(ctx, metricName)
		if err != nil {
			return nil, false, err
		}

		return nil, *averageUtilization > *s.metadata.ActivationAverageUtilization, nil
	}

	return nil, false, fmt.Errorf("no matching resource metric found for %s", s.resourceName)
}
