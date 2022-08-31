package storagenetwork

import (
	"context"
	"reflect"
	"strconv"
	"time"

	longhornv1 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	//ctlcorev1 "github.com/rancher/wrangler/pkg/generated/controllers/core/v1"
	"github.com/sirupsen/logrus"
	//corev1 "k8s.io/api/core/v1"
	//"github.com/rancher/wrangler/pkg/condition"
	v1 "github.com/rancher/wrangler/pkg/generated/controllers/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	harvesterv1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	"github.com/harvester/harvester/pkg/config"
	ctlharvesterv1 "github.com/harvester/harvester/pkg/generated/controllers/harvesterhci.io/v1beta1"
	//ctlkubevirtv1 "github.com/harvester/harvester/pkg/generated/controllers/kubevirt.io/v1"
	ctllonghornv1 "github.com/harvester/harvester/pkg/generated/controllers/longhorn.io/v1beta1"
	ctlmonitoringv1 "github.com/harvester/harvester/pkg/generated/controllers/monitoring.coreos.com/v1"
	"github.com/harvester/harvester/pkg/settings"
	"github.com/harvester/harvester/pkg/util"
	ctlmgmtv3 "github.com/rancher/rancher/pkg/generated/controllers/management.cattle.io/v3"
)

const (
	ControllerName                  = "harvester-storage-network-controller"
	StorageNetworkAnnotation        = "storage-network.settings.harvesterhci.io"
	ReplicaStorageNetworkAnnotation = StorageNetworkAnnotation + "/replica"
	PausedStorageNetworkAnnotation  = StorageNetworkAnnotation + "/paused"

	longhornStorageNetworkName = "storage-network"

	// Rancher monitoring
	CattleMonitoringSystemNamespace = "cattle-monitoring-system"
	RancherMonitoringPrometheus     = "rancher-monitoring-prometheus"
	FleetLocalNamespace             = "fleet-local"
	RancherMonitoring               = "rancher-monitoring"
	RancherMonitoringGrafana        = "rancher-monitoring-grafana"
)

type Handler struct {
	ctx                  context.Context
	longhornSettings     ctllonghornv1.SettingClient
	longhornSettingCache ctllonghornv1.SettingCache
	longhornVolumes      ctllonghornv1.VolumeClient
	longhornVolumeCache  ctllonghornv1.VolumeCache
	prometheus           ctlmonitoringv1.PrometheusClient
	prometheusCache      ctlmonitoringv1.PrometheusCache
	deployments          v1.DeploymentClient
	deploymentCache      v1.DeploymentCache
	managedCharts        ctlmgmtv3.ManagedChartClient
	managedChartCache    ctlmgmtv3.ManagedChartCache
	settings             ctlharvesterv1.SettingClient
	settingsController   ctlharvesterv1.SettingController
}

// register the setting controller and reconsile longhorn setting when storage network changed
func Register(ctx context.Context, management *config.Management, opts config.Options) error {
	settings := management.HarvesterFactory.Harvesterhci().V1beta1().Setting()
	longhornSettings := management.LonghornFactory.Longhorn().V1beta1().Setting()
	longhornVolumes := management.LonghornFactory.Longhorn().V1beta1().Volume()
	prometheus := management.MonitoringFactory.Monitoring().V1().Prometheus()
	deployments := management.AppsFactory.Apps().V1().Deployment()
	managedCharts := management.RancherManagementFactory.Management().V3().ManagedChart()

	controller := &Handler{
		ctx:                  ctx,
		longhornSettings:     longhornSettings,
		longhornSettingCache: longhornSettings.Cache(),
		longhornVolumes:      longhornVolumes,
		longhornVolumeCache:  longhornVolumes.Cache(),
		prometheus:           prometheus,
		prometheusCache:      prometheus.Cache(),
		settings:             settings,
		settingsController:   settings,
		deployments:          deployments,
		deploymentCache:      deployments.Cache(),
		managedCharts:        managedCharts,
		managedChartCache:    managedCharts.Cache(),
	}

	settings.OnChange(ctx, ControllerName, controller.OnStorageNetworkChange)
	return nil
}

// webhook needs check if VMs are off
// webhook needs check if nad is existing
func (h *Handler) OnStorageNetworkChange(key string, setting *harvesterv1.Setting) (*harvesterv1.Setting, error) {
	if setting == nil || setting.DeletionTimestamp != nil || setting.Name != settings.StorageNetworkName {
		return nil, nil
	}

	if setting.Value == h.getLonghornStorageNetwork() {
		// check if we need to restart monitoring pods
		if !h.checkPodStatusAndStart() {
			h.settingsController.EnqueueAfter(setting.Name, 5*time.Second)
		}

		// finish
		return nil, nil
	}

	logrus.Infof("storage network change:%s", setting.Value)

	// if replica eq 0, skip
	// save replica to annotation
	// set replica to 0
	if !h.checkPodStatusAndStop() {
		logrus.Infof("Requeue to check pod status again")
		h.settingsController.EnqueueAfter(setting.Name, 5*time.Second)
		return nil, nil
	}

	logrus.Infof("all pods are stopped")

	// check volume detach before put LH settings
	if !h.checkLonghornVolumeDetach() {
		logrus.Infof("still has attached volume")
		h.settingsController.EnqueueAfter(setting.Name, 5*time.Second)
		return nil, nil
	}

	logrus.Infof("all volumes are detached")
	logrus.Infof("update Longhorn settings")
	// push LH setting
	if err := h.updateLonghornStorageNetwork(setting.Value); err != nil {
		logrus.Warnf("Update Longhorn setting error %v", err)
	}
	h.settingsController.EnqueueAfter(setting.Name, 5*time.Second)

	return nil, nil
}

// true: all detach
func (h *Handler) checkLonghornVolumeDetach() bool {
	if volumes, err := h.longhornVolumeCache.List(util.LonghornSystemNamespaceName, labels.Everything()); err == nil {
		for _, volume := range volumes {
			logrus.Infof("volume state: %v", volume.Status.State)
			if volume.Status.State != "detached" {
				return false
			}
		}
		return true
	} else {
		logrus.Warnf("volume error %v", err)
	}
	return true
}

// check Pod status, if all pods are start, return true
func (h *Handler) checkPodStatusAndStart() bool {
	allStarted := true

	// check prometheus cattle-monitoring-system/rancher-monitoring-prometheus replica
	if prometheus, err := h.prometheusCache.Get(CattleMonitoringSystemNamespace, RancherMonitoringPrometheus); err == nil {
		logrus.Infof("prometheus: %v", *prometheus.Spec.Replicas)
		// check started or not
		if *prometheus.Spec.Replicas == 0 {
			logrus.Infof("start prometheus")
			allStarted = false
			prometheusCopy := prometheus.DeepCopy()
			if replicas, err := strconv.Atoi(prometheus.Annotations[ReplicaStorageNetworkAnnotation]); err == nil {
				*prometheusCopy.Spec.Replicas = int32(replicas)
			}
			delete(prometheusCopy.Annotations, ReplicaStorageNetworkAnnotation)

			if _, err := h.prometheus.Update(prometheusCopy); err != nil {
				logrus.Warnf("prometheus update error %v", err)
			}
		}
	}

	// check managedchart fleet-local/rancher-monitoring paused
	if monitoring, err := h.managedChartCache.Get(FleetLocalNamespace, RancherMonitoring); err == nil {
		logrus.Infof("Rancher Monitoring: %v", monitoring.Spec.Paused)
		// check pause or not
		if monitoring.Spec.Paused {
			logrus.Infof("start rancher monitoring")
			allStarted = false
			monitoringCopy := monitoring.DeepCopy()
			monitoringCopy.Spec.Paused = false
			delete(monitoringCopy.Annotations, PausedStorageNetworkAnnotation)

			if _, err := h.managedCharts.Update(monitoringCopy); err != nil {
				logrus.Warnf("rancher monitoring error %v", err)
			}
		}
	}

	// check deployment cattle-monitoring-system/rancher-monitoring-grafana replica
	if grafana, err := h.deploymentCache.Get(CattleMonitoringSystemNamespace, RancherMonitoringGrafana); err == nil {
		logrus.Infof("Grafana: %v", *grafana.Spec.Replicas)
		// check started or not
		if *grafana.Spec.Replicas == 0 {
			logrus.Infof("start grafana")
			allStarted = false
			grafanaCopy := grafana.DeepCopy()
			if replicas, err := strconv.Atoi(grafana.Annotations[ReplicaStorageNetworkAnnotation]); err == nil {
				*grafanaCopy.Spec.Replicas = int32(replicas)
			}
			delete(grafanaCopy.Annotations, ReplicaStorageNetworkAnnotation)

			if _, err := h.deployments.Update(grafanaCopy); err != nil {
				logrus.Warnf("Grafana update error %v", err)
			}
		}
	}

	return allStarted
}

// check Pod status, if all pods are stopped, return true
func (h *Handler) checkPodStatusAndStop() bool {
	allStopped := true

	// check prometheus cattle-monitoring-system/rancher-monitoring-prometheus replica
	if prometheus, err := h.prometheusCache.Get(CattleMonitoringSystemNamespace, RancherMonitoringPrometheus); err == nil {
		logrus.Infof("prometheus: %v", *prometheus.Spec.Replicas)
		// check stopped or not
		if *prometheus.Spec.Replicas != 0 {
			logrus.Infof("stop prometheus")
			allStopped = false
			prometheusCopy := prometheus.DeepCopy()
			prometheusCopy.Annotations[ReplicaStorageNetworkAnnotation] = strconv.Itoa(int(*prometheus.Spec.Replicas))
			*prometheusCopy.Spec.Replicas = 0

			if _, err := h.prometheus.Update(prometheusCopy); err != nil {
				logrus.Warnf("prometheus update error %v", err)
			}
		}
	}

	// check managedchart fleet-local/rancher-monitoring paused
	if monitoring, err := h.managedChartCache.Get(FleetLocalNamespace, RancherMonitoring); err == nil {
		logrus.Infof("Rancher Monitoring: %v", monitoring.Spec.Paused)
		// check pause or not
		if !monitoring.Spec.Paused {
			logrus.Infof("stop rancher monitoring")
			allStopped = false
			monitoringCopy := monitoring.DeepCopy()
			monitoringCopy.Annotations[PausedStorageNetworkAnnotation] = "false"
			monitoringCopy.Spec.Paused = true

			if _, err := h.managedCharts.Update(monitoringCopy); err != nil {
				logrus.Warnf("rancher monitoring error %v", err)
			}
		}
	}

	// check deployment cattle-monitoring-system/rancher-monitoring-grafana replica
	if grafana, err := h.deploymentCache.Get(CattleMonitoringSystemNamespace, RancherMonitoringGrafana); err == nil {
		logrus.Infof("Grafana: %v", *grafana.Spec.Replicas)
		// check stopped or not
		if *grafana.Spec.Replicas != 0 {
			logrus.Infof("stop grafana")
			allStopped = false
			grafanaCopy := grafana.DeepCopy()
			grafanaCopy.Annotations[ReplicaStorageNetworkAnnotation] = strconv.Itoa(int(*grafana.Spec.Replicas))
			*grafanaCopy.Spec.Replicas = 0

			if _, err := h.deployments.Update(grafanaCopy); err != nil {
				logrus.Warnf("Grafana update error %v", err)
			}
		}
	}

	return allStopped
}

func (h *Handler) getLonghornStorageNetwork() string {
	storage, err := h.longhornSettingCache.Get(util.LonghornSystemNamespaceName, longhornStorageNetworkName)
	if err != nil {
		return ""
	}
	return storage.Value
}

func (h *Handler) updateLonghornStorageNetwork(storageNetwork string) error {
	storage, err := h.longhornSettingCache.Get(util.LonghornSystemNamespaceName, longhornStorageNetworkName)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}

		if _, err := h.longhornSettings.Create(&longhornv1.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name:      longhornStorageNetworkName,
				Namespace: util.LonghornSystemNamespaceName,
			},
			Value: storageNetwork,
		}); err != nil {
			return err
		}
		return nil
	}

	storageCpy := storage.DeepCopy()
	storageCpy.Value = storageNetwork

	if !reflect.DeepEqual(storage, storageCpy) {
		_, err := h.longhornSettings.Update(storageCpy)
		return err
	}
	return nil
}
