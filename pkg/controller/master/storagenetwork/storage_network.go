package storagenetwork

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	cniv1 "github.com/containernetworking/cni/pkg/types"
	nadv1 "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	longhornv1 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	ctlmgmtv3 "github.com/rancher/rancher/pkg/generated/controllers/management.cattle.io/v3"
	v1 "github.com/rancher/wrangler/pkg/generated/controllers/apps/v1"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	harvesterv1 "github.com/harvester/harvester/pkg/apis/harvesterhci.io/v1beta1"
	"github.com/harvester/harvester/pkg/config"
	ctlharvesterv1 "github.com/harvester/harvester/pkg/generated/controllers/harvesterhci.io/v1beta1"
	ctlcniv1 "github.com/harvester/harvester/pkg/generated/controllers/k8s.cni.cncf.io/v1"
	ctllonghornv1 "github.com/harvester/harvester/pkg/generated/controllers/longhorn.io/v1beta1"
	ctlmonitoringv1 "github.com/harvester/harvester/pkg/generated/controllers/monitoring.coreos.com/v1"
	"github.com/harvester/harvester/pkg/settings"
	"github.com/harvester/harvester/pkg/util"
)

const (
	ControllerName                  = "harvester-storage-network-controller"
	StorageNetworkAnnotation        = "storage-network.settings.harvesterhci.io"
	ReplicaStorageNetworkAnnotation = StorageNetworkAnnotation + "/replica"
	PausedStorageNetworkAnnotation  = StorageNetworkAnnotation + "/paused"
	HashStorageNetworkAnnotation    = StorageNetworkAnnotation + "/hash"
	NadStorageNetworkAnnotation     = StorageNetworkAnnotation + "/net-attach-def"
	OldNadStorageNetworkAnnotation  = StorageNetworkAnnotation + "/old-net-attach-def"

	StorageNetworkNetAttachDefPrefix    = "storagenetwork-"
	StorageNetworkNetAttachDefNamespace = "harvester-system"

	BridgeSuffix = "-br"
	CNIVersion   = "0.3.1"
	DefaultPVID  = 1
	DefaultCNI   = "bridge"
	DefaultIPAM  = "whereabouts"

	// status
	ReasonInProgress         = "In Progress"
	ReasonCompleted          = "Completed"
	MsgRestartPod            = "Restarting Pods"
	MsgStopPod               = "Stoping Pods"
	MsgWaitForVolumes        = "Waiting for all volumes detached"
	MsgUpdateLonghornSetting = "Update Longhorn setting"

	longhornStorageNetworkName = "storage-network"

	// Rancher monitoring
	CattleMonitoringSystemNamespace = "cattle-monitoring-system"
	RancherMonitoringPrometheus     = "rancher-monitoring-prometheus"
	RancherMonitoringAlertmanager   = "rancher-monitoring-alertmanager"
	FleetLocalNamespace             = "fleet-local"
	RancherMonitoring               = "rancher-monitoring"
	RancherMonitoringGrafana        = "rancher-monitoring-grafana"

	// VM import controller
	HarvesterSystemNamespace    = "harvester-system"
	HarvesterVMImportController = "harvester-harvester-vm-import-controller"
)

type Config struct {
	ClusterNetwork string   `json:"clusterNetwork,omitempty"`
	Vlan           uint16   `json:"vlan,omitempty"`
	Range          string   `json:"range,omitempty"`
	Exclude        []string `json:"exclude,omitempty"`
}

// TODO use bridge & weherabouts struct directly
type BridgeConfig struct {
	cniv1.NetConf
	Bridge      string     `json:"bridge"`
	PromiscMode bool       `json:"promiscMode"`
	Vlan        uint16     `json:"vlan"`
	IPAM        IPAMConfig `json:"ipam"`
}

type IPAMConfig struct {
	Type    string   `json:"type"`
	Range   string   `json:"range"`
	Exclude []string `json:"exclude,omitempty"`
}

func NewBridgeConfig() *BridgeConfig {
	return &BridgeConfig{
		NetConf: cniv1.NetConf{
			CNIVersion: CNIVersion,
			Type:       DefaultCNI,
		},
		PromiscMode: true,
		Vlan:        DefaultPVID,
		IPAM: IPAMConfig{
			Type: DefaultIPAM,
		},
	}
}

type Handler struct {
	ctx                               context.Context
	longhornSettings                  ctllonghornv1.SettingClient
	longhornSettingCache              ctllonghornv1.SettingCache
	longhornVolumes                   ctllonghornv1.VolumeClient
	longhornVolumeCache               ctllonghornv1.VolumeCache
	prometheus                        ctlmonitoringv1.PrometheusClient
	prometheusCache                   ctlmonitoringv1.PrometheusCache
	alertmanager                      ctlmonitoringv1.AlertmanagerClient
	alertmanagerCache                 ctlmonitoringv1.AlertmanagerCache
	deployments                       v1.DeploymentClient
	deploymentCache                   v1.DeploymentCache
	managedCharts                     ctlmgmtv3.ManagedChartClient
	managedChartCache                 ctlmgmtv3.ManagedChartCache
	settings                          ctlharvesterv1.SettingClient
	settingsCache                     ctlharvesterv1.SettingCache
	settingsController                ctlharvesterv1.SettingController
	networkAttachmentDefinitions      ctlcniv1.NetworkAttachmentDefinitionClient
	networkAttachmentDefinitionsCache ctlcniv1.NetworkAttachmentDefinitionCache
}

// register the setting controller and reconsile longhorn setting when storage network changed
func Register(ctx context.Context, management *config.Management, opts config.Options) error {
	settings := management.HarvesterFactory.Harvesterhci().V1beta1().Setting()
	longhornSettings := management.LonghornFactory.Longhorn().V1beta1().Setting()
	longhornVolumes := management.LonghornFactory.Longhorn().V1beta1().Volume()
	prometheus := management.MonitoringFactory.Monitoring().V1().Prometheus()
	alertmanager := management.MonitoringFactory.Monitoring().V1().Alertmanager()
	deployments := management.AppsFactory.Apps().V1().Deployment()
	managedCharts := management.RancherManagementFactory.Management().V3().ManagedChart()
	networkAttachmentDefinitions := management.CniFactory.K8s().V1().NetworkAttachmentDefinition()

	controller := &Handler{
		ctx:                               ctx,
		longhornSettings:                  longhornSettings,
		longhornSettingCache:              longhornSettings.Cache(),
		longhornVolumes:                   longhornVolumes,
		longhornVolumeCache:               longhornVolumes.Cache(),
		prometheus:                        prometheus,
		prometheusCache:                   prometheus.Cache(),
		alertmanager:                      alertmanager,
		alertmanagerCache:                 alertmanager.Cache(),
		settings:                          settings,
		settingsCache:                     settings.Cache(),
		settingsController:                settings,
		deployments:                       deployments,
		deploymentCache:                   deployments.Cache(),
		managedCharts:                     managedCharts,
		managedChartCache:                 managedCharts.Cache(),
		networkAttachmentDefinitions:      networkAttachmentDefinitions,
		networkAttachmentDefinitionsCache: networkAttachmentDefinitions.Cache(),
	}

	settings.OnChange(ctx, ControllerName, controller.OnStorageNetworkChange)
	return nil
}

func (h *Handler) setConfiguredCondition(setting *harvesterv1.Setting, finish bool, reason string, msg string) (*harvesterv1.Setting, error) {

	settingCopy := setting.DeepCopy()
	if finish {
		harvesterv1.SettingConfigured.True(settingCopy)
	} else {
		harvesterv1.SettingConfigured.False(settingCopy)
	}

	harvesterv1.SettingConfigured.Reason(settingCopy, reason)
	harvesterv1.SettingConfigured.Message(settingCopy, msg)

	if !reflect.DeepEqual(settingCopy, setting) {
		s, err := h.settings.Update(settingCopy)
		if err != nil {
			return nil, err
		}
		return s, nil
	}

	return setting, nil
}

// webhook needs check if VMs are off
func (h *Handler) OnStorageNetworkChange(key string, setting *harvesterv1.Setting) (*harvesterv1.Setting, error) {
	if setting == nil || setting.DeletionTimestamp != nil || setting.Name != settings.StorageNetworkName {
		return setting, nil
	}
	settingCopy := setting.DeepCopy()

	if settingCopy.Annotations == nil {
		settingCopy.Annotations = make(map[string]string)
	}

	if ok, err := h.checkChange(settingCopy); !ok {
		return nil, err
	}

	if h.checkLonghornSetting(settingCopy) {
		// finish
		return nil, nil
	}

	logrus.Infof("storage network change: %s", settingCopy.Value)

	// if replica eq 0, skip
	// save replica to annotation
	// set replica to 0
	if !h.checkPodStatusAndStop() {
		logrus.Infof("Requeue to check pod status again")
		if _, err := h.setConfiguredCondition(settingCopy, false, ReasonInProgress, MsgStopPod); err != nil {
			logrus.Warnf("update status error %v", err)
		}
		h.settingsController.EnqueueAfter(settingCopy.Name, 5*time.Second)
		return nil, nil
	}

	logrus.Infof("all pods are stopped")

	// check volume detach before put LH settings
	if ok, err := h.checkLonghornVolumeDetach(); !ok {
		if err != nil {
			// log error only but not return error to controller
			logrus.Warnf("check Longhorn volume error: %v", err)
		}
		logrus.Infof("still has attached volume")
		if _, err := h.setConfiguredCondition(settingCopy, false, ReasonInProgress, MsgWaitForVolumes); err != nil {
			logrus.Warnf("update status error %v", err)
		}
		h.settingsController.EnqueueAfter(settingCopy.Name, 5*time.Second)
		return nil, nil
	}

	logrus.Infof("all volumes are detached")
	logrus.Infof("update Longhorn settings")
	// push LH setting
	nadName := settingCopy.Annotations[NadStorageNetworkAnnotation]
	if err := h.updateLonghornStorageNetwork(nadName); err != nil {
		logrus.Warnf("Update Longhorn setting error %v", err)
	}
	if _, err := h.setConfiguredCondition(settingCopy, false, ReasonInProgress, MsgUpdateLonghornSetting); err != nil {
		logrus.Warnf("update status error %v", err)
	}
	h.settingsController.EnqueueAfter(settingCopy.Name, 5*time.Second)

	return nil, nil
}

// calc sha1 hash
func (h *Handler) sha1(s string) string {
	hash := sha1.New()
	hash.Write([]byte(s))
	sha1sum := hash.Sum(nil)
	return fmt.Sprintf("%x", sha1sum)
}

func (h *Handler) checkHash(setting *harvesterv1.Setting) bool {
	currentHash := h.sha1(setting.Value)
	savedHash := setting.Annotations[HashStorageNetworkAnnotation]
	return currentHash == savedHash
}

func (h *Handler) saveHash(setting *harvesterv1.Setting) {
	setting.Annotations[HashStorageNetworkAnnotation] = h.sha1(setting.Value)
}

func (h *Handler) createAndSaveNad(setting *harvesterv1.Setting) error {
	var config Config
	bridgeConfig := NewBridgeConfig()

	if err := json.Unmarshal([]byte(setting.Value), &config); err != nil {
		logrus.Warnf("parsing value error %v", err)
		return err
	}

	bridgeConfig.Bridge = config.ClusterNetwork + BridgeSuffix
	bridgeConfig.IPAM.Range = config.Range

	if config.Vlan == 0 {
		config.Vlan = DefaultPVID
	}
	bridgeConfig.Vlan = config.Vlan

	if len(config.Exclude) > 0 {
		bridgeConfig.IPAM.Exclude = config.Exclude
	}

	nadConfig, err := json.Marshal(bridgeConfig)
	if err != nil {
		logrus.Warnf("output json error %v", err)
		return err
	}

	currentHash := h.sha1(setting.Value)
	nad := nadv1.NetworkAttachmentDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:      StorageNetworkNetAttachDefPrefix + currentHash,
			Namespace: StorageNetworkNetAttachDefNamespace,
		},
	}
	nad.Annotations = map[string]string{
		StorageNetworkAnnotation: "true",
	}
	nad.Spec.Config = string(nadConfig)

	// create nad
	if _, err := h.networkAttachmentDefinitions.Create(&nad); err != nil {
		logrus.Warnf("create net-attach-def failed %v", err)
		return err
	}

	// save nad name
	setting.Annotations[OldNadStorageNetworkAnnotation] = setting.Annotations[NadStorageNetworkAnnotation]
	setting.Annotations[NadStorageNetworkAnnotation] = nad.Namespace + "/" + nad.Name

	return nil
}

func (h *Handler) checkChange(setting *harvesterv1.Setting) (bool, error) {
	if !h.checkHash(setting) {
		if setting.Value == "" {
			setting.Annotations[OldNadStorageNetworkAnnotation] = setting.Annotations[NadStorageNetworkAnnotation]
			setting.Annotations[NadStorageNetworkAnnotation] = ""
			h.saveHash(setting)

			if _, err := h.setConfiguredCondition(setting, false, ReasonInProgress, "enqueue"); err != nil {
				logrus.Warnf("reset update status error %v", err)
			}
			h.settingsController.Enqueue(setting.Name)
			return false, nil
		} else {
			if err := h.createAndSaveNad(setting); err != nil {
				return false, err
			}

			h.saveHash(setting)

			if _, err := h.setConfiguredCondition(setting, false, ReasonInProgress, "create nad"); err != nil {
				logrus.Warnf("create nad update status error %v", err)
			}
			h.settingsController.Enqueue(setting.Name)
			return false, nil
		}
	}
	return true, nil
}

func (h *Handler) removeOldNad(setting *harvesterv1.Setting) error {
	oldNad := setting.Annotations[OldNadStorageNetworkAnnotation]
	if oldNad == "" {
		return nil
	}

	nadName := strings.Split(oldNad, "/")
	if len(nadName) != 2 {
		// ignore this error and skip
		logrus.Warnf("split nad namespace and name failed")
		setting.Annotations[OldNadStorageNetworkAnnotation] = ""
		return nil
	}
	namespace := nadName[0]
	name := nadName[1]

	if _, err := h.networkAttachmentDefinitionsCache.Get(namespace, name); err != nil {
		if apierrors.IsNotFound(err) {
			setting.Annotations[OldNadStorageNetworkAnnotation] = ""
			return nil
		}

		// retry again
		logrus.Warnf("check net-attach-def existing error %v", err)
		return err
	}

	if err := h.networkAttachmentDefinitions.Delete(namespace, name, &metav1.DeleteOptions{}); err != nil {
		logrus.Warnf("remove nad error %v", err)
		return err
	}

	setting.Annotations[OldNadStorageNetworkAnnotation] = ""
	return nil
}

// return true as finished
func (h *Handler) checkLonghornSetting(setting *harvesterv1.Setting) bool {
	value, err := h.getLonghornStorageNetwork()
	if err != nil {
		logrus.Warnf("get Longhorn settings error: %v", err)
		return false
	}

	currentNad := setting.Annotations[NadStorageNetworkAnnotation]
	if currentNad == value {
		// check if we need to restart monitoring pods
		if !h.checkPodStatusAndStart() {
			if _, err := h.setConfiguredCondition(setting, false, ReasonInProgress, MsgRestartPod); err != nil {
				logrus.Warnf("update status error %v", err)
			}
			h.settingsController.EnqueueAfter(setting.Name, 5*time.Second)
			return true
		}

		settingCopy := setting.DeepCopy()
		if err := h.removeOldNad(settingCopy); err != nil {
			logrus.Warnf("remove old nad error %v", err)
			h.settingsController.EnqueueAfter(setting.Name, 5*time.Second)
			return true
		}

		// finish
		if _, err := h.setConfiguredCondition(settingCopy, true, ReasonCompleted, ""); err != nil {
			logrus.Warnf("update status error %v", err)
		}
		return true
	}
	return false
}

// true: all detach
func (h *Handler) checkLonghornVolumeDetach() (bool, error) {
	volumes, err := h.longhornVolumeCache.List(util.LonghornSystemNamespaceName, labels.Everything())
	if err != nil {
		logrus.Warnf("volume error %v", err)
		return false, err
	}

	for _, volume := range volumes {
		logrus.Infof("volume state: %v", volume.Status.State)
		if volume.Status.State != "detached" {
			return false, nil
		}
	}
	return true, nil
}

func (h *Handler) checkPrometheusStatusAndStart() bool {
	// check prometheus cattle-monitoring-system/rancher-monitoring-prometheus replica
	prometheus, err := h.prometheusCache.Get(CattleMonitoringSystemNamespace, RancherMonitoringPrometheus)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("prometheus not found. skip")
			return true
		}
		logrus.Warnf("prometheus get error %v", err)
		return false
	}

	logrus.Infof("prometheus: %v", *prometheus.Spec.Replicas)
	// check started or not
	if *prometheus.Spec.Replicas == 0 {
		logrus.Infof("start prometheus")
		prometheusCopy := prometheus.DeepCopy()
		*prometheusCopy.Spec.Replicas = 1
		if replicas, err := strconv.Atoi(prometheus.Annotations[ReplicaStorageNetworkAnnotation]); err == nil {
			*prometheusCopy.Spec.Replicas = int32(replicas)
		}
		delete(prometheusCopy.Annotations, ReplicaStorageNetworkAnnotation)

		if _, err := h.prometheus.Update(prometheusCopy); err != nil {
			logrus.Warnf("prometheus update error %v", err)
			return false
		}
		return false
	}

	return true
}

func (h *Handler) checkAltermanagerStatusAndStart() bool {
	// check alertmanager cattle-monitoring-system/rancher-monitoring-alertmanager replica
	alertmanager, err := h.alertmanagerCache.Get(CattleMonitoringSystemNamespace, RancherMonitoringAlertmanager)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("Alertmanager not found. skip")
			return true
		}
		logrus.Warnf("alertmanager get error %v", err)
		return false
	}

	logrus.Infof("alertmanager: %v", *alertmanager.Spec.Replicas)
	// check started or not
	if *alertmanager.Spec.Replicas == 0 {
		logrus.Infof("start alertmanager")
		alertmanagerCopy := alertmanager.DeepCopy()
		*alertmanagerCopy.Spec.Replicas = 1
		if replicas, err := strconv.Atoi(alertmanager.Annotations[ReplicaStorageNetworkAnnotation]); err == nil {
			*alertmanagerCopy.Spec.Replicas = int32(replicas)
		}
		delete(alertmanagerCopy.Annotations, ReplicaStorageNetworkAnnotation)

		if _, err := h.alertmanager.Update(alertmanagerCopy); err != nil {
			logrus.Warnf("alertmanager update error %v", err)
			return false
		}
		return false
	}

	return true
}

func (h *Handler) checkGrafanaStatusAndStart() bool {
	// check deployment cattle-monitoring-system/rancher-monitoring-grafana replica
	grafana, err := h.deploymentCache.Get(CattleMonitoringSystemNamespace, RancherMonitoringGrafana)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("grafana not found. skip")
			return true
		}
		logrus.Warnf("grafana get error %v", err)
		return false
	}

	logrus.Infof("Grafana: %v", *grafana.Spec.Replicas)
	// check started or not
	if *grafana.Spec.Replicas == 0 {
		logrus.Infof("start grafana")
		grafanaCopy := grafana.DeepCopy()
		*grafanaCopy.Spec.Replicas = 1
		if replicas, err := strconv.Atoi(grafana.Annotations[ReplicaStorageNetworkAnnotation]); err == nil {
			*grafanaCopy.Spec.Replicas = int32(replicas)
		}
		delete(grafanaCopy.Annotations, ReplicaStorageNetworkAnnotation)

		if _, err := h.deployments.Update(grafanaCopy); err != nil {
			logrus.Warnf("Grafana update error %v", err)
			return false
		}
		return false
	}

	return true
}

func (h *Handler) checkRancherMonitoringStatusAndStart() bool {
	// check managedchart fleet-local/rancher-monitoring paused
	monitoring, err := h.managedChartCache.Get(FleetLocalNamespace, RancherMonitoring)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("rancher monitoring not found. skip")
			return true
		}
		logrus.Warnf("rancher monitoring get error %v", err)
		return false
	}

	logrus.Infof("Rancher Monitoring: %v", monitoring.Spec.Paused)
	// check pause or not
	if monitoring.Spec.Paused {
		logrus.Infof("start rancher monitoring")
		monitoringCopy := monitoring.DeepCopy()
		monitoringCopy.Spec.Paused = false
		delete(monitoringCopy.Annotations, PausedStorageNetworkAnnotation)

		if _, err := h.managedCharts.Update(monitoringCopy); err != nil {
			logrus.Warnf("rancher monitoring error %v", err)
			return false
		}
		return false
	}

	return true
}

func (h *Handler) checkVMImportControllerStatusAndStart() bool {
	// check deployment harvester-system/harvester-harvester-vm-import-controller replica
	vmimportcontroller, err := h.deploymentCache.Get(HarvesterSystemNamespace, HarvesterVMImportController)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("VM import controller not found. skip")
			return true
		}
		logrus.Warnf("vm import controller get error %v", err)
		return false
	}

	logrus.Infof("VM Import Controller: %v", *vmimportcontroller.Spec.Replicas)
	// check started or not
	if *vmimportcontroller.Spec.Replicas == 0 {
		logrus.Infof("start vm import controller")
		vmimportcontrollerCopy := vmimportcontroller.DeepCopy()
		*vmimportcontrollerCopy.Spec.Replicas = 1
		if replicas, err := strconv.Atoi(vmimportcontroller.Annotations[ReplicaStorageNetworkAnnotation]); err == nil {
			*vmimportcontrollerCopy.Spec.Replicas = int32(replicas)
		}
		delete(vmimportcontrollerCopy.Annotations, ReplicaStorageNetworkAnnotation)

		if _, err := h.deployments.Update(vmimportcontrollerCopy); err != nil {
			logrus.Warnf("VM Import Controller update error %v", err)
			return false
		}
		return false
	}

	return true
}

// check Pod status, if all pods are start, return true
func (h *Handler) checkPodStatusAndStart() bool {
	allStarted := true

	if !h.checkPrometheusStatusAndStart() {
		allStarted = false
	}

	if !h.checkAltermanagerStatusAndStart() {
		allStarted = false
	}

	if !h.checkGrafanaStatusAndStart() {
		allStarted = false
	}

	if !h.checkRancherMonitoringStatusAndStart() {
		allStarted = false
	}

	if !h.checkVMImportControllerStatusAndStart() {
		allStarted = false
	}

	return allStarted
}

func (h *Handler) checkRancherMonitoringStatusAndStop() bool {
	// check managedchart fleet-local/rancher-monitoring paused
	monitoring, err := h.managedChartCache.Get(FleetLocalNamespace, RancherMonitoring)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("rancher monitoring not found. skip")
			return true
		}
		logrus.Warnf("rancher monitoring get error %v", err)
		return false
	}

	logrus.Infof("Rancher Monitoring: %v", monitoring.Spec.Paused)
	// check pause or not
	if !monitoring.Spec.Paused {
		logrus.Infof("stop rancher monitoring")
		monitoringCopy := monitoring.DeepCopy()
		monitoringCopy.Annotations[PausedStorageNetworkAnnotation] = "false"
		monitoringCopy.Spec.Paused = true

		if _, err := h.managedCharts.Update(monitoringCopy); err != nil {
			logrus.Warnf("rancher monitoring error %v", err)
			return false
		}
		return false
	}

	return true
}

func (h *Handler) checkPrometheusStatusAndStop() bool {
	// check prometheus cattle-monitoring-system/rancher-monitoring-prometheus replica
	prometheus, err := h.prometheusCache.Get(CattleMonitoringSystemNamespace, RancherMonitoringPrometheus)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("prometheus not found. skip")
			return true
		}
		logrus.Warnf("prometheus get error %v", err)
		return false
	}
	logrus.Infof("prometheus: %v", *prometheus.Spec.Replicas)
	// check stopped or not
	if *prometheus.Spec.Replicas != 0 {
		logrus.Infof("stop prometheus")
		prometheusCopy := prometheus.DeepCopy()
		prometheusCopy.Annotations[ReplicaStorageNetworkAnnotation] = strconv.Itoa(int(*prometheus.Spec.Replicas))
		*prometheusCopy.Spec.Replicas = 0

		if _, err := h.prometheus.Update(prometheusCopy); err != nil {
			logrus.Warnf("prometheus update error %v", err)
			return false
		}
		return false
	}

	return true
}

func (h *Handler) checkAltermanagerStatusAndStop() bool {
	// check alertmanager cattle-monitoring-system/rancher-monitoring-alertmanager replica
	alertmanager, err := h.alertmanagerCache.Get(CattleMonitoringSystemNamespace, RancherMonitoringAlertmanager)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("Alertmanager not found. skip")
			return true
		}
		logrus.Warnf("alertmanager get error %v", err)
		return false
	}

	logrus.Infof("alertmanager: %v", *alertmanager.Spec.Replicas)
	// check stopped or not
	if *alertmanager.Spec.Replicas != 0 {
		logrus.Infof("stop alertmanager")
		alertmanagerCopy := alertmanager.DeepCopy()
		alertmanagerCopy.Annotations[ReplicaStorageNetworkAnnotation] = strconv.Itoa(int(*alertmanager.Spec.Replicas))
		*alertmanagerCopy.Spec.Replicas = 0

		if _, err := h.alertmanager.Update(alertmanagerCopy); err != nil {
			logrus.Warnf("alertmanager update error %v", err)
			return false
		}
		return false
	}

	return true
}

func (h *Handler) checkGrafanaStatusAndStop() bool {
	// check deployment cattle-monitoring-system/rancher-monitoring-grafana replica
	grafana, err := h.deploymentCache.Get(CattleMonitoringSystemNamespace, RancherMonitoringGrafana)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("grafana no found. skip")
			return true
		}
		logrus.Warnf("grafana get error %v", err)
		return false
	}

	logrus.Infof("Grafana: %v", *grafana.Spec.Replicas)
	// check stopped or not
	if *grafana.Spec.Replicas != 0 {
		logrus.Infof("stop grafana")
		grafanaCopy := grafana.DeepCopy()
		grafanaCopy.Annotations[ReplicaStorageNetworkAnnotation] = strconv.Itoa(int(*grafana.Spec.Replicas))
		*grafanaCopy.Spec.Replicas = 0

		if _, err := h.deployments.Update(grafanaCopy); err != nil {
			logrus.Warnf("Grafana update error %v", err)
			return false
		}
		return false
	}

	return true
}

func (h *Handler) checkVMImportControllerStatusAndStop() bool {
	// check deployment harvester-system/harvester-harvester-vm-import-controller replica
	vmimportcontroller, err := h.deploymentCache.Get(HarvesterSystemNamespace, HarvesterVMImportController)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.Infof("VM import controller no found. skip")
			return true
		}
		logrus.Warnf("vmimportcontroller get error %v", err)
		return false
	}

	logrus.Infof("VM Import Controller: %v", *vmimportcontroller.Spec.Replicas)
	// check stopped or not
	if *vmimportcontroller.Spec.Replicas != 0 {
		logrus.Infof("stop vmi import controller")
		vmimportcontrollerCopy := vmimportcontroller.DeepCopy()
		vmimportcontrollerCopy.Annotations[ReplicaStorageNetworkAnnotation] = strconv.Itoa(int(*vmimportcontroller.Spec.Replicas))
		*vmimportcontrollerCopy.Spec.Replicas = 0

		if _, err := h.deployments.Update(vmimportcontrollerCopy); err != nil {
			logrus.Warnf("VM Import Controller update error %v", err)
			return false
		}
		return false
	}

	return true
}

// check Pod status, if all pods are stopped, return true
func (h *Handler) checkPodStatusAndStop() bool {
	allStopped := true

	if !h.checkRancherMonitoringStatusAndStop() {
		allStopped = false
	}

	if !h.checkPrometheusStatusAndStop() {
		allStopped = false
	}

	if !h.checkAltermanagerStatusAndStop() {
		allStopped = false
	}

	if !h.checkGrafanaStatusAndStop() {
		allStopped = false
	}

	if !h.checkVMImportControllerStatusAndStop() {
		allStopped = false
	}

	return allStopped
}

func (h *Handler) getLonghornStorageNetwork() (string, error) {
	storage, err := h.longhornSettingCache.Get(util.LonghornSystemNamespaceName, longhornStorageNetworkName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}
	return storage.Value, nil
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
