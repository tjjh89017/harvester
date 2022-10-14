/*
Copyright 2022 Rancher Labs, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by main. DO NOT EDIT.

package fake

import (
	"context"

	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeServiceMonitors implements ServiceMonitorInterface
type FakeServiceMonitors struct {
	Fake *FakeMonitoringV1
	ns   string
}

var servicemonitorsResource = schema.GroupVersionResource{Group: "monitoring.coreos.com", Version: "v1", Resource: "servicemonitors"}

var servicemonitorsKind = schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor"}

// Get takes name of the serviceMonitor, and returns the corresponding serviceMonitor object, and an error if there is any.
func (c *FakeServiceMonitors) Get(ctx context.Context, name string, options v1.GetOptions) (result *monitoringv1.ServiceMonitor, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(servicemonitorsResource, c.ns, name), &monitoringv1.ServiceMonitor{})

	if obj == nil {
		return nil, err
	}
	return obj.(*monitoringv1.ServiceMonitor), err
}

// List takes label and field selectors, and returns the list of ServiceMonitors that match those selectors.
func (c *FakeServiceMonitors) List(ctx context.Context, opts v1.ListOptions) (result *monitoringv1.ServiceMonitorList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(servicemonitorsResource, servicemonitorsKind, c.ns, opts), &monitoringv1.ServiceMonitorList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &monitoringv1.ServiceMonitorList{ListMeta: obj.(*monitoringv1.ServiceMonitorList).ListMeta}
	for _, item := range obj.(*monitoringv1.ServiceMonitorList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested serviceMonitors.
func (c *FakeServiceMonitors) Watch(ctx context.Context, opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(servicemonitorsResource, c.ns, opts))

}

// Create takes the representation of a serviceMonitor and creates it.  Returns the server's representation of the serviceMonitor, and an error, if there is any.
func (c *FakeServiceMonitors) Create(ctx context.Context, serviceMonitor *monitoringv1.ServiceMonitor, opts v1.CreateOptions) (result *monitoringv1.ServiceMonitor, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(servicemonitorsResource, c.ns, serviceMonitor), &monitoringv1.ServiceMonitor{})

	if obj == nil {
		return nil, err
	}
	return obj.(*monitoringv1.ServiceMonitor), err
}

// Update takes the representation of a serviceMonitor and updates it. Returns the server's representation of the serviceMonitor, and an error, if there is any.
func (c *FakeServiceMonitors) Update(ctx context.Context, serviceMonitor *monitoringv1.ServiceMonitor, opts v1.UpdateOptions) (result *monitoringv1.ServiceMonitor, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(servicemonitorsResource, c.ns, serviceMonitor), &monitoringv1.ServiceMonitor{})

	if obj == nil {
		return nil, err
	}
	return obj.(*monitoringv1.ServiceMonitor), err
}

// Delete takes name of the serviceMonitor and deletes it. Returns an error if one occurs.
func (c *FakeServiceMonitors) Delete(ctx context.Context, name string, opts v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteActionWithOptions(servicemonitorsResource, c.ns, name, opts), &monitoringv1.ServiceMonitor{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeServiceMonitors) DeleteCollection(ctx context.Context, opts v1.DeleteOptions, listOpts v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(servicemonitorsResource, c.ns, listOpts)

	_, err := c.Fake.Invokes(action, &monitoringv1.ServiceMonitorList{})
	return err
}

// Patch applies the patch and returns the patched serviceMonitor.
func (c *FakeServiceMonitors) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts v1.PatchOptions, subresources ...string) (result *monitoringv1.ServiceMonitor, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(servicemonitorsResource, c.ns, name, pt, data, subresources...), &monitoringv1.ServiceMonitor{})

	if obj == nil {
		return nil, err
	}
	return obj.(*monitoringv1.ServiceMonitor), err
}
