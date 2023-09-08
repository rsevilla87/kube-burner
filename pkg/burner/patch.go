// Copyright 2022 The Kube-burner Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package burner

import (
	"context"
	"strings"
	"sync"

	"github.com/kube-burner/kube-burner/v2/pkg/config"
	log "github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
)

func (ex *JobExecutor) setupPatchJob() {
	log.Debugf("Preparing patch job: %s", ex.Name)
	ex.itemHandler = patchHandler
	if len(ex.ExecutionMode) == 0 {
		ex.ExecutionMode = config.ExecutionModeParallel
	}
	if _, ok := supportedExecutionMode[ex.ExecutionMode]; !ok {
		log.Fatalf("Unsupported Execution Mode: %s", ex.ExecutionMode)
	}

	for _, o := range ex.Objects {
		if len(o.PatchType) == 0 {
			log.Fatalln("Empty Patch Type not allowed")
		}
		log.Infof("Job %s: %s %s with selector %s", ex.Name, ex.JobType, o.Kind, labels.Set(o.LabelSelector))
		ex.objects = append(ex.objects, newObject(o, ex.mapper, APIVersionV1, ex.embedCfg))
	}
}

<<<<<<< HEAD
func patchHandler(ctx context.Context, ex *JobExecutor, obj *object, originalItem unstructured.Unstructured, iteration int, objectTimeUTC int64, wg *sync.WaitGroup) {
=======
// RunPatchJob executes a patch job
func (ex *Executor) RunPatchJob() {
	var itemList *unstructured.UnstructuredList
	log.Infof("Running patch job %s", ex.Name)
	var wg sync.WaitGroup
	for _, obj := range ex.objects {

		labelSelector := labels.Set(obj.labelSelector).String()
		listOptions := metav1.ListOptions{
			LabelSelector: labelSelector,
		}

		// Try to find the list of resources by GroupVersionResource.
		err := RetryWithExponentialBackOff(func() (done bool, err error) {
			itemList, err = DynamicClient.Resource(obj.gvr).List(context.TODO(), listOptions)
			if err != nil {
				log.Errorf("Error found listing %s labeled with %s: %s", obj.gvr.Resource, labelSelector, err)
				return false, nil
			}
			return true, nil
		}, 1*time.Second, 3, 0, ex.MaxWaitTimeout)
		if err != nil {
			continue
		}
		log.Infof("Found %d %s with selector %s; patching them", len(itemList.Items), obj.gvr.Resource, labelSelector)
		for i := 0; i < ex.JobIterations; i++ {
			for _, item := range itemList.Items {
				wg.Add(1)
				go ex.patchHandler(obj, item, i, &wg)
			}
		}
	}
	wg.Wait()
}

func (ex *Executor) patchHandler(obj object, originalItem unstructured.Unstructured,
	iteration int, wg *sync.WaitGroup) {

>>>>>>> fafd04be (Iterations should start from zero)
	defer wg.Done()
	// There are several patch modes. Three of them are client-side, and one
	// of them is server-side.
	var data []byte
	patchOptions := metav1.PatchOptions{}

	if strings.HasSuffix(obj.ObjectTemplate, "json") {
		if obj.PatchType == string(types.ApplyPatchType) {
			log.Fatalf("Apply patch type requires YAML")
		}
		data = ex.renderTemplateForObject(obj, iteration, 0, false)
	} else {
		var asJson bool
		if obj.PatchType == string(types.ApplyPatchType) {
			patchOptions.FieldManager = "kube-controller-manager"
			asJson = false
		} else {
			asJson = true
		}
		data = ex.renderTemplateForObject(obj, iteration, 0, asJson)
	}

	ns := originalItem.GetNamespace()
	log.Debugf("Patching %s/%s in namespace %s", originalItem.GetKind(),
		originalItem.GetName(), ns)
	ex.limiter.Wait(ctx)

	var uns *unstructured.Unstructured
	var err error
	if obj.namespaced {
		uns, err = ex.dynamicClient.Resource(obj.gvr).Namespace(ns).
			Patch(ctx, originalItem.GetName(),
				types.PatchType(obj.PatchType), data, patchOptions)
	} else {
		uns, err = ex.dynamicClient.Resource(obj.gvr).
			Patch(ctx, originalItem.GetName(),
				types.PatchType(obj.PatchType), data, patchOptions)
	}
	if err != nil {
		if errors.IsForbidden(err) {
			log.Fatalf("Authorization error patching %s/%s: %s", originalItem.GetKind(), originalItem.GetName(), err)
		} else {
			log.Errorf("Error patching object %s/%s in namespace %s: %s", originalItem.GetKind(),
				originalItem.GetName(), ns, err)
		}
	} else {
		log.Debugf("Patched %s/%s in namespace %s", uns.GetKind(), uns.GetName(), ns)
	}
}
