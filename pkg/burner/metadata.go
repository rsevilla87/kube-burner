// Copyright 2020 The Kube-burner Authors.
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
	"time"

	"github.com/cloud-bulldozer/go-commons/indexers"
	"github.com/kube-burner/kube-burner/pkg/config"
	log "github.com/sirupsen/logrus"
)

type JobSummary struct {
	Timestamp           time.Time              `json:"timestamp"`
	EndTimestamp        time.Time              `json:"endTimestamp"`
	ChurnStartTimestamp *time.Time             `json:"churnStartTimestamp,omitempty"`
	ChurnEndTimestamp   *time.Time             `json:"churnEndTimestamp,omitempty"`
	ElapsedTime         float64                `json:"elapsedTime"`
	UUID                string                 `json:"uuid"`
	MetricName          string                 `json:"metricName"`
	JobConfig           config.Job             `json:"jobConfig"`
	Metadata            map[string]interface{} `json:"metadata,omitempty"`
	Version             string                 `json:"version"`
	Passed              bool                   `json:"passed"`
	ExecutionErrors     string                 `json:"executionErrors,omitempty"`
	ObservedQPS         float64                `json:"observedQPS"`
	RequestsSent        int                    `json:"requestsSent"`
}

const jobSummaryMetric = "jobSummary"

// indexMetadataInfo Generates and indexes a document with metadata information of the passed job
func IndexJobSummary(jobSummaries []JobSummary, indexer indexers.Indexer) {
	log.Info("Indexing job summaries")
	var jobSummariesInt []interface{}
	indexingOpts := indexers.IndexingOpts{
		MetricName: jobSummaryMetric,
	}
	for _, summary := range jobSummaries {
		jobSummariesInt = append(jobSummariesInt, summary)
	}
	resp, err := indexer.Index(jobSummariesInt, indexingOpts)
	if err != nil {
		log.Error(err)
	} else {
		log.Info(resp)
	}
}
