// Copyright 2021 The Kube-burner Authors.
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

package util

import (
	"bytes"
	"fmt"
	"net/netip"
	"os"
	"strings"
	"text/template"

	sprig "github.com/Masterminds/sprig/v3"
	log "github.com/sirupsen/logrus"
	"gonum.org/v1/gonum/stat/combin"
)

type templateOption string

const (
	MissingKeyError templateOption = "missingkey=error"
	MissingKeyZero  templateOption = "missingkey=zero"
)

var funcMap = sprig.GenericFuncMap()

func init() {
	AddRenderingFunction("Binomial", combin.Binomial)
	AddRenderingFunction("IndexToCombination", combin.IndexToCombination)
	funcMap["GetSubnet24"] = func(subnetIdx int) string { // TODO Document this function
		return netip.AddrFrom4([4]byte{byte(subnetIdx>>16 + 1), byte(subnetIdx >> 8), byte(subnetIdx), 0}).String() + "/24"
	}
	// This function returns number of addresses requested per iteration from the list of total provided addresses
	funcMap["GetIPAddress"] = func(Addresses string, iteration int, addressesPerIteration int) string { // TODO Move this function to kube-burner-ocp
		var retAddrs []string
		addrSlice := strings.Split(Addresses, " ")
		for i := 0; i < addressesPerIteration; i++ {
			// For example, if iteration=6 and addressesPerIteration=2, return 12th address from list.
			// All addresses till 12th address were used in previous job iterations
			retAddrs = append(retAddrs, addrSlice[(iteration*addressesPerIteration)+i])
		}
		return strings.Join(retAddrs, " ")
	}
}

func AddRenderingFunction(name string, function any) {
	log.Debugf("Importing template function: %s", name)
	funcMap[name] = function
}

// RenderTemplate renders a go-template
func RenderTemplate(original []byte, inputData interface{}, options templateOption) ([]byte, error) {
	var rendered bytes.Buffer
	t, err := template.New("").Option(string(options)).Funcs(funcMap).Parse(string(original))
	if err != nil {
		return nil, fmt.Errorf("parsing error: %s", err)
	}
	err = t.Execute(&rendered, inputData)
	if err != nil {
		return nil, fmt.Errorf("rendering error: %s", err)
	}
	log.Tracef("Rendered template: %s", rendered.String())
	return rendered.Bytes(), nil
}

// EnvToMap returns the host environment variables as a map
func EnvToMap() map[string]interface{} {
	envMap := make(map[string]interface{})
	for _, v := range os.Environ() {
		envVar := strings.SplitN(v, "=", 2)
		envMap[envVar[0]] = envVar[1]
	}
	return envMap
}

// CreateFile creates a new file and writes content into it
func CreateFile(fileName string, fileContent []byte) error {
	fd, err := os.Create(fileName)
	if err != nil {
		return err
	}
	defer fd.Close()
	fd.Write(fileContent)
	return nil
}
