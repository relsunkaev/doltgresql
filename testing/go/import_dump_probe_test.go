// Copyright 2026 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package _go

import "testing"

// TestImportAlexTransitVenderctlProbe is a representative pg_dump restore gate
// for a schema that exercises dump cleanup preludes and hstore columns.
func TestImportAlexTransitVenderctlProbe(t *testing.T) {
	RunImportTests(t, []ImportTest{
		{
			Name:        "AlexTransit/venderctl",
			SQLFilename: "AlexTransit_venderctl.sql",
		},
	})
}

// TestImportExpandedRestoreGateProbe keeps a small external application dump
// corpus on the always-runnable restore path.
func TestImportExpandedRestoreGateProbe(t *testing.T) {
	RunImportTests(t, []ImportTest{
		{
			Name: "Boluwatife-AJB/backend-in-node",
			SetUpScript: []string{
				`CREATE USER "USER" WITH SUPERUSER PASSWORD 'password';`,
			},
			SQLFilename: "Boluwatife-AJB_backend-in-node.sql",
		},
		{
			Name: "linvivian7/fe-react-16-demo",
			SetUpScript: []string{
				`CREATE USER "Admin" WITH SUPERUSER PASSWORD 'password';`,
			},
			SQLFilename: "linvivian7_fe-react-16-demo.sql",
		},
		{
			Name:        "kirooha/adtech-simple",
			SQLFilename: "kirooha_adtech-simple.sql",
		},
		{
			Name: "bartr/agency",
			SetUpScript: []string{
				`CREATE USER root WITH SUPERUSER PASSWORD 'password';`,
			},
			SQLFilename: "bartr_agency.sql",
		},
	})
}
