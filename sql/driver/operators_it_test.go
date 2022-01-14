/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package driver_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/serialization"
	"github.com/hazelcast/hazelcast-go-client/sql/driver"
)

type employee struct {
	PersonnelInfo `json:"personnel_info"`
	CompanyInfo   `json:"company_info"`
	ID            int `json:"id"`
}
type PersonnelInfo struct {
	Name string `json:"name"`
	Age  int    `json:"age"`
}
type CompanyInfo struct {
	Title   string `json:"title"`
	DepCode int    `json:"dep_code"`
}

func TestNestedJSONQuery(t *testing.T) {
	it.SkipIf(t, "hz < 5.1")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		defer driver.SetSerializationConfig(nil)
		const rowCount = 50
		db := initDB(t, config)
		defer db.Close()
		ms := createMappingStr(mapName, "bigint", "json")
		it.Must(createMapping(t, db, ms))
		// Insert random employee entries.
		employees, err := populateMapWithEmployees(m, rowCount)
		it.Must(err)
		// Test query on nested json
		complexQuery := fmt.Sprintf(`SELECT JSON_VALUE(this, '$.personnel_info.name') FROM "%s" WHERE CAST(JSON_VALUE(this, '$.id') AS TINYINT)=0`, mapName)
		row := db.QueryRowContext(context.Background(), complexQuery)
		var name string
		it.Must(row.Scan(&name))
		assert.Equal(t, employees[0].PersonnelInfo.Name, name)
	})
}

func TestJSONOperators(t *testing.T) {
	it.SkipIf(t, "hz < 5.1")
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		const rowCount = 50
		defer driver.SetSerializationConfig(nil)
		db := initDB(t, config)
		defer db.Close()
		ms := createMappingStr(mapName, "bigint", "json")
		it.Must(createMapping(t, db, ms))
		// Create a view to test.
		createViewQuery := fmt.Sprintf(`CREATE VIEW Personnel AS SELECT CAST(JSON_VALUE(this, '$.id') AS int) as pid, JSON_QUERY(this, '$.personnel_info') as info FROM "%s"`, mapName)
		it.MustValue(db.Exec(createViewQuery))
		// Insert random employee entries.
		employees, err := populateMapWithEmployees(m, rowCount)
		it.Must(err)
		ctx := context.Background()
		rows := mustRows(db.QueryContext(ctx, `SELECT pid, info FROM "Personnel" ORDER BY pid ASC`))
		defer rows.Close()
		for i := 0; rows.Next(); i++ {
			var pid int
			var data serialization.JSON
			it.Must(rows.Scan(&pid, &data))
			assert.Equal(t, employees[i].ID, pid)
			info, ok := it.MustValue(json.Marshal(employees[i].PersonnelInfo)).([]byte)
			if !ok {
				t.Fatal("scanned value is not of the expected type")
			}
			assert.Equal(t, serialization.JSON(info), data)
		}
		// Clean up for next test (smart, non-smart)
		it.MustValue(db.Exec(`DROP VIEW Personnel`))
		it.MustValue(db.Exec(fmt.Sprintf(`DROP MAPPING "%s"`, mapName)))
	})
}

func initDB(t *testing.T, config *hz.Config) *sql.DB {
	dsn := makeDSN(config)
	sc := &serialization.Config{}
	sc.SetGlobalSerializer(&it.PanicingGlobalSerializer{})
	it.Must(driver.SetSerializationConfig(sc))
	db := mustDB(sql.Open("hazelcast", dsn))
	return db
}

func populateMapWithEmployees(m *hz.Map, rowCount int) ([]employee, error) {
	keyFn := func(i int) interface{} {
		return i
	}
	titles := []string{"SWE", "PM", "Manager"}
	var employees []employee
	valueFn := func(i int) interface{} {
		e := employee{
			ID: i,
			PersonnelInfo: PersonnelInfo{
				Name: fmt.Sprintf("emp-%d", i),
				Age:  18 + i,
			},
			CompanyInfo: CompanyInfo{
				// Assume there are 3 departments
				DepCode: i % 3,
				Title:   titles[i%len(titles)],
			},
		}
		b, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("error encountered on json.Marshall: %w", err)
		}
		employees = append(employees, e)
		return serialization.JSON(b)
	}
	_, err := populateMap(m, rowCount, keyFn, valueFn)
	return employees, err
}
