/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"

	hz "github.com/hazelcast/hazelcast-go-client"
	"github.com/hazelcast/hazelcast-go-client/internal/it"
	"github.com/hazelcast/hazelcast-go-client/types"

	_ "github.com/hazelcast/hazelcast-go-client/sql/driver"
)

func TestSQLQuery(t *testing.T) {
	testCases := []struct {
		keyFn, valueFn   func(i int) interface{}
		keyFmt, valueFmt string
	}{
		{
			keyFmt: "int", valueFmt: "int",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return int32(i * 100) },
		},
		{
			keyFmt: "int", valueFmt: "varchar",
			keyFn:   func(i int) interface{} { return int32(i) },
			valueFn: func(i int) interface{} { return fmt.Sprintf("val-%d", i*100) },
		},
	}
	for _, tc := range testCases {
		name := fmt.Sprintf("%s/%s", tc.keyFmt, tc.valueFmt)
		t.Run(name, func(t *testing.T) {
			testSQLQuery(t, tc.keyFmt, tc.valueFmt, tc.keyFn, tc.valueFn)
		})
	}
}

func testSQLQuery(t *testing.T, keyFmt, valueFmt string, keyFn, valueFn func(i int) interface{}) {
	it.SQLTester(t, func(t *testing.T, client *hz.Client, config *hz.Config, m *hz.Map, mapName string) {
		const rowCount = 10
		target, err := populateMap(m, rowCount, keyFn, valueFn)
		if err != nil {
			t.Fatal(err)
		}
		dsn := makeDSN(config)
		db, err := sql.Open("hazelcast", dsn)
		if err != nil {
			t.Fatal(err)
		}
		ms := createMappingStr(mapName, keyFmt, valueFmt)
		if err := createMapping(t, db, ms); err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		query := fmt.Sprintf(`SELECT __key, this FROM "%s" ORDER BY __key`, mapName)
		rows, err := db.Query(query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		entries := make([]types.Entry, len(target))
		var i int
		for rows.Next() {
			if err := rows.Scan(&entries[i].Key, &entries[i].Value); err != nil {
				log.Fatal(err)
			}
			i++
		}
		assert.Equal(t, target, entries)
	})
}

func createMapping(t *testing.T, db *sql.DB, mapping string) error {
	t.Logf("mapping: %s", mapping)
	_, err := db.Exec(mapping)
	return err
}

func createMappingStr(mapName, keyFmt, valueFmt string) string {
	return fmt.Sprintf(`
        CREATE MAPPING "%s"
        TYPE IMAP 
        OPTIONS (
            'keyFormat' = '%s',
            'valueFormat' = '%s'
        )
`, mapName, keyFmt, valueFmt)
}

func populateMap(m *hz.Map, count int, keyFn, valueFn func(i int) interface{}) ([]types.Entry, error) {
	entries := make([]types.Entry, count)
	for i := 0; i < count; i++ {
		entries[i] = types.Entry{
			Key:   keyFn(i),
			Value: valueFn(i),
		}
	}
	if err := m.PutAll(context.Background(), entries...); err != nil {
		return nil, err
	}
	return entries, nil
}

func makeDSN(config *hz.Config) string {
	return fmt.Sprintf("%s;ClusterName=%s;Cluster.Unisocket=%t", config.Cluster.Network.Addresses[0], config.Cluster.Name, config.Cluster.Unisocket)
}
