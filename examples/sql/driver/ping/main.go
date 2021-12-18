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

package main

import (
	"database/sql"
	"log"

	_ "github.com/hazelcast/hazelcast-go-client/sql/driver"
)

func main() {
	log.Println("Creating the database value.")
	db, err := sql.Open("hazelcast", "hz://?logger.level=info")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	log.Println("The database value was created. Note that the connection to Hazelcast is not established at this point.")
	log.Println("Sending a ping request to establish the connection...")
	if err := db.Ping(); err != nil {
		panic(err)
	}
	log.Println("Connection established, and the ping was successful. Time to exit.")
}
