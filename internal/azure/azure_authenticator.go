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

package azure

import (
	"context"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal/http"
)

type Authenticator struct {
	endpoint   string
	httpClient *http.Client
}

func NewAuthenticator(client *http.Client) *Authenticator {
	return NewAuthenticatorWithEndpoint(client, authEndpoint)
}

func NewAuthenticatorWithEndpoint(client *http.Client, endpoint string) *Authenticator {
	return &Authenticator{
		endpoint:   endpoint,
		httpClient: client,
	}
}

func (a *Authenticator) RefreshAccessToken(ctx context.Context, tenantID, clientID, clientSecret string) (string, error) {
	url := a.makeURL(tenantID, clientID, clientSecret)
	if j, err := getJSON(ctx, a.httpClient, url); err != nil {
		return "", fmt.Errorf("refreshing access token: %w", err)
	} else {
		return jsonStringValue(j, "access_token"), nil
	}
}

func (a *Authenticator) makeURL(tenantID, clientID, clientSecret string) string {
	qr := fmt.Sprintf("grant_type=%s&resource=%s&client_id=%s&client_secret=%s", grantType, apiEndpoint, clientID, clientSecret)
	return fmt.Sprintf("%s/%s/oauth2/token?%s", a.endpoint, tenantID, qr)
}
