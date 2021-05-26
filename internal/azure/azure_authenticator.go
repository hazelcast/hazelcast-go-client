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
	"errors"
	"fmt"

	"github.com/hazelcast/hazelcast-go-client/internal/http"
	"github.com/hazelcast/hazelcast-go-client/internal/logger"
)

const (
	grantType    = "client_credentials"
	authEndpoint = "https://login.microsoftonline.com"
)

type Authenticator struct {
	endpoint   string
	httpClient *http.Client
	logger     logger.Logger
}

func NewAuthenticator(client *http.Client, logger logger.Logger) *Authenticator {
	return NewAuthenticatorWithEndpoint(client, logger, authEndpoint)
}

func NewAuthenticatorWithEndpoint(client *http.Client, logger logger.Logger, endpoint string) *Authenticator {
	return &Authenticator{
		endpoint:   endpoint,
		httpClient: client,
		logger:     logger,
	}
}

func (a *Authenticator) RefreshAccessToken(ctx context.Context, tenantID, clientID, clientSecret string) (string, error) {
	if tenantID == "" {
		return "", errors.New("tenant ID is missing")
	}
	if clientID == "" {
		return "", errors.New("client ID is missing")
	}
	if clientSecret == "" {
		return "", errors.New("client secret is missing")
	}
	url := a.makeURL(tenantID, clientID, clientSecret)
	a.logger.Trace(func() string { return fmt.Sprintf("refreshing access token: %s", url) })
	if j, err := getJSON(ctx, a.httpClient, url); err != nil {
		return "", fmt.Errorf("error refreshing access token: %w", err)
	} else {
		return jsonStringValue(j, "access_token"), nil
	}
}

func (a *Authenticator) makeURL(tenantID, clientID, clientSecret string) string {
	qr := fmt.Sprintf("grant_type=%s&resource=%s&client_id=%s&client_secret=%s", grantType, apiEndpoint, clientID, clientSecret)
	return fmt.Sprintf("%s/%s/oauth2/token?%s", a.endpoint, tenantID, qr)
}
