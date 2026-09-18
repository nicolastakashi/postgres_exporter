// Copyright 2021 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package exporter

import (
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

func (e *Exporter) discoverDatabaseDSNs() []string {
	// connstring syntax is complex (and not sure if even regular).
	// we don't need to parse it, so just superficially validate that it starts
	// with a valid-ish keyword pair
	connstringRe := regexp.MustCompile(`^ *[a-zA-Z0-9]+ *= *[^= ]+`)

	dsns := make(map[string]struct{})
	for _, dsn := range e.dsn {
		var dsnURI *url.URL
		var dsnConnstring string

		if strings.HasPrefix(dsn, "postgresql://") || strings.HasPrefix(dsn, "postgres://") {
			var err error
			dsnURI, err = url.Parse(dsn)
			if err != nil {
				e.logger.Error("Unable to parse DSN as URI", "dsn", loggableDSN(dsn), "err", err)
				continue
			}
		} else if connstringRe.MatchString(dsn) {
			dsnConnstring = dsn
		} else {
			e.logger.Error("Unable to parse DSN as either URI or connstring", "dsn", loggableDSN(dsn))
			continue
		}

		server, err := e.servers.GetServer(dsn)
		if err != nil {
			e.logger.Error("Error opening connection to database", "dsn", loggableDSN(dsn), "err", err)
			continue
		}
		dsns[dsn] = struct{}{}

		// If autoDiscoverDatabases is true, set first dsn as master database (Default: false)
		server.master = true

		databaseNames, err := queryDatabases(server)
		if err != nil {
			e.logger.Error("Error querying databases", "dsn", loggableDSN(dsn), "err", err)
			continue
		}
		for _, databaseName := range databaseNames {
			if slices.Contains(e.excludeDatabases, databaseName) {
				continue
			}

			if len(e.includeDatabases) != 0 && !slices.Contains(e.includeDatabases, databaseName) {
				continue
			}

			if dsnURI != nil {
				dsnURI.Path = databaseName
				dsn = dsnURI.String()
			} else {
				// replacing one dbname with another is complicated.
				// just append new dbname to override.
				dsn = fmt.Sprintf("%s dbname=%s", dsnConnstring, databaseName)
			}
			dsns[dsn] = struct{}{}
		}
	}

	result := make([]string, len(dsns))
	index := 0
	for dsn := range dsns {
		result[index] = dsn
		index++
	}

	return result
}

func (e *Exporter) scrapeDSN(ch chan<- prometheus.Metric, dsn string) error {
	server, err := e.servers.GetServer(dsn)

	if err != nil {
		return &ErrorConnectToServer{fmt.Sprintf("Error opening connection to database (%s): %s", loggableDSN(dsn), err.Error())}
	}

	// Check if autoDiscoverDatabases is false, set dsn as master database (Default: false)
	if !e.autoDiscoverDatabases {
		server.master = true
	}

	// Check if map versions need to be updated
	if err := e.checkMapVersions(ch, server); err != nil {
		e.logger.Warn("Proceeding with outdated query maps, as the Postgres version could not be determined", "err", err)
	}

	return server.Scrape(ch)
}

// DataSourceOpts holds the values of the --datasource.* flags. An empty
// field falls back to the corresponding DATA_SOURCE_* environment variable.
//
// The user and password themselves are only settable via the
// DATA_SOURCE_USER/DATA_SOURCE_PASS environment variables (or the *_FILE
// variants / the file flags below), never via a plain CLI flag, since those
// are easy to leak (e.g. visible in `ps` output or a Kubernetes pod spec).
// GetDataSources reads them straight from the environment below, so no
// corresponding fields exist on this struct.
type DataSourceOpts struct {
	URI      string
	URIFile  string
	UserFile string
	PassFile string
}

func flagOrEnvVar(flagVal, envKey string) string {
	if flagVal != "" {
		return flagVal
	}
	return os.Getenv(envKey)
}

// readTrimmedFile reads path and returns its contents with surrounding
// whitespace removed, wrapping any read error with what the file was for.
func readTrimmedFile(what, path string) (string, error) {
	fileContents, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("failed loading data source %s file %s: %s", what, path, err.Error())
	}
	return strings.TrimSpace(string(fileContents)), nil
}

// try to get the DataSource
// The DATA_SOURCE_NAME env var always wins so we do not break older versions
// reading secrets from files wins over secrets passed directly
// dsn > {user|pass}-file > {user|pass}
func GetDataSources(opts DataSourceOpts) ([]string, error) {
	dsn := os.Getenv("DATA_SOURCE_NAME")
	if len(dsn) != 0 {
		return strings.Split(dsn, ","), nil
	}

	var user, pass, uri string
	var err error

	if dataSourceUserFile := flagOrEnvVar(opts.UserFile, "DATA_SOURCE_USER_FILE"); dataSourceUserFile != "" {
		if user, err = readTrimmedFile("user", dataSourceUserFile); err != nil {
			return nil, err
		}
	} else {
		user = os.Getenv("DATA_SOURCE_USER")
	}

	if dataSourcePassFile := flagOrEnvVar(opts.PassFile, "DATA_SOURCE_PASS_FILE"); dataSourcePassFile != "" {
		if pass, err = readTrimmedFile("pass", dataSourcePassFile); err != nil {
			return nil, err
		}
	} else {
		pass = os.Getenv("DATA_SOURCE_PASS")
	}

	ui := url.UserPassword(user, pass).String()
	if dataSourceURIFile := flagOrEnvVar(opts.URIFile, "DATA_SOURCE_URI_FILE"); dataSourceURIFile != "" {
		if uri, err = readTrimmedFile("URI", dataSourceURIFile); err != nil {
			return nil, err
		}
	} else {
		uri = flagOrEnvVar(opts.URI, "DATA_SOURCE_URI")
	}

	// No datasources found. This allows us to support the multi-target pattern
	// without an explicit datasource.
	if uri == "" {
		return []string{}, nil
	}

	dsn = "postgresql://" + ui + "@" + uri

	return []string{dsn}, nil
}
