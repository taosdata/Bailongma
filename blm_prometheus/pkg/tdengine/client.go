/*
 * Copyright (c) 2021 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package tdengine

import (
	"bailongma/v2/blm_prometheus/pkg/tdengine/read"
	"bailongma/v2/blm_prometheus/pkg/tdengine/write"
	"github.com/prometheus/prometheus/prompb"
	"net/http"
)

// Config for the database
type Config struct {
	Table         string
	DaemonIP      string
	DaemonName    string
	DbName        string
	DbUser        string
	DbPassword    string
	DriverName    string
	TagNumLimit   int
	BatchSize     int
	BufferSize    int
	RWPort        string
	ApiPort       string
	DebugPrt      int
	TagLen        int
	TagLimit      int
	TablePervNode int
}

type Client struct {
}

type processor interface {
	Process()
}

type ReaderProcessor struct {
}

func NewClient() *Client {
	client := &Client{}
	return client
}

func (c *Client) Write(req *prompb.WriteRequest) error {
	writerProcessor := write.NewProcessor()
	return writerProcessor.Process(req)
}

func (c *Client) Check(r *http.Request) (string, error) {
	writerProcessor := write.NewProcessor()
	return writerProcessor.Check(r)
}

func (c *Client) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {
	readerProcessor := read.NewProcessor()
	return readerProcessor.Process(req)
}

// Name identifies the client as a TDengine client.
func (c Client) Name() string {
	return "TDengine"
}
