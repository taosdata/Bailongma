package model

import (
	cmodel "github.com/open-falcon/falcon-plus/common/model"
	"github.com/open-falcon/falcon-plus/modules/api/app/controller/graph"
)

type HistoryRequest graph.APIQueryGraphDrawData

type HistoryResponse []*cmodel.GraphQueryResponse
