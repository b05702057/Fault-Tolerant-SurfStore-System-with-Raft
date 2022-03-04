package surfstore

import (
	"fmt"
)

var ErrServerCrashed = fmt.Errorf("server is crashed")
var ErrNotLeader = fmt.Errorf("server is not the leader")
