package dbconnect

import (
	"context"
	"os"
	"testing"

	config2 "github.com/databricks/databricks-sdk-go/config"
	"github.com/stretchr/testify/assert"
)

func TestSdkConfig_OverlappingConfs(t *testing.T) {

	config := config2.Config{}
	config.Profile = "DEFAULT"
	config.ServerlessComputeID = "None"
	config.ClusterID = "aaaa-bbbb-cccc-dddd"

	err := config.EnsureResolved()
	assert.NoError(t, err)

}

func TestDatabricksChannelBuilder_Build(t *testing.T) {
	ctx := context.Background()
	os.Setenv("SPARK_REMOTE", "unix:///databricks/sparkconnect/grpc.sock;user_id=PLACEHOLDER;session_id=187872cd-f25a-40ca-947e-fcff1e65929b")
	cb := NewDataBricksChannelBuilder()
	con, err := cb.Build(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, con)
}

func TestDatabricksChannelBuilder_Build_With_Serverless(t *testing.T) {
	ctx := context.Background()
	cb := NewDataBricksChannelBuilder()
	cb = cb.UseServerless()
	cb = cb.Profile("logfood")
	con, err := cb.Build(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, con)
}
