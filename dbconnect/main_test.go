package dbconnect

import (
	"context"
	"os"
	"testing"

	config2 "github.com/databricks/databricks-sdk-go/config"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
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

func TestDatabricksChannelBuilder_WithDialOption(t *testing.T) {
	cb := NewDataBricksChannelBuilder()

	// Test initial state
	assert.Equal(t, 0, len(cb.opts), "Initial opts slice should be empty")

	// Test adding a single dial option
	mockOpt := grpc.WithBlock()
	cb = cb.WithDialOption(mockOpt)
	assert.Equal(t, 1, len(cb.opts), "Should have one dial option after adding")
	assert.Equal(t, mockOpt, cb.opts[0], "First option should match the added option")

	// Test adding multiple dial options
	mockOpt2 := grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`)
	cb = cb.WithDialOption(mockOpt2)
	assert.Equal(t, 2, len(cb.opts), "Should have two dial options after adding second")
	assert.Equal(t, mockOpt, cb.opts[0], "First option should remain unchanged")
	assert.Equal(t, mockOpt2, cb.opts[1], "Second option should match the added option")

	// Test chaining
	cb2 := NewDataBricksChannelBuilder()
	cb2 = cb2.WithDialOption(mockOpt).WithDialOption(mockOpt2)
	assert.Equal(t, 2, len(cb2.opts), "Chained calls should work correctly")
}

func TestDatabricksChannelBuilder_WithDialOption_Chaining(t *testing.T) {
	cb := NewDataBricksChannelBuilder()

	// Test that WithDialOption returns the builder for chaining
	result := cb.WithDialOption(grpc.WithBlock())
	assert.Equal(t, cb, result, "WithDialOption should return the builder for chaining")

	// Test multiple chained calls
	cb = cb.WithDialOption(grpc.WithBlock()).
		WithDialOption(grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`)).
		WithDialOption(grpc.WithUserAgent("test-agent"))

	assert.Equal(t, 3, len(cb.opts), "Should have three dial options after chained calls")
}
