package dbconnect

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"github.com/apache/spark-connect-go/v35/spark/client/channel"
	"github.com/apache/spark-connect-go/v35/spark/sparkerrors"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/credentials/local"
	url2 "net/url"
	"os"
	"strings"
)

const (
	CONNECTION_TYPE_UNSPECIFIED = iota
	CONNECTION_TYPE_LOCAL       = iota
	CONNECTION_TYPE_CLUSTER     = iota
	CONNECTION_TYPE_SERVERLESS  = iota
)

var ErrFallbackConfigNotDetected = errors.New("failed to detect serverless configuration")

// DatabricksChannelBuilder is a builder that is used to create a GRPC connection to Databricks.
// It allows to connect to clusters and serverless depending on the configuration.
// To authenticate the channel builder relies on the unified auth of the SDK for Go.
type DatabricksChannelBuilder struct {
	channel.Builder
	headers        map[string]string
	config         *config.Config
	sessionId      string
	connectionType int
}

func (cb *DatabricksChannelBuilder) UseServerless() *DatabricksChannelBuilder {
	cb.sessionId = uuid.NewString()
	cb.config.ServerlessComputeID = "auto"
	cb.connectionType = CONNECTION_TYPE_SERVERLESS
	return cb
}

func (cb *DatabricksChannelBuilder) UseCluster(clusterId string) *DatabricksChannelBuilder {
	cb.config.ClusterID = clusterId
	cb.connectionType = CONNECTION_TYPE_CLUSTER
	return cb
}

func (cb *DatabricksChannelBuilder) WithConfig(config *config.Config) *DatabricksChannelBuilder {
	cb.config = config
	return cb
}

func (cb *DatabricksChannelBuilder) Profile(name string) *DatabricksChannelBuilder {
	cb.config.Profile = name
	return cb
}

func (cb *DatabricksChannelBuilder) Headers() map[string]string {
	return cb.headers
}

func (cb *DatabricksChannelBuilder) buildServerlessNotebookOrJob() (*grpc.ClientConn, error) {
	// Extract potential serverless interactive and jobs variables
	mtlsPort := os.Getenv("DATABRICKS_SERVERLESS_ADD_PORT")
	token := os.Getenv("DATABRICKS_API_TOKEN")
	// Metering session ID.
	meteringId := os.Getenv("DATABRICKS_SERVERLESS_CLUSTER_ID")
	sessionId := os.Getenv("DATABRICKS_SERVERLESS_SESSION_ID")
	affinityKey := os.Getenv("DATABRICKS_SERVERLESS_AFFINITY")

	// In serverless notebooks only the session ID is set, but not the affinity key
	if mtlsPort != "" && token != "" && sessionId != "" {
		var opts []grpc.DialOption
		// Initialize serverless notebooks
		cb.headers["x-databricks-session-id"] = sessionId

		if meteringId != "" && affinityKey != "" {
			// Initialize serverless notebooks
			cb.headers["x-databricks-spark-affinity-key"] = affinityKey
			cb.headers["x-databricks-metering-session-id"] = meteringId
		}

		// Setup authentication through mTLS.
		opts = append(opts, grpc.WithAuthority("localhost"))
		opts = append(opts, grpc.WithTransportCredentials(local.NewCredentials()))
		opts = append(opts, grpc.WithPerRPCCredentials(customTokenSource{token: token}))
		remote := fmt.Sprintf("localhost:%s", mtlsPort)
		conn, err := grpc.NewClient(remote, opts...)
		if err != nil {
			return nil, WithType(
				fmt.Errorf("failed to connect to remote %s: %w", remote, err),
				ConnectionError)
		}
		return conn, nil
	} else {
		return nil, ErrFallbackConfigNotDetected
	}
}

func (cb *DatabricksChannelBuilder) buildLocalRemote() (*grpc.ClientConn, error) {
	// We have to connect to the unix domain socket as identified by the SPARK_REMOTE environment variable.
	remote := os.Getenv("SPARK_REMOTE")
	if remote == "" {
		return nil, WithType(errors.New("SPARK_REMOTE not set"), InvalidConfigurationError)
	}

	u, err := url2.Parse(os.Getenv("SPARK_REMOTE"))
	if err != nil {
		return nil, WithType(InvalidConfigurationError, err)
	}
	parts := strings.Split(u.Path, ";")

	// Extract the actual socket path from the URL.
	socket_path := parts[0]
	for _, part := range parts {
		if strings.HasPrefix(part, "session_id=") {
			cb.sessionId = strings.TrimPrefix(part, "session_id=")
		}
	}

	// Create a grpc connection to the unix domain host.
	// Create gRPC dial options
	dialOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	// Dial the Unix domain socket
	return grpc.NewClient(fmt.Sprintf("unix://%s", socket_path), dialOptions...)
}

func (cb *DatabricksChannelBuilder) Build(ctx context.Context) (*grpc.ClientConn, error) {
	// Check that no conflicting options are set and we have a proper setup. Check the existence
	// of the session ID and cluster ID headers.
	hasSessionId := cb.config.ServerlessComputeID == "auto"
	hasClusterId := len(cb.config.ClusterID) > 0
	if hasSessionId && hasClusterId {
		return nil, WithType(InvalidConfigurationError, errors.New("only one of x-databricks-session-id or x-databricks-cluster-id must be present"))
	}

	var opts []grpc.DialOption
	remote := ""

	// If neither is present we're going to infer the behavior from the environment and try
	// to resolve serverless first. However, this will check for variables that are only present
	// for serverless notebooks and jobs. If they are not present, we will fall back to the
	// cluster configuration.
	if !hasSessionId && !hasClusterId {
		if conn, err := cb.buildServerlessNotebookOrJob(); err == nil || !errors.Is(err, ErrFallbackConfigNotDetected) {
			return conn, nil
		}
	}

	// On shared clusters, the local remote is passed as an environment variable.
	hasLocalRemote := strings.HasPrefix(os.Getenv("SPARK_REMOTE"), "unix://")
	// On Single User clusters, the location of the socket must be inferred, but we can check
	// if we run on a cluster using a different environment variable.
	isSingleUserCluster := len(os.Getenv("DATABRICKS_RUNTIME_VERSION")) > 0
	if cb.connectionType == CONNECTION_TYPE_UNSPECIFIED && (hasLocalRemote || isSingleUserCluster) {
		cb.connectionType = CONNECTION_TYPE_LOCAL
		if isSingleUserCluster {
			os.Setenv("SPARK_REMOTE", "unix:///databricks/sparkconnect/grpc.sock")
		}
		return cb.buildLocalRemote()
	}

	// If the connection has not been configured for serverless, we can try to talk directly
	// to the cluster using the unix domain socket.
	if cb.connectionType == CONNECTION_TYPE_SERVERLESS {
		cb.headers["x-databricks-session-id"] = cb.sessionId
	} else if cb.connectionType == CONNECTION_TYPE_CLUSTER {
		cb.headers["x-databricks-cluster-id"] = cb.config.ClusterID
	} else if cb.connectionType == CONNECTION_TYPE_UNSPECIFIED {
		return nil, WithType(errors.New("Must specify either cluster ID or UseServerless()"), InvalidConfigurationError)
	}

	// Extract from profile
	if cb.config.EnsureResolved() != nil {
		return nil, WithType(InvalidConfigurationError, errors.New("failed to extract Databricks SDK config information"))
	}

	url, err := url2.Parse(cb.config.Host)
	if err != nil {
		return nil, WithType(InvalidConfigurationError, err)
	}
	opts = append(opts, grpc.WithAuthority(url.Hostname()))
	remote = fmt.Sprintf("%v:443", url.Hostname())

	// Append the TLS certs and the auth source via profile.
	// Note: On the Windows platform, use of x509.SystemCertPool() requires
	// go version 1.18 or higher.
	systemRoots, err := x509.SystemCertPool()
	if err != nil {
		return nil, err
	}
	cred := credentials.NewTLS(&tls.Config{
		RootCAs: systemRoots,
	})
	opts = append(opts, grpc.WithTransportCredentials(cred))
	opts = append(opts, grpc.WithPerRPCCredentials(newUnifiedAuthCredentials(cb.config)))

	conn, err := grpc.NewClient(remote, opts...)
	if err != nil {
		return nil, sparkerrors.WithType(fmt.Errorf("failed to connect to remote %s: %w", remote, err), sparkerrors.ConnectionError)
	}
	return conn, nil
}

func (cb *DatabricksChannelBuilder) WithHeader(key, value string) *DatabricksChannelBuilder {
	cb.headers[key] = value
	return cb
}

func NewDataBricksChannelBuilder() *DatabricksChannelBuilder {
	return &DatabricksChannelBuilder{
		headers:        make(map[string]string),
		config:         &config.Config{},
		connectionType: CONNECTION_TYPE_UNSPECIFIED,
	}
}
