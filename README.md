# Unofficial unsupported experimental Databricks Connect Go Client

This is the unsupported unofficial experimental Databricks Connect Go client. 
It is based on the Python client and is not officially supported and affilieated with and
by Databricks.

It works similar to the Python version in that it allows you to connect to Databricks
by connecting to a cluster or the serverless compute.

## Using the Databricks Connect Go Client

In your go project add the client to your go dependencies.

```shell
go get github.com/grundprinzip/unofficial-dbconnect-go/v2
```

Now you can use it directly from your code:

```go
package main

import (
	"context"
	"fmt"
	"github.com/apache/spark-connect-go/v35/spark/sql"
	"github.com/apache/spark-connect-go/v35/spark/sql/functions"
	"github.com/grundprinzip/unofficial-dbconnect-go/v2/dbconnect"
	"log"
)

func main() {
	fmt.Println("Running my workload from Databricks")
	ctx := context.Background()
	cb := dbconnect.NewDataBricksChannelBuilder()

	// We can use the config to leverage the unified auth provided
	// by all Databricks SDKs.
	// cfg := config.Config{Profile: "DEFAULT"}
	// cb = cb.WithConfig(&cfg)

	// We can actually configure the session using different things.
	// Configure to use serverless...
	// cb = cb.UseServerless()

	// We can set a cluster ID
	// cb.UseCluster("2024-05-06-123456-123456")

	// But by default we just use magic...
	spark, err := sql.NewSessionBuilder().WithChannelBuilder(cb).Build(ctx)
	if err != nil {
		fmt.Printf("Failed: %s", err)
	}

	df, err := spark.Table("samples.nyctaxi.trips")
	if err != nil {
		log.Fatal(err)
	}

	df, err = df.GroupBy(
		functions.Round(functions.Col("fare_amount"), 0)).Sum(ctx, "fare_amount")
	if err != nil {
		log.Fatal(err)
	}

	err = df.Show(ctx, 100, false)
	if err != nil {
		log.Fatalf("Failed: %s", err)
	}
	fmt.Printf("Done...")
}

```