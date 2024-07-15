package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	metricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	pb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/schollz/progressbar/v3"
	"github.com/spf13/cobra"
)

func sendRequest(ctx context.Context, clientAddr string, metricData []*pb.ResourceMetrics, bar *progressbar.ProgressBar, wg *sync.WaitGroup, mu *sync.Mutex) {
	defer wg.Done()

	cli, err := grpc.NewClient(clientAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	conn := metricpb.NewMetricsServiceClient(cli)

	defer cli.Close()

	count := 0
	for _, j := range metricData {
		for _, v := range j.GetScopeMetrics() {
			for _, vv := range v.Metrics {
				count += len(vv.GetGauge().DataPoints)
			}
		}
	}

	_, err = conn.Export(ctx, &metricpb.ExportMetricsServiceRequest{
		ResourceMetrics: metricData,
	})
	if err != nil {
		log.Fatalf("Failed to send metrics: %v", err)
	}

	mu.Lock()
	err = bar.Add(count)
	mu.Unlock()
	if err != nil {
		log.Fatalf("Failed to update progress bar: %v", err)
	}
}

func main() {
	var fileName string
	var targetHost string
	var targetPort int
	var keyValues []string
	var threadCount int

	rootCmd := &cobra.Command{
		Use:   "metricSender",
		Short: "Sends OpenTelemetry metrics to a specified host and port",
		Run: func(cmd *cobra.Command, args []string) {
			clientAddr := fmt.Sprintf("%s:%d", targetHost, targetPort)
			preTime := time.Now()
			data, err := os.ReadFile(fileName)
			if err != nil {
				log.Fatalf("Failed to read file: %v", err)
			}
			var metricData pb.MetricsData
			if err := proto.Unmarshal(data, &metricData); err != nil {
				log.Fatalf("Failed to unmarshal data: %v", err)
			}
			log.Printf("Read and unmarshal used %v", time.Since(preTime))

			totalDataPoints := 0
			for _, resource := range metricData.ResourceMetrics {
				for _, v := range resource.ScopeMetrics {
					for _, vv := range v.Metrics {
						totalDataPoints += len(vv.GetGauge().DataPoints)
					}
				}
			}

			bar := progressbar.NewOptions(totalDataPoints,
				progressbar.OptionSetDescription("Sending data points"),
				progressbar.OptionShowCount(),
				progressbar.OptionSetWidth(15),
				progressbar.OptionShowDescriptionAtLineEnd(),
			)

			var wg sync.WaitGroup
			var mu sync.Mutex
			sem := make(chan struct{}, threadCount)

			resourceGroup := []*pb.ResourceMetrics{}
			for idx, resource := range metricData.ResourceMetrics {
				resourceGroup = append(resourceGroup, resource)
				if (idx+1)%100 == 0 || idx == len(metricData.ResourceMetrics)-1 {
					sem <- struct{}{}
					wg.Add(1)
					go func(resourceGroup []*pb.ResourceMetrics) {
						defer func() { <-sem }()
						sendRequest(context.Background(), clientAddr, resourceGroup, bar, &wg, &mu)
						time.Sleep(100 * time.Millisecond)
					}(resourceGroup)
					resourceGroup = []*pb.ResourceMetrics{}
				}
			}
			wg.Wait()
			fmt.Println("\nSent all data points")
		},
	}

	rootCmd.Flags().StringVarP(&fileName, "file", "f", "", "The file name to read metrics from")
	rootCmd.Flags().StringVarP(&targetHost, "host", "s", "", "The target host to send metrics to")
	rootCmd.Flags().IntVarP(&targetPort, "port", "p", 4317, "The target port to send metrics to")
	rootCmd.Flags().StringSliceVarP(&keyValues, "keyValue", "k", []string{}, "The key-value pairs to add to metrics (format: key=value)")
	rootCmd.Flags().IntVarP(&threadCount, "threads", "t", 10, "The number of threads to use for sending metrics")

	err := rootCmd.MarkFlagRequired("file")
	if err != nil {
		panic(err)
	}
	err = rootCmd.MarkFlagRequired("host")
	if err != nil {
		panic(err)
	}
	err = rootCmd.MarkFlagRequired("keyValue")
	if err != nil {
		panic(err)
	}

	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error executing command: %v", err)
	}
}
