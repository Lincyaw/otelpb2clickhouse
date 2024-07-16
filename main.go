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

const MaxNum = 4*1024*1024 - 1

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
func SplitMetricData(input *pb.MetricsData) [][]*pb.ResourceMetrics {
	var result [][]*pb.ResourceMetrics
	var currentChunk []*pb.ResourceMetrics
	currentSize := 0

	for _, resourceMetric := range input.ResourceMetrics {
		rmSize := proto.Size(resourceMetric)

		// 如果ResourceMetrics本身就超过MaxNum的大小
		if rmSize > MaxNum {
			// 处理ScopeMetrics分割
			splitResourceMetrics := splitScopeMetrics(resourceMetric)
			result = append(result, splitResourceMetrics)
			continue
		}

		// 如果添加当前的ResourceMetrics会超过MaxNum，开始一个新的chunk
		if currentSize+rmSize > MaxNum {
			if len(currentChunk) > 0 {
				result = append(result, currentChunk)
				currentChunk = nil
				currentSize = 0
			}
		}

		// 添加ResourceMetrics到当前的chunk
		currentChunk = append(currentChunk, resourceMetric)
		currentSize += rmSize
	}

	// 添加最后一个chunk如果它包含元素
	if len(currentChunk) > 0 {
		result = append(result, currentChunk)
	}

	return result
}

func splitScopeMetrics(resourceMetric *pb.ResourceMetrics) []*pb.ResourceMetrics {
	var splitResult []*pb.ResourceMetrics
	var currentScopeMetrics []*pb.ScopeMetrics
	currentSize := proto.Size(resourceMetric.Resource)

	for _, scopeMetric := range resourceMetric.ScopeMetrics {
		smSize := proto.Size(scopeMetric)

		// 如果 ScopeMetrics 本身就超过 MaxNum 的大小
		if smSize > MaxNum {
			// 处理 Metrics 分割
			splitScopeMetrics := splitMetrics(scopeMetric, resourceMetric)
			splitResult = append(splitResult, splitScopeMetrics...)
			continue
		}

		// 如果添加当前的 ScopeMetrics 会超过 MaxNum，开始一个新的 chunk
		if currentSize+smSize > MaxNum {
			if len(currentScopeMetrics) > 0 {
				splitResult = append(splitResult, &pb.ResourceMetrics{
					Resource:     resourceMetric.Resource,
					ScopeMetrics: currentScopeMetrics,
					SchemaUrl:    resourceMetric.SchemaUrl,
				})
				currentScopeMetrics = nil
				currentSize = proto.Size(resourceMetric.Resource)
			}
		}

		// 添加 ScopeMetrics 到当前的 chunk
		currentScopeMetrics = append(currentScopeMetrics, scopeMetric)
		currentSize += smSize
	}

	// 添加最后一个 chunk 如果它包含元素
	if len(currentScopeMetrics) > 0 {
		splitResult = append(splitResult, &pb.ResourceMetrics{
			Resource:     resourceMetric.Resource,
			ScopeMetrics: currentScopeMetrics,
			SchemaUrl:    resourceMetric.SchemaUrl,
		})
	}

	return splitResult
}

func splitMetrics(scopeMetric *pb.ScopeMetrics, resourceMetric *pb.ResourceMetrics) []*pb.ResourceMetrics {
	var splitResult []*pb.ResourceMetrics
	var currentMetrics []*pb.Metric
	currentSize := proto.Size(scopeMetric.Scope)

	for _, metric := range scopeMetric.Metrics {
		mSize := proto.Size(metric)

		// 如果 Metric 本身就超过 MaxNum 的大小，需要特殊处理（假设不会出现这种情况）
		if mSize > MaxNum {
			// 这里直接跳过，假设每个 Metric 都不会超过 MaxNum
			continue
		}

		// 如果添加当前的 Metric 会超过 MaxNum，开始一个新的 chunk
		if currentSize+mSize > MaxNum {
			if len(currentMetrics) > 0 {
				splitResult = append(splitResult, &pb.ResourceMetrics{
					Resource: resourceMetric.Resource,
					ScopeMetrics: []*pb.ScopeMetrics{
						{
							Scope:     scopeMetric.Scope,
							Metrics:   currentMetrics,
							SchemaUrl: scopeMetric.SchemaUrl,
						},
					},
					SchemaUrl: resourceMetric.SchemaUrl,
				})
				currentMetrics = nil
				currentSize = proto.Size(scopeMetric.Scope)
			}
		}

		// 添加 Metric 到当前的 chunk
		currentMetrics = append(currentMetrics, metric)
		currentSize += mSize
	}

	// 添加最后一个 chunk 如果它包含元素
	if len(currentMetrics) > 0 {
		splitResult = append(splitResult, &pb.ResourceMetrics{
			Resource: resourceMetric.Resource,
			ScopeMetrics: []*pb.ScopeMetrics{
				{
					Scope:     scopeMetric.Scope,
					Metrics:   currentMetrics,
					SchemaUrl: scopeMetric.SchemaUrl,
				},
			},
			SchemaUrl: resourceMetric.SchemaUrl,
		})
	}

	return splitResult
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

			for _, resource := range SplitMetricData(&metricData) {
				for _, v := range resource {
					sem <- struct{}{}
					wg.Add(1)
					go func(resourceGroup []*pb.ResourceMetrics) {
						defer func() { <-sem }()
						sendRequest(context.Background(), clientAddr, resourceGroup, bar, &wg, &mu)
						time.Sleep(100 * time.Millisecond)
					}([]*pb.ResourceMetrics{v})
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
