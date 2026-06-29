package connector

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/longxiucai/connectest/internal/config"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type MinIOConnector struct{}

func (c *MinIOConnector) Name() string { return "MinIO" }

func (c *MinIOConnector) newClient(cfg config.Config) (*minio.Client, error) {
	endpoint := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	opts := &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.User, cfg.Password, ""),
		Secure: cfg.UseTLS,
	}
	if cfg.UseTLS {
		tlsCfg := buildTLSConfig(cfg)
		opts.Transport = &http.Transport{
			TLSClientConfig: tlsCfg,
		}
	}
	return minio.New(endpoint, opts)
}

func (c *MinIOConnector) TestConnection(ctx context.Context, cfg config.Config) (*config.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := c.newClient(cfg)
	if err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("创建客户端失败: %v", err)}, nil
	}

	// 尝试列出桶来验证连接
	buckets, err := client.ListBuckets(ctx)
	if err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("连接失败: %v", err)}, nil
	}

	si := &config.ServerInfo{Status: "Running"}
	si.InfoItems = append(si.InfoItems,
		config.InfoItem{Label: "服务器地址", Value: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		config.InfoItem{Label: "存储桶数", Value: fmt.Sprintf("%d", len(buckets))},
	)

	// 计算所有桶的对象总数
	totalObjects := 0
	totalSize := int64(0)
	for _, bucket := range buckets {
		si.InfoItems = append(si.InfoItems, config.InfoItem{
			Label: "桶: " + bucket.Name,
			Value: bucket.CreationDate.Format("2006-01-02 15:04:05"),
		})
		// 统计每个桶的对象数和大小
		count := 0
		for obj := range client.ListObjects(ctx, bucket.Name, minio.ListObjectsOptions{Recursive: true}) {
			if obj.Err != nil {
				continue
			}
			count++
			totalSize += obj.Size
			if count >= 10000 { // 限制统计数量
				break
			}
		}
		totalObjects += count
	}
	si.InfoItems = append(si.InfoItems,
		config.InfoItem{Label: "对象总数(估)", Value: fmt.Sprintf("%d", totalObjects)},
		config.InfoItem{Label: "总大小(估)", Value: formatSize(totalSize)},
	)

	// 尝试获取 MinIO 服务器信息（通过 health endpoint）
	scheme := "http"
	if cfg.UseTLS {
		scheme = "https"
	}
	healthClient := &http.Client{Timeout: 5 * time.Second}
	if cfg.UseTLS {
		healthClient.Transport = &http.Transport{
			TLSClientConfig: buildTLSConfig(cfg),
		}
	}
	healthURL := fmt.Sprintf("%s://%s:%d/minio/health/live", scheme, cfg.Host, cfg.Port)
	if resp, err := healthClient.Get(healthURL); err == nil {
		resp.Body.Close()
		if resp.StatusCode == 200 {
			si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "健康检查", Value: "✅ 正常"})
		} else {
			si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "健康检查", Value: fmt.Sprintf("⚠️ HTTP %d", resp.StatusCode)})
		}
	}

	// MinIO 集群检测（通过 /minio/health/cluster）
	clusterURL := fmt.Sprintf("%s://%s:%d/minio/health/cluster", scheme, cfg.Host, cfg.Port)
	if resp, err := healthClient.Get(clusterURL); err == nil {
		resp.Body.Close()
		if resp.StatusCode == 200 {
			si.Cluster = &config.ClusterInfo{
				Mode:    "Erasure Code Cluster",
				Summary: "集群模式运行中",
			}
		}
	} else {
		// 单节点模式
		if len(buckets) >= 0 {
			si.Cluster = &config.ClusterInfo{Mode: "Standalone"}
		}
	}

	return &config.Result{
		Success:    true,
		Message:    fmt.Sprintf("连接成功 - MinIO (%d 个桶)", len(buckets)),
		ServerInfo: si,
	}, nil
}

// formatSize 格式化文件大小
func formatSize(size int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)
	switch {
	case size >= TB:
		return fmt.Sprintf("%.2f TB", float64(size)/float64(TB))
	case size >= GB:
		return fmt.Sprintf("%.2f GB", float64(size)/float64(GB))
	case size >= MB:
		return fmt.Sprintf("%.2f MB", float64(size)/float64(MB))
	case size >= KB:
		return fmt.Sprintf("%.2f KB", float64(size)/float64(KB))
	default:
		return fmt.Sprintf("%d B", size)
	}
}

func (c *MinIOConnector) SupportedActions() []config.Action {
	return []config.Action{
		{
			Name:        "list_buckets",
			Label:       "列出所有桶",
			Description: "列出 MinIO 上的所有存储桶",
		},
		{
			Name:        "create_bucket",
			Label:       "创建桶",
			Description: "创建一个新的存储桶",
			Params: []config.ActionParam{
				{Name: "bucket", Label: "桶名称", Placeholder: "test-bucket", Required: true, Default: "connectest-test-bucket"},
			},
		},
		{
			Name:        "upload",
			Label:       "上传文件",
			Description: "向指定桶上传一个测试文件",
			Params: []config.ActionParam{
				{Name: "bucket", Label: "桶名称", Placeholder: "test-bucket", Required: true, Default: "connectest-test-bucket"},
				{Name: "object", Label: "对象名", Placeholder: "test.txt", Required: true, Default: "connectest-test.txt"},
				{Name: "content", Label: "文件内容", Placeholder: "hello world", Required: true, Default: "Hello from connectest!"},
			},
		},
		{
			Name:        "download",
			Label:       "下载文件",
			Description: "从指定桶下载一个文件",
			Params: []config.ActionParam{
				{Name: "bucket", Label: "桶名称", Placeholder: "test-bucket", Required: true, Default: "connectest-test-bucket"},
				{Name: "object", Label: "对象名", Placeholder: "test.txt", Required: true, Default: "connectest-test.txt"},
			},
		},
		{
			Name:        "list_objects",
			Label:       "列出对象",
			Description: "列出指定桶中的所有对象",
			Params: []config.ActionParam{
				{Name: "bucket", Label: "桶名称", Placeholder: "test-bucket", Required: true, Default: "connectest-test-bucket"},
			},
		},
	}
}

func (c *MinIOConnector) ExecuteAction(ctx context.Context, cfg config.Config, action string, params map[string]string) (*config.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client, err := c.newClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("创建客户端失败: %w", err)
	}

	switch action {
	case "list_buckets":
		buckets, err := client.ListBuckets(ctx)
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("列出桶失败: %v", err)}, nil
		}

		var output strings.Builder
		for _, b := range buckets {
			output.WriteString(fmt.Sprintf("%-30s 创建时间: %s\n", b.Name, b.CreationDate.Format("2006-01-02 15:04:05")))
		}

		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("共 %d 个桶", len(buckets)),
			Details: output.String(),
		}, nil

	case "create_bucket":
		bucketName := params["bucket"]
		if bucketName == "" {
			return nil, fmt.Errorf("桶名称不能为空")
		}

		exists, err := client.BucketExists(ctx, bucketName)
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("检查桶失败: %v", err)}, nil
		}
		if exists {
			return &config.Result{
				Success: true,
				Message: fmt.Sprintf("桶 '%s' 已存在", bucketName),
			}, nil
		}

		if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("创建桶失败: %v", err)}, nil
		}

		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("桶 '%s' 创建成功", bucketName),
		}, nil

	case "upload":
		bucketName := params["bucket"]
		objectName := params["object"]
		content := params["content"]

		if bucketName == "" || objectName == "" {
			return nil, fmt.Errorf("桶名称和对象名不能为空")
		}
		if content == "" {
			content = "Hello from connectest!"
		}

		// 确保桶存在
		exists, err := client.BucketExists(ctx, bucketName)
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("检查桶失败: %v", err)}, nil
		}
		if !exists {
			if err := client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
				return &config.Result{Success: false, Message: fmt.Sprintf("创建桶失败: %v", err)}, nil
			}
		}

		reader := bytes.NewReader([]byte(content))
		info, err := client.PutObject(ctx, bucketName, objectName, reader, int64(len(content)), minio.PutObjectOptions{
			ContentType: "text/plain",
		})
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("上传失败: %v", err)}, nil
		}

		return &config.Result{
			Success: true,
			Message: "文件上传成功",
			Details: fmt.Sprintf("桶: %s\n对象: %s\n大小: %d bytes\nETag: %s", bucketName, objectName, info.Size, info.ETag),
		}, nil

	case "download":
		bucketName := params["bucket"]
		objectName := params["object"]

		if bucketName == "" || objectName == "" {
			return nil, fmt.Errorf("桶名称和对象名不能为空")
		}

		reader, err := client.GetObject(ctx, bucketName, objectName, minio.GetObjectOptions{})
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("下载失败: %v", err)}, nil
		}
		defer reader.Close()

		data, err := io.ReadAll(io.LimitReader(reader, 1024*1024)) // 限制 1MB
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("读取失败: %v", err)}, nil
		}

		stat, _ := reader.Stat()
		return &config.Result{
			Success: true,
			Message: "文件下载成功",
			Details: fmt.Sprintf("桶: %s\n对象: %s\n大小: %d bytes\n内容:\n%s",
				bucketName, objectName, stat.Size, string(data)),
		}, nil

	case "list_objects":
		bucketName := params["bucket"]
		if bucketName == "" {
			return nil, fmt.Errorf("桶名称不能为空")
		}

		var output strings.Builder
		count := 0
		for obj := range client.ListObjects(ctx, bucketName, minio.ListObjectsOptions{Recursive: true}) {
			if obj.Err != nil {
				continue
			}
			output.WriteString(fmt.Sprintf("%-40s 大小: %-10d 修改: %s\n",
				obj.Key, obj.Size, obj.LastModified.Format("2006-01-02 15:04:05")))
			count++
			if count >= 100 {
				output.WriteString("\n... (仅显示前 100 个对象)")
				break
			}
		}

		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("桶 '%s' 共 %d 个对象", bucketName, count),
			Details: output.String(),
		}, nil

	default:
		return nil, fmt.Errorf("不支持的操作: %s", action)
	}
}

