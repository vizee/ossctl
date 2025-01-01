package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/spf13/cobra"
)

var ossClient = func() *oss.Client {
	ossRegion := cmp.Or(os.Getenv("OSS_REGION"), "cn-hangzhou")
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credentials.NewEnvironmentVariableCredentialsProvider()).
		WithRegion(ossRegion).
		WithConnectTimeout(time.Second * 10).
		WithReadWriteTimeout(time.Minute).
		WithUseInternalEndpoint(os.Getenv("OSS_INTERNAL_ENDPOINT") == "1").
		WithDisableSSL(os.Getenv("OSS_DISABLE_SSL") == "1").
		WithRetryMaxAttempts(3)
	return oss.NewClient(cfg)
}()

var verbose bool

func vprintf(format string, args ...any) {
	if verbose {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format, args...)
	os.Exit(1)
}

func splitBucketKey(s string) (string, string, bool) {
	bucket, key, ok := strings.Cut(s, ":")
	if !ok || bucket == "" || key == "" {
		return "", "", false
	}
	return bucket, strings.TrimLeft(key, "/"), true
}

func listObjects(ctx context.Context, bucket string, prefix string, delimiter string, startAfter string, f func(*oss.ObjectProperties) error) error {
	var nextToken *string
	for {
		req := &oss.ListObjectsV2Request{
			Bucket:            oss.Ptr(bucket),
			MaxKeys:           100,
			Prefix:            oss.Ptr(prefix),
			ContinuationToken: nextToken,
		}
		if delimiter != "" {
			req.Delimiter = oss.Ptr(delimiter)
		}
		if startAfter != "" {
			req.StartAfter = oss.Ptr(startAfter)
		}
		result, err := ossClient.ListObjectsV2(ctx, req)
		if err != nil {
			return err
		}

		for i := range result.Contents {
			err := f(&result.Contents[i])
			if err != nil {
				return err
			}
		}
		if !result.IsTruncated {
			break
		}
		nextToken = result.NextContinuationToken
	}
	return nil
}

type DownloadOptions struct {
	Force       bool
	UseTempFile bool
	SkipExisted bool
	Recursive   bool
	DryRun      bool
	Workers     int
}

func downloadObject(ctx context.Context, downloader *oss.Downloader, opts *DownloadOptions, bucket string, key string, outputPath string) error {
	_, err := os.Stat(outputPath)
	if err == nil {
		if opts.Force {
			if !opts.UseTempFile {
				err := os.Remove(outputPath)
				if err != nil {
					return err
				}
			}
		} else if opts.SkipExisted {
			return nil
		}
	}
	vprintf("download: %s:%s -> %s\n", bucket, key, outputPath)

	if !opts.DryRun {
		res, err := downloader.DownloadFile(ctx, &oss.GetObjectRequest{
			Bucket: oss.Ptr(bucket),
			Key:    oss.Ptr(key),
		}, outputPath)
		if err != nil {
			return err
		}

		vprintf("download result: %s written=%d", outputPath, res.Written)
	}

	return nil
}

func catObject(ctx context.Context, w io.Writer, bucket string, key string) error {
	res, err := ossClient.GetObject(ctx, &oss.GetObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(key),
	})
	if err != nil {
		return err
	}
	_, err = io.Copy(w, res.Body)
	return err
}

func downloadDir(ctx context.Context, downloader *oss.Downloader, opts *DownloadOptions, bucket string, prefix string, outputRoot string) error {
	var delimiter string
	if opts.Recursive {
		delimiter = ""
	} else {
		delimiter = "/"
	}
	if opts.Workers <= 0 {
		opts.Workers = 1
	}

	err := os.MkdirAll(outputRoot, 0755)
	if err != nil {
		return err
	}

	var (
		wg sync.WaitGroup
	)
	ch := make(chan *oss.ObjectProperties, opts.Workers*4)
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for op := range ch {
				relDir, fileName := path.Split(strings.TrimPrefix(oss.ToString(op.Key), prefix))
				var outDir string
				if relDir != "" {
					outDir = filepath.Join(outputRoot, filepath.FromSlash(relDir))
					_, err := os.Stat(outDir)
					if err != nil {
						if !os.IsNotExist(err) {
							fmt.Fprintf(os.Stderr, "os.Stat: %v\n", err)
							continue
						}
						err = os.MkdirAll(outDir, 0755)
						if err != nil {
							fmt.Fprintf(os.Stderr, "os.MkdirAll: %v\n", err)
							continue
						}
					}
				} else {
					outDir = outputRoot
				}
				outputFile := filepath.Join(outDir, fileName)
				err := downloadObject(ctx, downloader, opts, bucket, oss.ToString(op.Key), outputFile)
				if err != nil {
					fmt.Fprintf(os.Stderr, "download object: %v\n", err)
					continue
				}
			}
		}()
	}

	err = listObjects(ctx, bucket, prefix, delimiter, "", func(op *oss.ObjectProperties) error {
		ch <- op
		return nil
	})

	close(ch)
	wg.Wait()

	return err
}

type UploadOptions struct {
	Workers         int
	ForbidOverwrite bool
	IncludeHidden   bool
	DryRun          bool
}

func uploadObject(ctx context.Context, uploader *oss.Uploader, opts *UploadOptions, bucket string, key string, filePath string) error {
	req := &oss.PutObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(key),
	}
	if opts.ForbidOverwrite {
		req.ForbidOverwrite = oss.Ptr("true")
	}

	vprintf("upload: %s -> %s:%s\n", filePath, bucket, key)

	if !opts.DryRun {
		res, err := uploader.UploadFile(ctx, req, filePath)
		if err != nil {
			return err
		}

		vprintf("upload result: %s etag=%s\n", filePath, oss.ToString(res.ETag))
	}

	return nil
}

func uploadDir(ctx context.Context, uploader *oss.Uploader, opts *UploadOptions, bucket string, key string, root string, recursive bool) error {
	if !strings.HasSuffix(root, "/") {
		root += "/"
	}

	var (
		wg sync.WaitGroup
	)
	ch := make(chan string, opts.Workers*4)
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for filePath := range ch {
				relPath := filepath.ToSlash(strings.TrimPrefix(filePath, root))
				err := uploadObject(ctx, uploader, opts, bucket, path.Join(key, relPath), filePath)
				if err != nil {
					fmt.Fprintf(os.Stderr, "upload object: %v\n", err)
					continue
				}
			}
		}()
	}

	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		isHidden := strings.HasPrefix(d.Name(), ".")

		if d.IsDir() {
			if root == path {
				return nil
			}

			if !recursive || (isHidden && !opts.IncludeHidden) {
				return filepath.SkipDir
			}

			return nil
		}

		if isHidden && !opts.IncludeHidden {
			return nil
		}

		ch <- path

		return nil
	})

	close(ch)
	wg.Wait()

	return err
}

type CopyOptions struct {
	ForbidOverwrite bool
	Recursive       bool
	DryRun          bool
	Workers         int
}

func copyObject(ctx context.Context, copier *oss.Copier, opts *CopyOptions, srcBucket string, srcKey string, dstBucket string, dstKey string) error {
	vprintf("copy: %s:%s -> %s:%s\n", srcBucket, srcKey, dstBucket, dstKey)

	if !opts.DryRun {
		req := &oss.CopyObjectRequest{
			Bucket:       oss.Ptr(dstBucket),
			Key:          oss.Ptr(dstKey),
			SourceBucket: oss.Ptr(srcBucket),
			SourceKey:    oss.Ptr(srcKey),
		}
		if opts.ForbidOverwrite {
			req.ForbidOverwrite = oss.Ptr("true")
		}
		_, err := copier.Copy(ctx, req)
		if err != nil {
			return err
		}
	}

	return nil
}

func copyDir(ctx context.Context, copier *oss.Copier, opts *CopyOptions, srcBucket string, srcKey string, dstBucket string, dstKey string) error {
	var delimiter string
	if opts.Recursive {
		delimiter = ""
	} else {
		delimiter = "/"
	}

	var (
		wg sync.WaitGroup
	)
	ch := make(chan *oss.ObjectProperties, opts.Workers*4)
	for i := 0; i < opts.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for op := range ch {
				srcKey := oss.ToString(op.Key)
				err := copyObject(ctx, copier, opts, srcBucket, srcKey, dstBucket, path.Join(dstKey, srcKey))
				if err != nil {
					fmt.Fprintf(os.Stderr, "copy object: %v\n", err)
					continue
				}
			}
		}()
	}

	err := listObjects(ctx, srcBucket, srcKey, delimiter, "", func(op *oss.ObjectProperties) error {
		ch <- op
		return nil
	})

	close(ch)
	wg.Wait()

	return err
}

func deleteObject(ctx context.Context, bucket string, key string, dryRun bool) error {
	vprintf("delete: %s:%s\n", bucket, key)

	if !dryRun {
		_, err := ossClient.DeleteObject(ctx, &oss.DeleteObjectRequest{
			Bucket: oss.Ptr(bucket),
			Key:    oss.Ptr(key),
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func listCommand() *cobra.Command {
	var (
		delimiter   string
		startAfter  string
		stripPrefix bool
	)

	cmd := &cobra.Command{
		Use:     "list bucket:prefix",
		Aliases: []string{"ls"},
		Args:    cobra.ExactArgs(1),
		Short:   "list oss object",
		Run: func(cmd *cobra.Command, args []string) {
			bucket, prefix, ok := splitBucketKey(args[0])
			if !ok {
				fatalf("invalid bucket:prefix\n")
			}
			err := listObjects(cmd.Context(), bucket, prefix, delimiter, startAfter, func(op *oss.ObjectProperties) error {
				key := oss.ToString(op.Key)
				if stripPrefix {
					key = strings.TrimPrefix(key, prefix)
				}
				fmt.Println(key)
				return nil
			})
			if err != nil {
				fatalf("list object: %v\n", err)
			}
		},
	}

	cmd.Flags().StringVar(&delimiter, "delimiter", "", "delimiter")
	cmd.Flags().StringVar(&startAfter, "start-after", "", "start after")
	cmd.Flags().BoolVar(&stripPrefix, "strip-prefix", false, "strip key prefix")

	return cmd
}

func catCommand() *cobra.Command {
	var (
		output string
	)

	cmd := &cobra.Command{
		Use:   "cat bucket:key...",
		Args:  cobra.MinimumNArgs(1),
		Short: "print content to stdout",
		Run: func(cmd *cobra.Command, args []string) {
			var out io.Writer
			if output == "-" {
				out = os.Stdout
			} else {
				f, err := os.Create(output)
				if err != nil {
					fatalf("%v\n", err)
				}
				defer f.Close()
				out = f
			}

			for _, arg := range args {
				bucket, key, ok := splitBucketKey(arg)
				if !ok {
					fatalf("invalid bucket:prefix\n")
				}
				err := catObject(cmd.Context(), out, bucket, key)
				if err != nil {
					fatalf("read object: %v\n", err)
				}
			}
		},
	}

	cmd.Flags().StringVarP(&output, "output", "o", "-", "output")

	return cmd
}

type ObjectInfo struct {
	ContentLength int64     `json:"contentLength"`
	ContentType   string    `json:"contentType"`
	Etag          string    `json:"etag"`
	LastModified  time.Time `json:"lastModified"`
	ContentMD5    string    `json:"contentMD5"`
	HashCRC64     string    `json:"hashCRC64"`
}

func headCommand() *cobra.Command {
	var (
		jsonFormat bool
	)

	cmd := &cobra.Command{
		Use:   "head bucket:key",
		Args:  cobra.ExactArgs(1),
		Short: "print head information",
		Run: func(cmd *cobra.Command, args []string) {
			bucket, key, ok := splitBucketKey(args[0])
			if !ok {
				fatalf("invalid bucket:prefix\n")
			}

			info, err := ossClient.HeadObject(cmd.Context(), &oss.HeadObjectRequest{
				Bucket: oss.Ptr(bucket),
				Key:    oss.Ptr(key),
			})
			if err != nil {
				fatalf("read object information: %v\n", err)
			}

			oi := &ObjectInfo{
				ContentLength: info.ContentLength,
				ContentType:   oss.ToString(info.ContentType),
				Etag:          oss.ToString(info.ETag),
				LastModified:  oss.ToTime(info.LastModified),
				ContentMD5:    oss.ToString(info.ContentMD5),
				HashCRC64:     oss.ToString(info.HashCRC64),
			}

			if jsonFormat {
				_ = json.NewEncoder(os.Stdout).Encode(oi)
			} else {
				fmt.Println("ContentLength:", oi.ContentLength)
				fmt.Println("ContentType:", oi.ContentType)
				fmt.Println("Etag:", oi.Etag)
				fmt.Println("LastModified:", oi.LastModified.Format(time.RFC3339))
				fmt.Println("ContentMD5:", oi.ContentMD5)
				fmt.Println("HashCRC64:", oi.HashCRC64)
			}
		},
	}

	cmd.Flags().BoolVar(&jsonFormat, "json", false, "JSON format")

	return cmd
}

func downloadCommand() *cobra.Command {
	var (
		outputPath  string
		parallelNum int
	)
	opts := &DownloadOptions{
		Force:       false,
		UseTempFile: false,
		SkipExisted: false,
		Recursive:   false,
		Workers:     2,
	}

	cmd := &cobra.Command{
		Use:   "download bucket:key",
		Args:  cobra.ExactArgs(1),
		Short: "download oss object",
		Run: func(cmd *cobra.Command, args []string) {
			bucket, key, ok := splitBucketKey(args[0])
			if !ok {
				fatalf("invalid bucket:key\n")
			}

			downloader := ossClient.NewDownloader(func(do *oss.DownloaderOptions) {
				do.UseTempFile = opts.UseTempFile
				do.ParallelNum = parallelNum
			})

			if strings.HasSuffix(key, "/") || opts.Recursive {
				if outputPath == "" {
					outputPath = "."
				}
				err := downloadDir(cmd.Context(), downloader, opts, bucket, key, outputPath)
				if err != nil {
					fatalf("download directory: %v\n", err)
				}
			} else {
				if outputPath == "" {
					outputPath = path.Base(key)
				}
				err := downloadObject(cmd.Context(), downloader, opts, bucket, key, outputPath)
				if err != nil {
					fatalf("download object: %v\n", err)
				}
			}
		},
	}

	cmd.Flags().StringVarP(&outputPath, "output", "o", "", "output path")
	cmd.Flags().IntVar(&parallelNum, "parallel", oss.DefaultDownloadParallel, "parallel for download task")

	cmd.Flags().BoolVarP(&opts.Force, "force", "f", false, "overwrite file")
	cmd.Flags().BoolVar(&opts.UseTempFile, "use-temp-file", false, "use .temp file")
	cmd.Flags().BoolVar(&opts.SkipExisted, "skip-existed", false, "skip existed file")
	cmd.Flags().BoolVarP(&opts.Recursive, "recursive", "R", false, "download all objects recursively")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "dry run")
	cmd.Flags().IntVar(&opts.Workers, "workers", 1, "workers for download files")

	return cmd
}

func uploadCommand() *cobra.Command {
	var (
		parallelNum int
		recursive   bool
	)

	opts := &UploadOptions{
		Workers:         0,
		ForbidOverwrite: false,
		IncludeHidden:   false,
	}

	cmd := &cobra.Command{
		Use:   "upload bucket:key local-path",
		Args:  cobra.ExactArgs(2),
		Short: "upload file/directory to oss",
		Run: func(cmd *cobra.Command, args []string) {
			bucket, key, ok := splitBucketKey(args[0])
			if !ok {
				fatalf("invalid bucket:key\n")
			}
			localPath := args[1]
			fi, err := os.Stat(localPath)
			if err != nil {
				fatalf("%v\n", err)
			}

			uploader := ossClient.NewUploader(func(uo *oss.UploaderOptions) {
				uo.ParallelNum = parallelNum
			})

			localPath = filepath.Clean(localPath)
			if fi.IsDir() {
				err := uploadDir(cmd.Context(), uploader, opts, bucket, key, localPath, recursive)
				if err != nil {
					fatalf("upload directory: %v\n", err)
				}
			} else {
				err := uploadObject(cmd.Context(), uploader, opts, bucket, key, localPath)
				if err != nil {
					fatalf("upload object: %v\n", err)
				}
			}
		},
	}

	cmd.Flags().IntVar(&parallelNum, "parallel", oss.DefaultUploadParallel, "parallel for upload task")
	cmd.Flags().BoolVarP(&recursive, "recursive", "R", false, "upload all files recursively")

	cmd.Flags().BoolVar(&opts.ForbidOverwrite, "forbid-overwrite", false, "forbid overwrite")
	cmd.Flags().BoolVarP(&opts.IncludeHidden, "hidden", "H", false, "include hidden files")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "dry run")
	cmd.Flags().IntVar(&opts.Workers, "workers", 1, "workers for upload files")

	return cmd
}

func copyCommand() *cobra.Command {
	var (
		parallelNum int
	)

	opts := &CopyOptions{
		ForbidOverwrite: false,
		Recursive:       false,
		DryRun:          false,
		Workers:         0,
	}

	cmd := &cobra.Command{
		Use:     "copy source-bucket:key target-bucket:key",
		Aliases: []string{"cp"},
		Args:    cobra.ExactArgs(2),
		Short:   "copy oss object",
		Run: func(cmd *cobra.Command, args []string) {
			srcBucket, srcKey, ok := splitBucketKey(args[0])
			if !ok {
				fatalf("invalid bucket:key\n")
			}
			dstBucket, dstKey, ok := splitBucketKey(args[1])
			if !ok {
				fatalf("invalid bucket:key\n")
			}

			copier := ossClient.NewCopier(func(co *oss.CopierOptions) {
				co.ParallelNum = parallelNum
			})

			if strings.HasSuffix(srcKey, "/") || opts.Recursive {
				err := copyDir(cmd.Context(), copier, opts, srcBucket, srcKey, dstBucket, dstKey)
				if err != nil {
					fatalf("copy directory: %v\n", err)
				}
			} else {
				err := copyObject(cmd.Context(), copier, opts, srcBucket, srcKey, dstBucket, dstKey)
				if err != nil {
					fatalf("copy object: %v\n", err)
				}
			}
		},
	}

	cmd.Flags().IntVar(&parallelNum, "parallel", oss.DefaultUploadParallel, "parallel for upload task")

	cmd.Flags().BoolVarP(&opts.Recursive, "recursive", "R", false, "copy object recursively")
	cmd.Flags().BoolVar(&opts.ForbidOverwrite, "forbid-overwrite", false, "forbid overwrite")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "dry run")
	cmd.Flags().IntVar(&opts.Workers, "workers", 1, "workers for copy files")

	return cmd
}

func removeCommand() *cobra.Command {
	var (
		recursive bool
		dryRun    bool
	)

	cmd := &cobra.Command{
		Use:     "remove bucket:key",
		Aliases: []string{"rm"},
		Args:    cobra.ExactArgs(1),
		Short:   "remove oss object",
		Run: func(cmd *cobra.Command, args []string) {
			bucket, key, ok := splitBucketKey(args[0])
			if !ok {
				fatalf("invalid bucket:key\n")
			}

			if strings.HasSuffix(key, "/") || recursive {
				var delimiter string
				if recursive {
					delimiter = ""
				} else {
					delimiter = "/"
				}
				err := listObjects(cmd.Context(), bucket, key, delimiter, "", func(op *oss.ObjectProperties) error {
					err := deleteObject(cmd.Context(), bucket, oss.ToString(op.Key), dryRun)
					if err != nil {
						fmt.Fprintf(os.Stderr, "delete object: %v\n", err)
					}
					return nil
				})
				if err != nil {
					fatalf("delete directory: %v\n", err)
				}
			} else {
				err := deleteObject(cmd.Context(), bucket, key, dryRun)
				if err != nil {
					fatalf("delete object: %v\n", err)
				}
			}
		},
	}

	cmd.Flags().BoolVarP(&recursive, "recursive", "R", false, "delete object recursively")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "dry run")

	return cmd
}

func main() {
	app := &cobra.Command{
		Use:               "ossctl",
		DisableAutoGenTag: true,
	}
	app.PersistentFlags().BoolVarP(&verbose, "verbose", "V", false, "show verbose")

	app.AddCommand(listCommand())
	app.AddCommand(catCommand())
	app.AddCommand(headCommand())
	app.AddCommand(uploadCommand())
	app.AddCommand(downloadCommand())
	app.AddCommand(copyCommand())
	app.AddCommand(removeCommand())

	_ = app.Execute()
}
