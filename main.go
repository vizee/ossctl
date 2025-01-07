package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
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

var (
	verbose bool
)

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

type MatchRule struct {
	re  *regexp.Regexp
	yes bool
}

type Filter struct {
	rules []*MatchRule
	def   bool
}

func (f *Filter) isMatch(s string) bool {
	for _, rule := range f.rules {
		if rule.re.MatchString(s) {
			return rule.yes
		}
	}
	return false
}

func buildFilter(rules []string) (*Filter, error) {
	mrs := make([]*MatchRule, 0, len(rules))
	for _, rule := range rules {
		yes := true
		if strings.HasPrefix(rule, "!") {
			rule = rule[1:]
			yes = false
		}
		re, err := regexp.Compile(rule)
		if err != nil {
			return nil, err
		}
		mrs = append(mrs, &MatchRule{
			re:  re,
			yes: yes,
		})
	}
	return &Filter{
		rules: mrs,
	}, nil
}

var stats struct {
	total int64
	size  int64
	start time.Time
}

func toHumanReadableSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}

func addStatsFlag(cmd *cobra.Command, withSize bool) {
	var showStats bool

	cmd.PostRun = func(cmd *cobra.Command, args []string) {
		if showStats {
			elapsed := time.Since(stats.start).Round(time.Second)
			var result strings.Builder
			fmt.Fprintf(&result, "elapsed: %s, total: %d files", elapsed.String(), stats.total)
			if withSize {
				fmt.Fprintf(&result, ", size: %s", toHumanReadableSize(stats.size))
			}
			result.WriteByte('\n')
			io.WriteString(os.Stderr, result.String())
		}
	}

	cmd.Flags().BoolVar(&showStats, "stats", false, "show statistics")
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
	stats.total++

	if !opts.DryRun {
		res, err := downloader.DownloadFile(ctx, &oss.GetObjectRequest{
			Bucket: oss.Ptr(bucket),
			Key:    oss.Ptr(key),
		}, outputPath)
		if err != nil {
			return err
		}

		vprintf("download result: %s written=%d", outputPath, res.Written)
		stats.size += res.Written
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
	nwr, err := io.Copy(w, res.Body)
	if err != nil {
		return err
	}
	stats.total++
	stats.size += nwr
	return nil
}

func downloadDir(ctx context.Context, downloader *oss.Downloader, opts *DownloadOptions, bucket string, prefix string, outputRoot string, filter *Filter) error {
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
				objectKey := oss.ToString(op.Key)
				if filter.isMatch(objectKey) {
					continue
				}

				relDir, fileName := path.Split(strings.TrimPrefix(objectKey, prefix))
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
				err := downloadObject(ctx, downloader, opts, bucket, objectKey, outputFile)
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

func getFileMD5(fname string, generatePreMD5 bool) ([]byte, error) {
	preMD5File := filepath.Join(filepath.Dir(fname), "."+filepath.Base(fname)+".md5")
	preMD5, err := os.ReadFile(preMD5File)
	if err == nil {
		return preMD5, nil
	}

	f, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	hasher := md5.New()

	buf := make([]byte, 8*1024)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			_, _ = hasher.Write(buf[:n])
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	md5sum := hasher.Sum(nil)

	if generatePreMD5 {
		_ = os.WriteFile(preMD5File, md5sum, 0644)
	}

	return md5sum, nil
}

type FileComparer struct {
	checkMD5       bool
	generatePreMD5 bool
}

func (c *FileComparer) compareFile(curFile string, diffFile string) (bool, error) {
	curInfo, err := os.Stat(curFile)
	if err != nil {
		return false, err
	}
	var curMD5 []byte
	if c.checkMD5 {
		curMD5, err = getFileMD5(curFile, c.generatePreMD5)
		if err != nil {
			return false, err
		}
	}

	diffInfo, err := os.Stat(diffFile)
	if err != nil {
		if !os.IsNotExist(err) {
			return false, err
		}
		return false, nil
	}

	if curInfo.Size() != diffInfo.Size() {
		return false, nil
	}

	if c.checkMD5 {
		diffMD5, err := getFileMD5(diffFile, false)
		if err != nil {
			return false, err
		}
		if !bytes.Equal(curMD5, diffMD5) {
			return false, nil
		}
	}
	return true, nil
}

type UploadOptions struct {
	Workers         int
	ForbidOverwrite bool
	IncludeHidden   bool
	DryRun          bool
	DiffDir         string
	DiffMD5         bool
	GenerateMD5     bool
}

func uploadObject(ctx context.Context, uploader *oss.Uploader, opts *UploadOptions, bucket string, key string, filePath string) error {
	fi, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	req := &oss.PutObjectRequest{
		Bucket: oss.Ptr(bucket),
		Key:    oss.Ptr(key),
	}
	if opts.ForbidOverwrite {
		req.ForbidOverwrite = oss.Ptr("true")
	}

	vprintf("upload: %s -> %s:%s\n", filePath, bucket, key)
	stats.total++
	stats.size += fi.Size()

	if !opts.DryRun {
		res, err := uploader.UploadFile(ctx, req, filePath)
		if err != nil {
			return err
		}

		vprintf("upload result: %s etag=%s\n", filePath, oss.ToString(res.ETag))
	}

	return nil
}

func uploadDir(ctx context.Context, uploader *oss.Uploader, opts *UploadOptions, bucket string, key string, root string, recursive bool, filter *Filter) error {
	if !strings.HasSuffix(root, "/") {
		root += "/"
	}

	var fc *FileComparer
	if opts.DiffDir != "" {
		fc = &FileComparer{
			checkMD5:       opts.DiffMD5,
			generatePreMD5: opts.GenerateMD5,
		}
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
				if filter.isMatch(filePath) {
					continue
				}

				relPath := strings.TrimPrefix(filePath, root)
				if fc != nil {
					same, err := fc.compareFile(filePath, filepath.Join(opts.DiffDir, relPath))
					if err == nil && same {
						continue
					}
				}
				err := uploadObject(ctx, uploader, opts, bucket, path.Join(key, filepath.ToSlash(relPath)), filePath)
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
	stats.total++

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

func copyDir(ctx context.Context, copier *oss.Copier, opts *CopyOptions, srcBucket string, srcKey string, dstBucket string, dstKey string, filter *Filter) error {
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
				if filter.isMatch(srcKey) {
					continue
				}

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
	stats.total++

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
		filterRules []string
	)

	cmd := &cobra.Command{
		Use:     "list bucket:prefix",
		Aliases: []string{"ls"},
		Args:    cobra.ExactArgs(1),
		Short:   "list oss object",
		Run: func(cmd *cobra.Command, args []string) {
			filter, err := buildFilter(filterRules)
			if err != nil {
				fatalf("build filter: %v\n", err)
			}

			bucket, prefix, ok := splitBucketKey(args[0])
			if !ok {
				fatalf("invalid bucket:prefix\n")
			}
			err = listObjects(cmd.Context(), bucket, prefix, delimiter, startAfter, func(op *oss.ObjectProperties) error {
				objectKey := oss.ToString(op.Key)
				if filter.isMatch(objectKey) {
					return nil
				}
				if stripPrefix {
					objectKey = strings.TrimPrefix(objectKey, prefix)
				}
				fmt.Println(objectKey)
				stats.total++
				stats.size += op.Size
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

	cmd.Flags().StringArrayVarP(&filterRules, "filter", "F", nil, "filter")
	addStatsFlag(cmd, true)

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
	addStatsFlag(cmd, true)

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
		filterRules []string
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
			filter, err := buildFilter(filterRules)
			if err != nil {
				fatalf("build filter: %v\n", err)
			}

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
				err := downloadDir(cmd.Context(), downloader, opts, bucket, key, outputPath, filter)
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

	cmd.Flags().StringArrayVarP(&filterRules, "filter", "F", nil, "filter")
	addStatsFlag(cmd, true)

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
		filterRules []string
	)

	opts := &UploadOptions{
		Workers:         0,
		ForbidOverwrite: false,
		IncludeHidden:   false,
		DryRun:          false,
		DiffDir:         "",
		DiffMD5:         false,
	}

	cmd := &cobra.Command{
		Use:   "upload bucket:key local-path",
		Args:  cobra.ExactArgs(2),
		Short: "upload file/directory to oss",
		Run: func(cmd *cobra.Command, args []string) {
			filter, err := buildFilter(filterRules)
			if err != nil {
				fatalf("build filter: %v\n", err)
			}

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
				err := uploadDir(cmd.Context(), uploader, opts, bucket, key, localPath, recursive, filter)
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

	cmd.Flags().StringArrayVarP(&filterRules, "filter", "F", nil, "filter")
	addStatsFlag(cmd, true)

	cmd.Flags().BoolVar(&opts.ForbidOverwrite, "forbid-overwrite", false, "forbid overwrite")
	cmd.Flags().BoolVarP(&opts.IncludeHidden, "hidden", "H", false, "include hidden files")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "dry run")
	cmd.Flags().IntVar(&opts.Workers, "workers", 1, "workers for upload files")
	cmd.Flags().StringVar(&opts.DiffDir, "diff-dir", "", "compare file changes before upload")
	cmd.Flags().BoolVar(&opts.DiffMD5, "diff-md5", false, "compare file by MD5")
	cmd.Flags().BoolVar(&opts.GenerateMD5, "generate-md5", false, "generate .md5 file")

	return cmd
}

func copyCommand() *cobra.Command {
	var (
		parallelNum int
		filterRules []string
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
			filter, err := buildFilter(filterRules)
			if err != nil {
				fatalf("build filter: %v\n", err)
			}

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
				err := copyDir(cmd.Context(), copier, opts, srcBucket, srcKey, dstBucket, dstKey, filter)
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

	cmd.Flags().StringArrayVarP(&filterRules, "filter", "F", nil, "filter")
	addStatsFlag(cmd, false)

	cmd.Flags().BoolVarP(&opts.Recursive, "recursive", "R", false, "copy object recursively")
	cmd.Flags().BoolVar(&opts.ForbidOverwrite, "forbid-overwrite", false, "forbid overwrite")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "dry run")
	cmd.Flags().IntVar(&opts.Workers, "workers", 1, "workers for copy files")

	return cmd
}

func removeCommand() *cobra.Command {
	var (
		recursive   bool
		dryRun      bool
		filterRules []string
	)

	cmd := &cobra.Command{
		Use:     "remove bucket:key",
		Aliases: []string{"rm"},
		Args:    cobra.ExactArgs(1),
		Short:   "remove oss object",
		Run: func(cmd *cobra.Command, args []string) {
			filter, err := buildFilter(filterRules)
			if err != nil {
				fatalf("build filter: %v\n", err)
			}

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
					objectKey := oss.ToString(op.Key)
					if filter.isMatch(objectKey) {
						return nil
					}

					err := deleteObject(cmd.Context(), bucket, objectKey, dryRun)
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

	cmd.Flags().StringArrayVarP(&filterRules, "filter", "F", nil, "filter")
	addStatsFlag(cmd, false)

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

	stats.start = time.Now()
	_ = app.Execute()
}
