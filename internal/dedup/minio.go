package dedup

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// MinioBlobStore — реализация BlobStore поверх MinIO/S3. Один бакет на все
// сегменты dedup; ключ объекта = Prefix+seg-<uuid>.zst (задаётся в PackerConfig).
type MinioBlobStore struct {
	client *minio.Client
	bucket string
}

// MinioConfig — параметры подключения к MinIO/S3.
type MinioConfig struct {
	Endpoint  string // host:port без схемы, напр. "localhost:9000"
	AccessKey string
	SecretKey string
	Bucket    string
	UseSSL    bool
}

// NewMinioBlobStore подключается к MinIO и убеждается, что бакет существует
// (создаёт при отсутствии). Бакет — единая точка хранения сегментов.
func NewMinioBlobStore(ctx context.Context, cfg MinioConfig) (*MinioBlobStore, error) {
	client, err := minio.New(cfg.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.AccessKey, cfg.SecretKey, ""),
		Secure: cfg.UseSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("dedup: minio клиент: %w", err)
	}
	exists, err := client.BucketExists(ctx, cfg.Bucket)
	if err != nil {
		return nil, fmt.Errorf("dedup: проверка бакета %s: %w", cfg.Bucket, err)
	}
	if !exists {
		if err := client.MakeBucket(ctx, cfg.Bucket, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("dedup: создание бакета %s: %w", cfg.Bucket, err)
		}
	}
	return &MinioBlobStore{client: client, bucket: cfg.Bucket}, nil
}

// PutObject кладёт сегмент целиком.
func (s *MinioBlobStore) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := s.client.PutObject(ctx, s.bucket, key,
		bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/zstd"})
	if err != nil {
		return fmt.Errorf("dedup: put %s: %w", key, err)
	}
	return nil
}

// GetRange читает length байт объекта key с offset — это HTTP Range-запрос к S3,
// который тянет ровно один zstd-фрейм, а не весь сегмент.
func (s *MinioBlobStore) GetRange(ctx context.Context, key string, offset int64, length int) ([]byte, error) {
	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(offset, offset+int64(length)-1); err != nil {
		return nil, fmt.Errorf("dedup: set range %s: %w", key, err)
	}
	obj, err := s.client.GetObject(ctx, s.bucket, key, opts)
	if err != nil {
		return nil, fmt.Errorf("dedup: get %s: %w", key, err)
	}
	defer obj.Close()

	buf := make([]byte, length)
	// ReadFull: Range обязан вернуть ровно length байт; меньше — ошибка/битый объект.
	if _, err := io.ReadFull(obj, buf); err != nil {
		return nil, fmt.Errorf("dedup: чтение диапазона %s: %w", key, err)
	}
	return buf, nil
}
