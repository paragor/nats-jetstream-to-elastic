package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/nats-io/nats.go/jetstream"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/nats-io/nats.go"
	"github.com/paragor/nats-jetstream-to-elastic/pkg/logger"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
	"net/http"
)

const app = "nats-jetstream-to-elastic"
const indexNameSize = 256

type Config struct {
	IndexTemplate string `yaml:"indexTemplate"`
	Nats          struct {
		Url            string `yaml:"url"`
		Stream         string `yaml:"stream"`
		Consumer       string `yaml:"consumer"`
		PullMaxMessage int    `yaml:"pullMaxMessage,omitempty"`
		PullMaxBytes   int    `yaml:"pullMaxBytes,omitempty"`
		DisableAck     bool   `yaml:"disableAck"`
	} `yaml:"nats"`
	Elastic struct {
		Addrs           []string      `yaml:"addrs"`
		Username        string        `yaml:"username,omitempty"`
		Password        string        `yaml:"password,omitempty"`
		MaxWorkers      int           `yaml:"maxWorkers,omitempty"`
		MaxRetries      int           `yaml:"maxRetries,omitempty"`
		RetryOnStatus   []int         `yaml:"retryOnStatus,omitempty"`
		SkipSslVerify   bool          `yaml:"skipSslVerify"`
		FlushInterval   time.Duration `yaml:"flushInterval,omitempty"`
		FlushBytes      int           `yaml:"flushBytes,omitempty"`
		QueryFilterPath []string      `yaml:"queryFilterPath"`
	} `yaml:"elastic"`
}

func main() {
	logger.Init(app, getLogLevel())
	log := logger.Logger()
	c := &Config{}
	c.Elastic.RetryOnStatus = []int{502, 503, 504, 429}
	c.Elastic.MaxRetries = 5
	c.Elastic.MaxWorkers = 10
	c.Elastic.FlushBytes = 1 * 1024 * 1024
	c.Elastic.FlushInterval = 10 * time.Second
	c.Elastic.Username = os.Getenv("ELASTICSEARCH_USERNAME")
	c.Elastic.Password = os.Getenv("ELASTICSEARCH_PASSWORD")

	configPath := flag.String("config", "config.yaml", "path to config")
	flag.Parse()

	if err := ParseConfig(*configPath, c); err != nil {
		log.With(zap.Error(err)).Fatal("cant parse config")
	}

	if c.IndexTemplate == "" {
		log.Fatal("index template cant be empty")
	}

	indexTemplate, err := template.New("index").Funcs(map[string]any{
		"now": func() time.Time {
			return time.Now()
		},
	}).Parse(c.IndexTemplate)
	if err != nil {
		log.With(zap.Error(err)).Fatal("cant parse index template")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	natsConsumer, err := CreateNatsConsumer(ctx, c)
	if err != nil {
		log.With(zap.Error(err)).Fatal("cant create nats consumer")
	}
	consumerOptions := []jetstream.PullConsumeOpt{}
	if c.Nats.PullMaxMessage > 0 {
		consumerOptions = append(consumerOptions, jetstream.PullMaxBytes(c.Nats.PullMaxBytes))
	}
	if c.Nats.PullMaxMessage > 0 {
		consumerOptions = append(consumerOptions, jetstream.PullMaxMessages(c.Nats.PullMaxMessage))
	}

	bi, err := CreateElasticBulkIndexer(ctx, c)
	if err != nil {
		log.With(zap.Error(err)).Fatal("error on create elastic bulk indexer")
	}

	go func() {
		for {
			time.Sleep(time.Second)
			log.Debug("bulk indexer stats", zap.Any("stats", bi.Stats()))
		}
	}()

	consumerContext, err := natsConsumer.Consume(func(msg jetstream.Msg) {
		select {
		case <-ctx.Done():
			_ = msg.Nak()
			return
		default:
		}
		log := log
		metadata, err := msg.Metadata()
		if err == nil {
			log = log.With(
				zap.String("stream", metadata.Stream),
				zap.Uint64("sequence_stream", metadata.Sequence.Stream),
				zap.Uint64("sequence_consumer", metadata.Sequence.Consumer),
			)
		}
		unmarshalledData := map[string]interface{}{}
		if err := json.Unmarshal(msg.Data(), &unmarshalledData); err != nil {
			log.With(zap.Error(err)).Error("cant unmarshal data, so term")
			_ = msg.Term()
			return
		}
		bytesBuffer := bufferPool.Get().([]byte)
		defer func() {
			if len(bytesBuffer) <= indexNameSize {
				bufferPool.Put(bytesBuffer[:0])
			}
		}()
		buffer := bytes.NewBuffer(bytesBuffer[:0])
		if err := indexTemplate.Execute(buffer, unmarshalledData); err != nil {
			log.With(zap.Error(err)).Error("cant execute index template, so term")
			_ = msg.Term()
			return
		}
		if err := bi.Add(context.Background(), esutil.BulkIndexerItem{
			Action: "index",
			Index:  strings.TrimSpace(buffer.String()),
			Body:   bytes.NewReader(msg.Data()),
			OnSuccess: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem) {
				if c.Nats.DisableAck {
					return
				}
				_ = msg.Ack()
			},
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, res esutil.BulkIndexerResponseItem, err error) {
				log = log.With(zap.Error(err))
				if c.Nats.DisableAck {
					log.Error("error on bulk indexer")
					return
				}
				if metadata == nil {
					log.Error("error on bulk indexer, cant get metadata, so term msg")
					_ = msg.Term()
				}
				if metadata.NumDelivered > 5 {
					log.Error("error on bulk indexer, message is redelivered more than 5 times, so term msg")
					_ = msg.Term()
				} else {
					log.Error("error on bulk indexer, redeliver message")
					_ = msg.Nak()
				}
			},
		},
		); err != nil {
			log.With(zap.Error(err)).Warn("cant add to bulk indexer, so nack")
			_ = msg.Nak()
		}
	}, consumerOptions...)
	if err != nil {
		log.With(zap.Error(err)).Fatal("error on consume nats")
	}
	defer consumerContext.Stop()

	log.Info("consumer started!")
	select {
	case <-ctx.Done():
		log.Info("context is done, finish consuming")
		consumerContext.Drain()
		select {
		case <-time.After(time.Second*10 + c.Elastic.FlushInterval):
			log.Error("force stop consumer")
		case <-consumerContext.Closed():
		}
		log.Info("consumer is drained")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second+c.Elastic.FlushInterval*2)
		defer shutdownCancel()
		if err := bi.Close(shutdownCtx); err != nil {
			log.With(zap.Error(err)).Error("error on close elastic bulk indexer")
		}
		log.Info("bulk indexer is stopped")
	}
}

var bufferPool = sync.Pool{
	New: func() any {
		return make([]byte, indexNameSize)
	},
}

func getLogLevel() zapcore.Level {
	switch strings.ToLower(os.Getenv("LOG_LEVEL")) {
	case "info", "":
		return zapcore.InfoLevel
	case "debug":
		return zapcore.DebugLevel
	case "error":
		return zapcore.ErrorLevel
	case "warn":
		return zapcore.WarnLevel
	default:
		panic("unknown logger level")
	}
}

func ParseConfig(configPath string, defaultConfig *Config) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("cant read config file: %w", err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(defaultConfig); err != nil {
		return fmt.Errorf("cant parse config file: %w", err)
	}
	return nil
}

func CreateNatsConsumer(ctx context.Context, c *Config) (jetstream.Consumer, error) {
	natsConnect, err := nats.Connect(c.Nats.Url)
	if err != nil {
		return nil, fmt.Errorf("cant connect to nats: %w", err)
	}
	js, err := jetstream.New(natsConnect)
	if err != nil {
		return nil, fmt.Errorf("cant init jetstream: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	natsStream, err := js.Stream(ctx, c.Nats.Stream)
	if err != nil {
		return nil, fmt.Errorf("cant get nats stream: %w", err)
	}
	natsConsumer, err := natsStream.Consumer(ctx, c.Nats.Consumer)
	if err != nil {
		return nil, fmt.Errorf("cant get nats consumer: %w", err)
	}
	return natsConsumer, nil
}

func CreateElasticBulkIndexer(ctx context.Context, c *Config) (esutil.BulkIndexer, error) {
	retryBackoff := backoff.NewExponentialBackOff()
	es, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: c.Elastic.Addrs,
		Username:  c.Elastic.Username,
		Password:  c.Elastic.Password,

		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: c.Elastic.SkipSslVerify,
			},
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     time.Second * 15,
		},
		CompressRequestBody: true,
		RetryOnStatus:       c.Elastic.RetryOnStatus,
		RetryBackoff: func(i int) time.Duration {
			if i == 1 {
				retryBackoff.Reset()
			}
			return retryBackoff.NextBackOff()
		},
		MaxRetries: c.Elastic.MaxRetries,
	})
	if err != nil {
		return nil, fmt.Errorf("cant create elastic client: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	response, err := es.Ping(es.Ping.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("cant ping elastic: %w", err)
	}
	if response.StatusCode != 200 && response.StatusCode != 204 {
		return nil, fmt.Errorf("cant ping elastic: status code != 200 (%d)", response.StatusCode)
	}
	bi, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		NumWorkers:    c.Elastic.MaxWorkers,
		FlushBytes:    c.Elastic.FlushBytes,
		FlushInterval: c.Elastic.FlushInterval,
		Client:        es,
		FilterPath:    c.Elastic.QueryFilterPath,
		OnError: func(ctx context.Context, err error) {
			logger.Logger().With(zap.Error(err)).Error("bulk indexer general error")
		},
	})
	if err != nil {
		return nil, fmt.Errorf("cant create indexer: %w", err)
	}
	return bi, nil
}
