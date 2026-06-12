// SPDX-FileCopyrightText: 2026 Free Mobile
// SPDX-License-Identifier: AGPL-3.0-only

// Package kafkaout exports enriched flows to a Kafka topic, in parallel with
// the ClickHouse output. This is a first-draft component to evaluate the size
// of the change; delivery is best-effort (async produce), mirroring the
// inlet's producer and the core HTTP flow tee.
package kafkaout

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"gopkg.in/tomb.v2"

	"akvorado/common/daemon"
	"akvorado/common/kafka"
	"akvorado/common/pb"
	"akvorado/common/reporter"
)

// Component represents the Kafka output.
type Component struct {
	r      *reporter.Reporter
	d      *Dependencies
	t      tomb.Tomb
	config Configuration

	kafkaOpts   []kgo.Opt
	kafkaTopic  string
	kafkaClient *kgo.Client
	errLogger   reporter.Logger
	metrics     metrics
}

// Dependencies define the dependencies of the Kafka output.
type Dependencies struct {
	Daemon daemon.Component
}

// New creates a new Kafka output component.
func New(r *reporter.Reporter, configuration Configuration, dependencies Dependencies) (*Component, error) {
	c := Component{
		r:          r,
		d:          &dependencies,
		config:     configuration,
		kafkaTopic: fmt.Sprintf("%s-v%d", configuration.Topic, pb.Version),
		errLogger:  r.Sample(reporter.BurstSampler(10*time.Second, 3)),
	}
	c.initMetrics()

	// Inert when disabled, so existing deployments are unaffected.
	if !configuration.Enabled {
		return &c, nil
	}

	kafkaOpts, err := kafka.NewConfig(r, configuration.Configuration)
	if err != nil {
		return nil, err
	}
	kafkaOpts = append(kafkaOpts,
		kgo.AllowAutoTopicCreation(),
		kgo.MaxBufferedRecords(configuration.QueueSize),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.RecordPartitioner(kgo.UniformBytesPartitioner(64<<20, true, true, nil)),
	)
	if err := kgo.ValidateOpts(kafkaOpts...); err != nil {
		return nil, fmt.Errorf("invalid Kafka configuration: %w", err)
	}
	c.kafkaOpts = kafkaOpts
	c.d.Daemon.Track(&c.t, "outlet/kafkaout")
	return &c, nil
}

// Enabled reports whether the Kafka output is active.
func (c *Component) Enabled() bool { return c.config.Enabled }

// Start starts the Kafka output component.
func (c *Component) Start() error {
	if !c.config.Enabled {
		return nil
	}
	c.r.Info().Msg("starting Kafka output component")

	kafkaMetrics := kprom.NewMetrics("")
	kafkaClient, err := kgo.NewClient(append(c.kafkaOpts, kgo.WithHooks(kafkaMetrics))...)
	if err != nil {
		return fmt.Errorf("unable to create Kafka client: %w", err)
	}
	c.r.RegisterMetricCollector(kafkaMetrics)
	c.kafkaClient = kafkaClient

	c.t.Go(func() error {
		<-c.t.Dying()
		kafkaClient.Close()
		return nil
	})
	return nil
}

// Stop stops the Kafka output component.
func (c *Component) Stop() error {
	if !c.config.Enabled {
		return nil
	}
	defer c.r.Info().Msg("Kafka output component stopped")
	c.r.Info().Msg("stopping Kafka output component")
	c.t.Kill(nil)
	return c.t.Wait()
}

// Send produces one enriched flow record to Kafka. Best-effort: errors are
// counted and logged but do not block the ClickHouse path. A production
// version would need an explicit delivery-guarantee policy.
func (c *Component) Send(key string, payload []byte) {
	if c.kafkaClient == nil {
		return
	}
	record := &kgo.Record{Topic: c.kafkaTopic, Key: []byte(key), Value: payload}
	c.kafkaClient.Produce(context.Background(), record, func(_ *kgo.Record, err error) {
		if err != nil {
			if ke, ok := err.(*kerr.Error); ok {
				c.metrics.errors.WithLabelValues(ke.Message).Inc()
			} else {
				c.metrics.errors.WithLabelValues("unknown").Inc()
			}
			c.errLogger.Err(err).Str("topic", c.kafkaTopic).Msg("Kafka producer error")
			return
		}
		c.metrics.messagesSent.Inc()
		c.metrics.bytesSent.Add(float64(len(payload)))
	})
}
