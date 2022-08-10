package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
	"github.com/fujiwara/logutils"
	"github.com/ken39arg/go-flagx"
	"github.com/mashiike/sqpulser"
)

var (
	Version string
)

func main() {
	filter := &logutils.LevelFilter{
		Levels: []logutils.LogLevel{"debug", "info", "notice", "warn", "error"},
		ModifierFuncs: []logutils.ModifierFunc{
			logutils.Color(color.FgHiBlack),
			nil,
			logutils.Color(color.FgHiBlue),
			logutils.Color(color.FgYellow),
			logutils.Color(color.FgRed, color.BgBlack),
		},
		MinLevel: "info",
		Writer:   os.Stderr,
	}
	log.SetOutput(filter)
	var (
		inQueueURL   string
		outQueueURL  string
		inQueueName  string
		outQueueName string
		minLevel     string
		emitInterval string
		offset       string
	)
	flag.CommandLine.Usage = func() {
		fmt.Fprintln(flag.CommandLine.Output(), "sqpulser is a tool for compiling SQS messages and emitting them in a pulsatile cycle")
		fmt.Fprintln(flag.CommandLine.Output(), "version:", Version)
		flag.CommandLine.PrintDefaults()
	}
	flag.StringVar(&inQueueURL, "in-queue-url", "", "Incoming SQS queue URL")
	flag.StringVar(&outQueueURL, "out-queue-url", "", "Outgoing SQS queue URL")
	flag.StringVar(&inQueueName, "in", "", "Incoming SQS queue Name")
	flag.StringVar(&outQueueName, "out", "", "Outgoing SQS queue Name")
	flag.StringVar(&minLevel, "log-level", "info", "awstee log level")
	flag.StringVar(&emitInterval, "emit-interval", "15m", "sqs message emit interval")
	flag.StringVar(&offset, "offset", "0m", "sqs message emit offset")
	flag.VisitAll(flagx.EnvToFlagWithPrefix("SQPULSER_"))
	flag.Parse()
	filter.SetMinLevel(logutils.LogLevel(strings.ToLower(minLevel)))

	i, err := time.ParseDuration(emitInterval)
	if err != nil {
		log.Fatalln("[error] -emit-interval parse failed", err)
	}
	o, err := time.ParseDuration(offset)
	if err != nil {
		log.Fatalln("[error] -offset parse failed", err)
	}
	opt := &sqpulser.Option{
		IncomingQueueURL:  inQueueURL,
		OutgoingQueueURL:  outQueueURL,
		IncomingQueueName: inQueueName,
		OutgoingQueueName: outQueueName,
		EmitInterval:      i,
		Offset:            o,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	app, err := sqpulser.New(ctx, opt)
	if err != nil {
		log.Fatalln("[error]", err)
	}
	if err := app.Run(ctx); err != nil {
		log.Fatalln("[error]", err)
	}
}
