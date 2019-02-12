// Copyright 2014 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/pubsub"
)

var (
	topic        string
	subscription string
	projectid    string
)

func main() {
	// Making a blocking chan here just to prevent the program from exiting
	done := make(chan bool)

	// Register stats and trace exporters to export
	// the collected data.
	//view.RegisterExporter(&exporter.PrintExporter{})
	// Register the view to collect gRPC client stats.
	// if err := view.Register(ocgrpc.DefaultClientViews...); err != nil {
	// 	log.Fatal(err)
	// }

	// Future Add tracing to measture some backend status
	// grpcDialOption := grpc.WithStatsHandler(&ocgrpc.ClientHandler{})
	// opt := option.WithGRPCDialOption(grpcDialOption)

	// Out Commandline parameters
	flag.StringVar(&subscription, "subscription", "", "optional subscription name to listen on")
	flag.StringVar(&topic, "topic", "", "Please provide the topic name! (required)")
	flag.StringVar(&projectid, "projectid", "", "Please provide the GCP projectid hosting the topic and optional subscription")
	flag.Parse()
	if topic == "" {
		fmt.Println("The pubsub topic must be provided")
		os.Exit(1)
	}

	// Build a context.  If you are running this on a local machine you will need
	// to run gcloud auth application-default login.  Otherwise you will need
	// to use a json key and set the envirometn variable something like
	//export GOOGLE_APPLICATION_CREDENTIALS="/home/user/Downloads/[FILE_NAME].json"
	ctx := context.Background()

	// Build the connection Client
	psClient, err := pubsub.NewClient(ctx, projectid)
	if err != nil {
		fmt.Printf("Pubsub Client creation error:%v\n", err.Error())
	}
	// Mark time in the log so we can see when the connection was set up.
	// You might not see any network traffic until the first meessage is created
	log.Printf("Client connection built now!")
	defer psClient.Close()
	t := psClient.Topic(topic)

	// Publish a message every 5 seconds.
	// This goroutine will exit when the
	// Program exists
	go func(ctx context.Context, pt *pubsub.Topic) {
		ticker := time.NewTicker(time.Second * 5)

		for t := range ticker.C {
			m := pubsub.Message{
				Data: []byte(strconv.FormatInt(t.UnixNano(), 10)),
			}
			res := pt.Publish(ctx, &m)
			<-res.Ready()
			_, err := res.Get(ctx)
			if err != nil {
				log.Printf("Pubsub Message Publish Failed with error:%v", err.Error())
			} else {
				log.Printf("Published Message:%v", string(m.Data))
			}

		}

	}(ctx, t)
	// If you provided a subscription at the commandline we attempt to monitor here
	if subscription != "" {
		sub := psClient.Subscription(subscription)
		err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
			mt, err := strconv.ParseInt(string(m.Data), 10, 64)
			if err != nil {
				fmt.Println("Can't convert message body to an int")
			}
			log.Printf("Recieved Message: %s in %s", m.Data, time.Since(time.Unix(0, mt)))
			m.Ack()
		})
		if err != nil {
			log.Printf("Subscription Error: %s", err.Error())
		}
	}

	<-done
	t.Stop()
}

//ups-troubleshooting
//Topic_HPS
