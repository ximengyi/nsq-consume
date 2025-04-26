package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
)

// 消费者配置结构体
type ConsumerConfig struct {
	NSQDAddress string
	Topics      []string
	Channel     string
	Workers     int
}

// 自定义的 NSQ 消息处理器
type MessageHandler struct{}

// 处理 NSQ 消息的方法
func (h *MessageHandler) HandleMessage(m *nsq.Message) error {
	fmt.Printf("Received message: %s\n", string(m.Body))
	m.Finish()
	return nil
}

// 启动消费者协程
func startConsumer(config ConsumerConfig, wg *sync.WaitGroup) {
	for _, topic := range config.Topics {
		for i := 0; i < config.Workers; i++ {
			wg.Add(1)
			go func(t string) {
				defer wg.Done()
				cfg := nsq.NewConfig()
				consumer, err := nsq.NewConsumer(t, config.Channel, cfg)
				if err != nil {
					log.Fatal(err)
				}

				consumer.AddHandler(&MessageHandler{})

				err = consumer.ConnectToNSQD(config.NSQDAddress)
				if err != nil {
					log.Fatal(err)
				}

				<-consumer.StopChan
			}(topic)
		}
	}
}

// 监控协程
func monitorWorkers(config ConsumerConfig, wg *sync.WaitGroup) {
	for {
		time.Sleep(5 * time.Second)
		// 这里可以添加逻辑来检查协程数量，当前示例仅打印监控信息
		fmt.Println("Monitoring workers...")
	}
}

func main() {
	config := ConsumerConfig{
		NSQDAddress: "192.168.71.25:4150",
		Topics:      []string{"sightseeing_bus_device_csm_24"},
		Channel:     "sightseeing_bus_chan_24",
		Workers:     1,
	}

	var wg sync.WaitGroup

	// 启动消费者协程
	startConsumer(config, &wg)

	// 启动监控协程
	go monitorWorkers(config, &wg)

	// 等待所有协程完成
	wg.Wait()
}
