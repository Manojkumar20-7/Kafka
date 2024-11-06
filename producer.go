package main

import (
	"fmt"
	"math/rand"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func producer(){
	p,err:=kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers":"localhost:40977",
	})
	if err!=nil{
		fmt.Printf("Failed to create producer: %s\n",err)
		return
	}
	go func(){
		for e:=range p.Events(){
			switch ev:=e.(type){
			case *kafka.Message:
				if ev.TopicPartition.Error!=nil{
					fmt.Printf("Failed to deliver message: %v",ev.TopicPartition)
				}else{
					fmt.Printf("Produced event to topic %s: key= %-10s value= %s\n",*ev.TopicPartition.Topic,string(ev.Key),string(ev.Value))
				}
			}
		}
	}()
	
	users:=[...]string{"Manoj","Mouli","Naresh","Vignesh"}
	items:=[...]string{"book","watch","laptop","mobile"}
	topic:="purchases"

	for n:=0;n<10;n++{
		key:=users[rand.Intn(len(users))]
		data:=items[rand.Intn(len(items))]
		msg:=&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic,Partition: kafka.PartitionAny},
			Key: []byte(key),
			Value: []byte(data),
		}
		p.Produce(msg,nil)
	}
	fmt.Println(p.String())
	p.Flush(15 * 1000)
	p.Close()
}