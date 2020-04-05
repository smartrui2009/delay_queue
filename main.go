package main

import (
	"github.com/smartrui/queue_delay/config"
	"github.com/smartrui/queue_delay/delay_queue"

)


func  main() {
	//初始化配置
	config.InitConfig()

	//初始化队队操作
	delay_queue.Init()


	//delay_queue.PushJob()
	//delay_queue.PopJob([]string{"topic1","topic2"})
	select{}

}
