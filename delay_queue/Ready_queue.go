package delay_queue

import (
	"fmt"
	"github.com/smartrui/queue_delay/cache"
	"github.com/smartrui/queue_delay/config"
)

// 添加JobId到队列中
func pushToReadyQueue(queueName string, jobId string) error {
	queueName = fmt.Sprintf(config.G_Config.QueueName, queueName)
	_, err := cache.ExecRedisCommand("RPUSH", queueName, jobId)

	return err
}

// 从队列中阻塞获取JobId
func blockPopFromReadyQueue(queues []string, timeout int) (string, error) {
	var args []interface{}
	for _, queue := range queues {
		args = append(args, queue)
	}
	args = append(args, timeout)

	var (
		value interface{}
		valuesBytes []interface{}
		err error
	)
	if value, err = cache.ExecRedisCommand("BLPOP", args...); err != nil {
		return "", err
	}

	valuesBytes = value.([]interface{})

	if len(valuesBytes) == 0 {
		return "", nil
	}

	elements := string(valuesBytes[1].([]byte))

	return elements, nil
}