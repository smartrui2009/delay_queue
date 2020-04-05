package delay_queue

import (
	"encoding/json"
	"github.com/smartrui/queue_delay/cache"
)

/*
Topic：Job类型。可以理解成具体的业务名称。
Id：Job的唯一标识。用来检索和删除指定的Job信息。
Delay：Job需要延迟的时间。单位：秒。（服务端会将其转换为绝对时间）
TTR（time-to-run)：Job执行超时时间。单位：秒。
Body
*/
type Job struct {
	Topic string `json:"topic"`
	JobId string `json:"job_id"`
	Delay int64 `json:"delay"`
	TTR   int64 `json:"ttr"`
	Body string `json:"body"`
}

var (
	jobPrefix = "jobpool_"
)

//添加一个job
func (j *Job) Put() error {
	var (
		b []byte
		err error
	)
	b, err = json.Marshal(j)
	if err != nil {
		return err
	}
	_, err = cache.ExecRedisCommand("set", jobPrefix + j.JobId, string(b))

	return err
}

//获取job
func (j *Job) Get() (job *Job, err error){
	var (
		jsonvalue interface{}
	)
	jsonvalue, err = cache.ExecRedisCommand("get", jobPrefix + j.JobId)
	if err != nil {
		return nil ,err
	}
	job = &Job{}
	if err = json.Unmarshal(jsonvalue.([]byte), job); err != nil {
		return nil ,err
	}

	return job,nil
}

//删除job
func(j *Job) remove() error {
	_, err := cache.ExecRedisCommand("DEL", j.JobId)

	return err
}