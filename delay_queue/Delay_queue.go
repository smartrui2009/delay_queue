package delay_queue

import (
	"errors"
	"fmt"
	"github.com/smartrui/queue_delay/config"
	"log"
	"time"

	"github.com/smartrui/queue_delay/cache"
)

var (
	times []*time.Ticker
	bucketNameChan <-chan string
)

func Init() {

	//初始化Redis
	cache.InitRedisPool()

	//初始化定时器
	initTimer()

	bucketNameChan = buildBucketName()

}

/*
添加一个队列到job_pool，同时放入到bucket中
 */
func PutJob(job Job) error {
 var (
		err error
  )
  if job.JobId == "" || job.Topic == "" || job.Delay < 0 || job.TTR < 0 {
  	return errors.New("无效的Job")
  }

  if err = job.Put(); err != nil {
  	return err
  }

  if err = pushToBucket(<-bucketNameChan,job.Delay, job.JobId); err != nil {
  	log.Printf("添加job到bucket失败,#job-%+v#%s", job, err.Error())
  	return err
  }


  return  nil
}

func PopJob(topics []string) (*Job , error) {
	var(
		job *Job
		jobId string
		err error
	)
	if jobId, err = blockPopFromReadyQueue(topics, config.G_Config.QueueBlockTimeout); err != nil {
		return nil,  err
	}

	//查下job
	job = &Job{JobId:jobId}
	if job, err  = job.Get(); err != nil {
		return nil, err
	}

	if job == nil {
		return nil, err
	}

	timestamp := time.Now().Unix() + job.TTR
	err = pushToBucket(<-bucketNameChan, timestamp, job.JobId)

	return job, err

}

func RemoveJob(jobId string ) error {
	var (
		job *Job
		err error
	)

	job = &Job{JobId:jobId}
	err = job.remove()

	return err
}



// Get 查询Job
func Get(jobId string) (*Job, error) {
	var (
		job *Job
		err error
	)

	job = &Job{JobId:jobId}
	if job, err =  job.Get(); err != nil{
		return job, err
	}

	// 消息不存在, 可能已被删除
	if job == nil {
		return nil, nil
	}

	return job, err
}



func buildBucketName() <- chan string{
	var (
		c chan string
	)
	c = make(chan string)
	go func(){
		i := 1
		for {
			c <- fmt.Sprintf(config.G_Config.BucketName, i)
			if i >= config.G_Config.BucketSize {
				i = 1
			} else {
				i = i+1
			}
		}
	}()

	return c
}

func initTimer() {
	times = make([]*time.Ticker, config.G_Config.BucketSize)
	var bucketName string

	for i := 0 ;i < config.G_Config.BucketSize; i++ {
		times[i] = time.NewTicker(1 * time.Second)
		bucketName = fmt.Sprintf(config.G_Config.BucketName, i+1)
		go tickRound(times[i], bucketName)
	}

}

func tickRound(timer *time.Ticker, bucketName string) {
	var (
		t   time.Time
	)
	for {
		select {
		   case t = <-timer.C:
			   tickTaskHandler(t, bucketName)
		}
	}
}

func  tickTaskHandler(t   time.Time,bucketName string) {
	for {
		var (
			bucketItem *BucketItem
			job *Job
			err        error
		)

		if bucketItem, err = getJobFromBucket(bucketName); err != nil {
			log.Println(err)
			return
		}

		if bucketItem == nil {
			return
		}

		if bucketItem.timestamp > t.Unix()  {
			return
		}

		//查下job
		job = &Job{JobId:bucketItem.jobId}
		if job, err  = job.Get(); err != nil {
			return
		}

		if job == nil {
			log.Println("job不存在了")
			removeFromBucket(bucketName, bucketItem.jobId)
			continue
		}

		if job.Delay > t.Unix() {
			removeFromBucket(bucketName, bucketItem.jobId)

			pushToBucket(<-bucketNameChan, job.Delay, job.JobId)

			continue
		}

		//放入准备队列中
		if err = pushToReadyQueue(job.Topic,job.JobId); err != nil {
			log.Println("放入到准备队列出错",err)
			continue
		}

		removeFromBucket(bucketName, bucketItem.jobId)

	}

}



func PushJob() {
	 j := Job{
		Topic:"order",
		JobId:"order2009121226",
		Delay:time.Now().Unix() + 120,
		TTR:2334,
		Body:"这是一条延迟消息",
	}

	PutJob(j)


}
