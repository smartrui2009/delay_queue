package delay_queue

import (
	"github.com/smartrui/queue_delay/cache"
	"strconv"
)

type BucketItem struct {
	timestamp int64
	jobId     string
}

func pushToBucket(key string, timestamp int64 , jobId string) error {

	_, err := cache.ExecRedisCommand("ZADD", key, timestamp, jobId)

	return err
}

func getJobFromBucket(key string) (*BucketItem, error){
	var (
		values interface{}
		err error
		timeStr string
	)
	if values, err = cache.ExecRedisCommand("ZRANGE",key, 0, 0 , "withscores"); err != nil {
		return nil ,err
	}

	//转化下
	var valuesBytes []interface{}
	valuesBytes = values.([]interface{})

	if len(valuesBytes) == 0 {
		return nil, nil
	}

	timeStr = string(valuesBytes[1].([]byte))

	bucketItem := &BucketItem{
		jobId:string(valuesBytes[0].([]byte)),
	}
	bucketItem.timestamp, _ = strconv.ParseInt(timeStr, 10, 64)

	return bucketItem, err
}

//从bucket删除
func removeFromBucket(key, jobId string) error {
	var (
		err error
	)
	if _, err = cache.ExecRedisCommand("ZREM", key, jobId); err != nil {
		return err
	}
	return nil
}