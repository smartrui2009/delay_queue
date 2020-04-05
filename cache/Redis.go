package cache

import (
	"github.com/garyburd/redigo/redis"
	"github.com/smartrui/queue_delay/config"
	"log"
	"time"
)


var (
	// RedisPool RedisPool连接池实例
	G_RedisPool *redis.Pool
)

// 初始化连接池
func InitRedisPool(){
	G_RedisPool  = &redis.Pool{
		MaxIdle:      config.G_Config.Redis.MaxIdle,
		MaxActive:    config.G_Config.Redis.MaxActive,
		IdleTimeout:  300 * time.Second,
		Dial:         redisDial,
		TestOnBorrow: redisTestOnBorrow,
		Wait:         true,
	}


}

// 连接redis
func redisDial() (redis.Conn, error) {
	conn, err := redis.Dial(
		"tcp",
		config.G_Config.Redis.Host,
		redis.DialConnectTimeout(time.Duration(config.G_Config.Redis.ConnectTimeout)*time.Millisecond),
		redis.DialReadTimeout(time.Duration(config.G_Config.Redis.ReadTimeout)*time.Millisecond),
		redis.DialWriteTimeout(time.Duration(config.G_Config.Redis.WriteTimeout)*time.Millisecond),
	)
	if err != nil {
		log.Printf("连接redis失败#%s", err.Error())
		return nil, err
	}

	if config.G_Config.Redis.Password != "" {
		if _, err := conn.Do("AUTH", config.G_Config.Redis.Password); err != nil {
			conn.Close()
			log.Printf("redis认证失败#%s", err.Error())
			return nil, err
		}
	}

	_, err = conn.Do("SELECT", config.G_Config.Redis.Db)
	if err != nil {
		conn.Close()
		log.Printf("redis选择数据库失败#%s", err.Error())
		return nil, err
	}

	return conn, nil
}

// 从池中取出连接后，判断连接是否有效
func redisTestOnBorrow(conn redis.Conn, t time.Time) error {
	_, err := conn.Do("PING")
	if err != nil {
		log.Printf("从redis连接池取出的连接无效#%s", err.Error())
	}

	return err
}

// 执行redis命令, 执行完成后连接自动放回连接池
func ExecRedisCommand(command string, args ...interface{}) (interface{}, error) {
	redis := G_RedisPool.Get()
	defer redis.Close()

	return redis.Do(command, args...)
}