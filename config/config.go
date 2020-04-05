package config

var (
	G_Config *Config
)

type Config struct {
	BindAddress       string
	BucketSize        int
	BucketName        string
	QueueName         string
	QueueBlockTimeout int
	Redis             RedisConfig
}



type RedisConfig struct {
	Host           string
	Db             int
	Password       string
	MaxIdle        int
	MaxActive      int
	ConnectTimeout int
	ReadTimeout    int
	WriteTimeout   int
}

func(c *Config) InitDefaultConfig() {

	c.BucketSize = 3
	c.BucketName = "job_bucket_%d"
	c.QueueName  = "job_queue_%s"
	c.QueueBlockTimeout = 30

	c.Redis.Host = "127.0.0.1:6379"
	c.Redis.Db = 0
	c.Redis.MaxIdle = 2
	c.Redis.MaxActive = 3
}

func InitConfig() {
	G_Config = &Config{}
	G_Config.InitDefaultConfig()
}