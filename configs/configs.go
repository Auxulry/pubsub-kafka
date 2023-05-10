package configs

import "github.com/spf13/viper"

type Config struct {
	KafkaUser     string `mapstructure:"KAFKA_USER"`
	KafkaPassword string `mapstructure:"KAFKA_PASSWORD"`
}

func LoadConfig(path string) (config Config, err error) {
	viper.AddConfigPath(path)
	viper.SetConfigFile(".env")
	viper.SetConfigType("env")

	viper.AutomaticEnv()
	viper.WatchConfig()

	err = viper.ReadInConfig()
	if err != nil {
		return
	}

	err = viper.Unmarshal(&config)
	return
}