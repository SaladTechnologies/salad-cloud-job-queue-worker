package config

import (
	"github.com/caarlos0/env/v11"
)

// TODO: Add option as an env var to include metadata in HTTP request body.
type Config struct {
	LogLevel           string `env:"SALAD_LOG_LEVEL" envDefault:"error"`
	MetadataURI        string `env:"SALAD_METADATA_URI" envDefault:"http://169.254.169.254:80"`
	ServiceEndpoint    string `env:"SALAD_SERVICE_ENDPOINT" envDefault:"job-queue-worker-api.salad.com:443"`
	ServiceUseTLS      bool   `env:"SALAD_SERVICE_USE_TLS" envDefault:"true"`
	GrpcMaxMessageSize int    `env:"SALAD_GRPC_MAX_MESSAGE_SIZE" envDefault:"11534336"`
}

func NewConfigFromEnv() (Config, error) {
	config := Config{}
	if err := env.Parse(&config); err != nil {
		return Config{}, err
	}

	return config, nil
}
