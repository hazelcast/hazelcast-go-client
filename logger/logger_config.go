package logger

type Config struct {
	Level Level
}

func (c Config) Clone() Config {
	return Config{Level: c.Level}
}
