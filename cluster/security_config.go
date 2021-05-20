package cluster

type SecurityConfig struct {
	Username string
	Password string
}

func NewSecurityConfig() SecurityConfig {
	return SecurityConfig{}
}

func (c SecurityConfig) Clone() SecurityConfig {
	return SecurityConfig{
		Username: c.Username,
		Password: c.Password,
	}
}

func (c SecurityConfig) Validate() error {
	return nil
}
