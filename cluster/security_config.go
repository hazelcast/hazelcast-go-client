package cluster

type SecurityConfig struct {
	Username string `json:",omitempty"`
	Password string `json:",omitempty"`
}

func (c SecurityConfig) Clone() SecurityConfig {
	return SecurityConfig{
		Username: c.Username,
		Password: c.Password,
	}
}

func (c *SecurityConfig) Validate() error {
	return nil
}
