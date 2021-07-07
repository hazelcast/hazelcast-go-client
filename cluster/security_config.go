package cluster

type SecurityConfig struct {
	Credentials CredentialsConfig
}

func (c SecurityConfig) Clone() SecurityConfig {
	return c
}

func (c *SecurityConfig) Validate() error {
	return nil
}

type CredentialsConfig struct {
	Username string `json:",omitempty"`
	Password string `json:",omitempty"`
}

func (c CredentialsConfig) Clone() CredentialsConfig {
	return CredentialsConfig{
		Username: c.Username,
		Password: c.Password,
	}
}

func (c CredentialsConfig) Validate() error {
	return nil
}
