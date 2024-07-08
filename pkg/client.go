package pkg

type Client struct{}

func (c *Client) Init(master string, loadedConfig Config) {
	masterNode = master
	config = &loadedConfig
}
