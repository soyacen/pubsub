module github.com/soyacen/easypubsub/amqp

go 1.15

require (
	github.com/Shopify/sarama v1.29.1
	github.com/hashicorp/go-multierror v1.1.1
	github.com/soyacen/easypubsub v0.0.0
	github.com/soyacen/goutils/errorutils v0.0.0-20210722124525-e18c75bb2c2e
	github.com/soyacen/goutils/stringutils v0.0.0-20210722124525-e18c75bb2c2e
	github.com/streadway/amqp v1.0.0
)

replace github.com/soyacen/easypubsub v0.0.0 => ../
