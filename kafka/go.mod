module github.com/soyacen/easypubsub/kafka

go 1.15

require (
	github.com/Shopify/sarama v1.29.1
	github.com/google/uuid v1.3.0
	github.com/soyacen/easypubsub v0.0.0
	github.com/soyacen/goutils/errorutils v0.0.0-20210722124525-e18c75bb2c2e
	github.com/soyacen/goutils/stringutils v0.0.0-20210722124525-e18c75bb2c2e
)

replace github.com/soyacen/easypubsub v0.0.0 => ../
