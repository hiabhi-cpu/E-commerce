package main

type Order struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Item       string  `json:"item"`
	Quantity   int     `json:"quantity"`
	Amount     float64 `json:"amount"`
}
