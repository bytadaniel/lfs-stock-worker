package main

import (
	"encoding/json"
	"io"
	// "log"
	"net/http"
	"time"
	"strings"
)

type ParseStocksData struct {
	Articles []string
	XInfo	 string
}

type SizeStock struct {
	Wh 	int		`json:"wh"`
	Qty int		`json:"qty"`
}

type NmSize struct {
	Name 		string 		`json:"name"`
	OrigName 	string 		`json:"origName"`
	Rank 		int 		`json:"rank"`
	OptionId 	int 		`json:"optionId"`
	Stocks 		[]SizeStock `json:"stocks"`
}

type NmProduct struct {
	Id 	  		int			`json:"id"`
	Sizes 		[]NmSize 	`json:"sizes"`
	BrandId 	int			`json:"brandId"`
	Brand		string		`json:"brand"`
	SubjectId	int			`json:"subjectId"`
	Name		string 		`json:"name"`
	SupplierId 	int			`json:"supplierId"`
	PriceU 		int 		`json:"priceU"`
	SalePriceU 	int 		`json:"salePriceU"`
}

type NmCatalogReponse struct {
	State 	int 				`json:"state"`
	Data 	struct {
		Products []NmProduct	`json:"products"`
	} 							`json:"data"`
}

type ArticleStocksRow struct {
	DateTime		string			`json:"dateTime"`
	Article			int				`json:"article"`
	OptionId 		int				`json:"optionId"`
	SizeOrigName	string			`json:"sizeOrigName"`
	BrandId 		int				`json:"brandId"`
	BrandName 		string			`json:"brandName"`
	SubjectId 		int				`json:"subjectId"`
	Name 			string			`json:"name"`
	SupplierId 		int				`json:"supplierId"`
	PriceU 			int				`json:"priceU"`
	SalePriceU 		int				`json:"salePriceU"`
	StocksSum 		int				`json:"stocksSum"`
	Stocks 			map[int]int		`json:"stocks"`
}

func ParseStocksHandler(rabbit RabbitWrapper, p MessageData) {
	// log.Printf("Start processing task parse_stocks. input: %v", p)
	reqUrl := "https://wbxcatalog-ru.wildberries.ru/nm-2-card/catalog?" + p.XInfo + "&nm=" + strings.Join(p.Articles, ";")//"13362963"

	response, _ := http.Get(reqUrl)
	body, _ := io.ReadAll(response.Body)

	nmCatalog := NmCatalogReponse{}
	json.Unmarshal(body, &nmCatalog)

	for _, product := range nmCatalog.Data.Products {
		for _, size := range product.Sizes {
			stocks := map[int]int{}
			stocksSum := 0
			for _, stock := range size.Stocks {
				stocks[stock.Wh] = stock.Qty
				stocksSum += stock.Qty
			}

			row := &ArticleStocksRow{
				DateTime:		time.Now().Format("2006-01-02 15:04:05"),
				Article:		product.Id,
				OptionId:		size.OptionId,
				SizeOrigName:	size.OrigName,
				BrandId:		product.BrandId,
				BrandName: 		product.Brand,
				SubjectId: 		product.SubjectId,
				Name: 			product.Name,
				SupplierId: 	product.SupplierId,
				PriceU: 		product.PriceU,
				SalePriceU: 	product.SalePriceU,
				StocksSum: 		stocksSum,
				Stocks: 		stocks,
			}

			if row.SubjectId == 0 {
				row.SubjectId = -1
			}

			if row.PriceU == 0 {
				row.PriceU = -1
			}

			if row.SalePriceU == 0 {
				row.SalePriceU = -1
			}

			rabbit.SendMessage("go_queue_2", "cache_clickhouse_row", MessageData{
				Table: "article_stocks",
				Row: row,
			})
		}


	}

}
