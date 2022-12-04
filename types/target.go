package types

type Paper struct {
	Id         string   `json:"id"`
	Title      string   `json:"title"`
	Abstract   string   `json:"abstract"`
	References []string `json:"references"`
	Relateds   []string `json:"relateds"`
	Authors    []struct {
		Id   string `json:"id"`
		Name string `json:"name"`
		Org  string `json:"org"`
	} `json:"authors"`
	Doi       string   `json:"doi"`
	Keywords  []string `json:"keywords"`
	NCitation int64    `json:"n_citation"`
	Url       []string `json:"url"`
	Venue     string   `json:"venue"`
	Year      int64    `json:"year"`
	DocType   string   `json:"doc_type"`
	Isbn      string   `json:"isbn"`
	Issue     string   `json:"issue"`
	Issn      string   `json:"issn"`
	PageStart string   `json:"page_start"`
	PageEnd   string   `json:"page_end"`
	Volume    string   `json:"volume"`
}
type Scholar struct {
	Id         string `json:"id"`
	HIndex     int64  `json:"h_index"`
	NCitation  int64  `json:"n_citation"`
	NPubs      int64  `json:"n_pubs"`
	Name       string `json:"name"`
	Statistics []struct {
		Year      int64 `json:"year"`
		NCitation int64 `json:"n_citation"`
		NPubs     int64 `json:"n_pubs"`
	} `json:"statistics"`
	Orgs     []string `json:"orgs"`
	Position string   `json:"position"`
	Pubs     []struct {
		Id    string `json:"i"`
		Order int64  `json:"r"`
	} `json:"pubs"`
	Tags []struct {
		Name   string `json:"t"`
		Weight int64  `json:"w"`
	} `json:"tags"`
}

func (p *Paper) CheckValidation() bool {
	return p.Title != "" && p.Id != ""
}

func (p *Scholar) CheckValidation() bool {
	return p.Name != "" && p.Id != ""
}
