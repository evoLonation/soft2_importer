package types

type SNArticle struct {
	Id  string `json:"id"`
	Url string `json:"url"`
	// 也可作为文献链接
	SameAs []string `json:"sameAs"`
	// 摘要
	Description string     `json:"description"`
	Author      []SNPerson `json:"author"`
	InLanguage  string     `json:"inLanguage"`
	//当name 为"doi"的时候value就是他的doi号
	ProductId []struct {
		Name  string   `json:"name"`
		Value []string `json:"value"`
	} `json:"productId"`
	Keywords []string `json:"keywords"`
	Citation []struct {
		// 参考文献的id，这个id有可能是其他文献的id也有可能是其他文献的url
		Id string `json:"id"`
	} `json:"citation"`
}
type SNPerson struct {
	Id string `json:"id"`
	//姓
	FamilyName string `json:"familyName"`
	//名
	GivenName string `json:"givenName"`
	//机构
	affiliation []struct {
		Name string `json:"name"`
	}
}
type SNOrganization struct {
	Id   string `json:"id"`
	Name string `json:"rdfs:label"`
}
