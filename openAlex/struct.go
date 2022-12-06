package openAlex

import (
	"soft2_importer/types"
	"strings"
)

type OAArticle struct {
	OAid         string `json:"id"`
	Doi          string `json:"doi"`
	Title        string `json:"title"`
	Year         int64  `json:"publication_year"`
	CitedByCount int64  `json:"cited_by_count"`
	HostVenue    struct {
		DisplayName string `json:"display_name"`
		Issn        string `json:"issn_l"`
		Url         string `json:"url"`
	} `json:"host_venue"`
	Type        string `json:"types"`
	Authorships []struct {
		Author       OAAuthor `json:"author"`
		Institutions []struct {
			DisplayName string `json:"display_name"`
		} `json:"institutions"`
	} `json:"authorships"`
	Biblio struct {
		Volume    string `json:"volume"`
		Issue     string `json:"issue"`
		FirstPage string `json:"first_page"`
		LastPage  string `json:"last_page"`
	} `json:"biblio"`
	Concepts []struct {
		DisplayName string `json:"display_name"`
	} `json:"concepts"`
	AlternateHostVenues []struct {
		Url string `json:"url"`
	} `json:"alternate_host_venues"`
	ReferencedWorks       []string         `json:"referenced_works"`
	RelatedWorks          []string         `json:"related_works"`
	AbstractInvertedIndex map[string][]int `json:"abstract_inverted_index"`
}
type OAAuthor struct {
	OAid        string `json:"id"`
	DisplayName string `json:"display_name"`
}
type OAScholar struct {
	OAAuthor
	WorksCount           int64 `json:"works_count"`
	CitedByCount         int64 `json:"cited_by_count"`
	LastKnownInstitution struct {
		DisplayName string `json:"display_name"`
	} `json:"last_known_institution"`
	Concepts []struct {
		DisplayName string  `json:"display_name"`
		Score       float64 `json:"score"`
	} `json:"x_concepts"`
	CountsByYear []struct {
		Year         int64 `json:"year"`
		WorksCount   int64 `json:"works_count"`
		CitedByCount int64 `json:"cited_by_count"`
	} `json:"counts_by_year"`
}

func (p *OAArticle) Parse() *types.Paper {
	paper := &types.Paper{
		Id:         getId(p.OAid),
		Title:      p.Title,
		Abstract:   p.getAbstract(),
		References: getIds(p.ReferencedWorks),
		Relateds:   getIds(p.RelatedWorks),
		Authors:    p.getAuthors(),
		Doi:        p.getDoi(),
		Keywords:   p.getKeywords(),
		NCitation:  p.CitedByCount,
		Url:        p.getUrls(),
		Venue:      p.HostVenue.DisplayName,
		Year:       p.Year,
		DocType:    p.getDocType(),
		Isbn:       "",
		Issue:      p.Biblio.Issue,
		Issn:       p.HostVenue.Issn,
		PageStart:  p.Biblio.FirstPage,
		PageEnd:    p.Biblio.LastPage,
		Volume:     p.Biblio.Volume,
	}
	return paper
}

func (p *OAArticle) getAbstract() string {
	abstractMp := p.AbstractInvertedIndex
	l := 512
	buf := make([]string, l)
	maxIndex := 0
	for word, indexs := range abstractMp {
		for _, index := range indexs {
			if index >= l {
				l = l * 2
				newbuf := make([]string, l)
				copy(newbuf, buf)
				buf = newbuf
			}
			buf[index] = word
			if index > maxIndex {
				maxIndex = index
			}
		}
	}
	return strings.Join(buf[:maxIndex+1], " ")
}

func (p *OAArticle) getAuthors() []struct {
	Id   string `json:"id"`
	Name string `json:"name"`
	Org  string `json:"org"`
} {
	ret := make([]struct {
		Id   string `json:"id"`
		Name string `json:"name"`
		Org  string `json:"org"`
	}, len(p.Authorships))
	for i, e := range p.Authorships {
		ret[i].Name = e.Author.DisplayName
		ret[i].Id = getId(e.Author.OAid)
		if len(e.Institutions) > 0 {
			ret[i].Org = e.Institutions[0].DisplayName
		}
	}
	return ret
}

func (p *OAArticle) getKeywords() []string {
	ret := make([]string, len(p.Concepts))
	for i, e := range p.Concepts {
		ret[i] = e.DisplayName
	}
	return ret
}

func (p *OAArticle) getUrls() []string {
	var ret []string
	if p.HostVenue.Url != "" {
		ret = append(ret, p.HostVenue.Url)
	}
	for _, e := range p.AlternateHostVenues {
		if e.Url != "" {
			ret = append(ret, e.Url)
		}
	}
	return ret
}

func (p *OAArticle) getDocType() string {
	targetTypes := []string{"book", "patent", "journal"}
	origin := strings.ToLower(p.Type)
	for _, target := range targetTypes {
		if strings.Contains(origin, target) {
			return target
		}
	}
	return targetTypes[0]
}

func (p *OAArticle) getDoi() string {
	_, after, _ := strings.Cut(p.Doi, "https://doi.org/")
	return after
}

func getId(OAid string) string {
	_, ret, _ := strings.Cut(OAid, "https://openalex.org/")
	return "OA:" + ret
}
func getIds(OAids []string) []string {
	ret := make([]string, len(OAids))
	for i, OAid := range OAids {
		ret[i] = getId(OAid)
	}
	return ret
}

func (p *OAScholar) Parse() *types.Scholar {
	scholar := &types.Scholar{
		Id:         getId(p.OAid),
		HIndex:     -1,
		NCitation:  p.CitedByCount,
		NPubs:      p.WorksCount,
		Name:       p.DisplayName,
		Statistics: p.getStatistics(),
		//todo 可能出现数组种的空字符串
		Orgs:     []string{p.LastKnownInstitution.DisplayName},
		Position: "",
		Pubs: []struct {
			Id    string `json:"i"`
			Order int64  `json:"r"`
		}{},
		Tags: p.getTags(),
	}
	return scholar
}

func (p *OAScholar) getStatistics() []struct {
	Year      int64 `json:"year"`
	NCitation int64 `json:"n_citation"`
	NPubs     int64 `json:"n_pubs"`
} {
	ret := make([]struct {
		Year      int64 `json:"year"`
		NCitation int64 `json:"n_citation"`
		NPubs     int64 `json:"n_pubs"`
	}, len(p.CountsByYear))
	for i, e := range p.CountsByYear {
		ret[i].NPubs = e.WorksCount
		ret[i].NCitation = e.CitedByCount
		ret[i].Year = e.Year
	}
	return ret
}

func (p *OAScholar) getTags() []struct {
	Name   string `json:"t"`
	Weight int64  `json:"w"`
} {
	ret := make([]struct {
		Name   string `json:"t"`
		Weight int64  `json:"w"`
	}, len(p.Concepts))
	for i, e := range p.Concepts {
		ret[i].Weight = int64(e.Score)
		ret[i].Name = e.DisplayName
	}
	return ret
}
