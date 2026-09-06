package dto

type PageRequest struct {
	PageNum  int32 `json:"pageNum" form:"pageNum"`
	PageSize int32 `json:"pageSize" form:"pageSize"`
}

// 返回实际使用的分页参数给前端
type PageResponse[T any] struct {
	List        []T   `json:"list"`
	Total       int64 `json:"total"`
	PageSize    int64 `json:"pageSize"`
	PageNum     int64 `json:"pageNum"`
	Pages       int64 `json:"pages"`
	HasNext     bool  `json:"hasNext"`
	HasPrevious bool  `json:"hasPrevious"`
}
//page means the total num of pages
func NewPageResponse[T any](
	list []T,
	total int64,
	pageNum int,
	pageSize int,
) *PageResponse[T] {
	pages := int64(0)

	if pageSize > 0 {
		pages = (total + int64(pageSize) - 1) / int64(pageSize)
	}

	if list == nil {
		list = []T{}
	}

	return &PageResponse[T]{
		List:        list,
		Total:       total,
		PageSize:    int64(pageSize),
		PageNum:     int64(pageNum),
		Pages:       pages,
		HasNext:     int64(pageNum) < pages,
		HasPrevious: pageNum > 1 && pages > 0,
	}
}
//select data by pageNum
func NewPageResponseFromAll[T any](
	allData []T,
	pageNum int,
	pageSize int,
)*PageResponse[T]{
	if pageNum<1{
		pageNum=1
	}
	if pageSize<1 {
		pageSize=10
	}
	total:=int64(len(allData))
	fromIndex:=(pageNum-1)*pageSize
	if fromIndex >= len(allData) {
		return NewPageResponse(
			[]T{},
			total,
			pageNum,
			pageSize,
		)
	}
	toIndex := fromIndex + pageSize
	if toIndex > len(allData) {
		toIndex = len(allData)
	}

	list := allData[fromIndex:toIndex]

	return NewPageResponse(
		list,
		total,
		pageNum,
		pageSize,
	)
}