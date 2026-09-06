package common

import (
	"net/http"
	"reflect"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/zeromicro/go-zero/rest/httpx"
)

//校验器
// 输入：某个结构体
// 读取：结构体字段上的validate标签
// 输出：nil或者校验错误
var validatorEngine = validator.New()

//创建符合接口的结构体
type customValidator struct{}

//init在包加载时自动执行
func init() {
	//导入校验器
	httpx.SetValidator(&customValidator{})
}
func (c *customValidator)Validate(
	_ *http.Request,
	data any,
)error{
	err:=validatorEngine.Struct(data)
	if err==nil{
		return nil
	}
	//一组字段错误
	validationErrors,ok:=err.(validator.ValidationErrors)
	//防止Struct里面传入的不是可以校验的结构体
	if !ok{
		return SystemError
	}
	//使用反射找到对应的msg
	dataType := reflect.TypeOf(data)
	//判断是否为指针
	if dataType.Kind() == reflect.Ptr {
		dataType = dataType.Elem()
	}
	for _, validationErr := range validationErrors {
		field, ok := dataType.FieldByName(validationErr.Field())
		if !ok {
			continue
		}

		message := parseValidationMessage(
			field.Tag.Get("msg"),//msg标签信息
			validationErr.Tag(),//如min
		)
		if message != "" {
			return ErrorCode{
				Code:    ParamsError.Code,
				Message: message,
			}
		}
	}

	return ParamsError
}
func parseValidationMessage(msg string, failedTag string) string {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return ""
	}

	for _, part := range strings.Split(msg, ";") {
		part = strings.TrimSpace(part)

		tag, message, found := strings.Cut(part, "=")
		if !found {
			continue
		}

		if strings.TrimSpace(tag) == failedTag {
			return strings.TrimSpace(message)
		}
	}

	// 兼容只有一句普通提示的写法
	if !strings.Contains(msg, "=") {
		return msg
	}

	return ""
}