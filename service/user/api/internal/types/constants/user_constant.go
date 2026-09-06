package constants

import "regexp"

var (
	//“编译”失败就panic
	PhoneRegex = regexp.MustCompile(`^1[3-9]\d{9}$`)
	EmailRegex = regexp.MustCompile(
		`^[a-zA-Z0-9_+&*-]+(?:\.[a-zA-Z0-9_+&*-]+)*@(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}$`,
	)
)

const (
	LoginCodeEmailPrefix     = "login_code_email:"
	LoginCodePhonePrefix     = "login_code_phone:"
	PasswordUpdateSuccessMsg = "密码修改成功"
)
