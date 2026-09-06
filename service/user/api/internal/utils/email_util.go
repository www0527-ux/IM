package utils

import (
	"crypto/tls"
	"fmt"
	"infinitechatgo/service/user/api/internal/config"
	"mime"
	"net"
	"net/smtp"
	"strconv"
	"strings"
)

func SendEmailCode(
	emailConf config.EmailConf,
	targetEmail string,
	authCode string,
) error {
	address := net.JoinHostPort(
		emailConf.Host,
		strconv.Itoa(emailConf.Port),
	)
	tlsConfig := &tls.Config{
		ServerName: emailConf.Host,
		MinVersion: tls.VersionTLS12,
	}
	conn, err := tls.Dial("tcp", address, tlsConfig)
	if err != nil {
		return fmt.Errorf("连接SMTP服务器失败: %w", err)
	}
	defer conn.Close()
	client, err := smtp.NewClient(conn, emailConf.Host)
	if err != nil {
		return fmt.Errorf("创建SMTP客户端失败: %w", err)
	}
	defer client.Close()

	auth := smtp.PlainAuth(
		"",
		emailConf.Username,
		emailConf.Password,
		emailConf.Host,
	)

	if err = client.Auth(auth); err != nil {
		return fmt.Errorf("SMTP认证失败: %w", err)
	}

	if err = client.Mail(emailConf.Username); err != nil {
		return fmt.Errorf("设置发件人失败: %w", err)
	}

	if err = client.Rcpt(targetEmail); err != nil {
		return fmt.Errorf("设置收件人失败: %w", err)
	}

	writer, err := client.Data()
	if err != nil {
		return fmt.Errorf("获取邮件写入流失败: %w", err)
	}

	message := buildEmailMessage(
		emailConf,
		targetEmail,
		authCode,
	)

	if _, err = writer.Write([]byte(message)); err != nil {
		_ = writer.Close()
		return fmt.Errorf("写入邮件内容失败: %w", err)
	}

	if err = writer.Close(); err != nil {
		return fmt.Errorf("提交邮件内容失败: %w", err)
	}

	if err = client.Quit(); err != nil {
		return fmt.Errorf("结束SMTP会话失败: %w", err)
	}

	return nil
}

func buildEmailMessage(
	emailConf config.EmailConf,
	targetEmail string,
	authCode string,
) string {
	fromName := mime.QEncoding.Encode("UTF-8", emailConf.FromName)
	subject := mime.QEncoding.Encode("UTF-8", emailConf.Subject)

	body := fmt.Sprintf(
		"您的验证码为：%s（五分钟内有效）",
		authCode,
	)

	return strings.Join([]string{
		fmt.Sprintf(
			"From: %s <%s>",
			fromName,
			emailConf.Username,
		),
		"To: " + targetEmail,
		"Subject: " + subject,
		"MIME-Version: 1.0",
		`Content-Type: text/plain; charset="UTF-8"`,
		"Content-Transfer-Encoding: 8bit",
		"",
		body,
		"",
	}, "\r\n")
}
