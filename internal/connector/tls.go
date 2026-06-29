package connector

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/longxiucai/connectest/internal/config"
)

// resolvePEM 自动判断输入是 PEM 内容还是文件路径，返回规范化后的 PEM 字节。
// PEM 内容会自动修复换行问题（空格/字面量 \n 等还原为标准换行符）。
func resolvePEM(input string) ([]byte, error) {
	trimmed := strings.TrimSpace(input)
	var raw []byte
	if strings.HasPrefix(trimmed, "-----BEGIN") {
		raw = []byte(trimmed)
	} else {
		data, err := os.ReadFile(trimmed)
		if err != nil {
			return nil, fmt.Errorf("读取文件 %s 失败: %w", trimmed, err)
		}
		raw = data
	}
	return []byte(normalizePEM(string(raw))), nil
}

// normalizePEM 修复粘贴导致的换行丢失问题。
func normalizePEM(s string) string {
	s = strings.Map(func(r rune) rune {
		switch r {
		case 0xFEFF, 0x200B, 0x200C, 0x200D, 0x00A0:
			return -1
		}
		return r
	}, s)

	s = strings.ReplaceAll(s, `\r\n`, "\n")
	s = strings.ReplaceAll(s, `\n`, "\n")
	s = strings.ReplaceAll(s, "\r", "")

	if strings.Count(s, "\n") >= 2 {
		return s
	}

	beginRe := regexp.MustCompile(`(-----BEGIN\s+[A-Z0-9 ]+?-----)\s*`)
	endRe := regexp.MustCompile(`\s*(-----END\s+[A-Z0-9 ]+?-----)`)

	s = beginRe.ReplaceAllString(s, "$1\n")
	s = endRe.ReplaceAllString(s, "\n$1\n")

	var result strings.Builder
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "-----") {
			result.WriteString(trimmed)
			result.WriteByte('\n')
		} else {
			clean := strings.ReplaceAll(trimmed, " ", "")
			for len(clean) > 0 {
				end := 76
				if end > len(clean) {
					end = len(clean)
				}
				result.WriteString(clean[:end])
				result.WriteByte('\n')
				clean = clean[end:]
			}
		}
	}
	return result.String()
}

// buildTLSConfig 根据配置构建 *tls.Config。
// 支持 CA 证书验证和 mTLS 双向认证。
// 如果未提供 CA 证书则跳过服务端验证。
func buildTLSConfig(cfg config.Config) *tls.Config {
	if !cfg.UseTLS {
		return nil
	}
	tlsCfg := &tls.Config{
		InsecureSkipVerify: true,
	}

	if cfg.CACert != "" {
		caCert, err := resolvePEM(cfg.CACert)
		if err == nil {
			caCertPool := x509.NewCertPool()
			if caCertPool.AppendCertsFromPEM(caCert) {
				tlsCfg.RootCAs = caCertPool
				tlsCfg.InsecureSkipVerify = false
			}
		}
	}

	if cfg.Cert != "" && cfg.Key != "" {
		certPEM, err := resolvePEM(cfg.Cert)
		if err == nil {
			keyPEM, err := resolvePEM(cfg.Key)
			if err == nil {
				if cert, err := tls.X509KeyPair(certPEM, keyPEM); err == nil {
					tlsCfg.Certificates = []tls.Certificate{cert}
				}
			}
		}
	}

	return tlsCfg
}
