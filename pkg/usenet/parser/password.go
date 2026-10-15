package parser

import (
	"bytes"
	"encoding/xml"
	"errors"
	"regexp"
	"strings"

	"golang.org/x/net/html/charset"
)

// ErrArchivePassword reports an archive whose data is encrypted and that no
// known password opens. It never means the posting is dead: the same NZB
// imports once the right password is supplied, so it must stay out of the
// dead-posting cache.
var ErrArchivePassword = errors.New("archive is encrypted and no known password opens it")

// namePasswordPattern matches SABnzbd's "Name{{password}}.nzb" convention.
var namePasswordPattern = regexp.MustCompile(`\{\{(.+?)\}\}`)

// SplitNamePassword removes a SABnzbd-style "{{password}}" from an NZB name
// and returns the cleaned name and the password. A name without one is
// returned unchanged with an empty password.
func SplitNamePassword(name string) (string, string) {
	loc := namePasswordPattern.FindAllStringSubmatchIndex(name, -1)
	if len(loc) == 0 {
		return name, ""
	}
	last := loc[len(loc)-1]
	password := name[last[2]:last[3]]
	clean := strings.TrimSpace(name[:last[0]]) + name[last[1]:]
	return strings.TrimSpace(clean), password
}

// MovePasswordIntoNZB moves a "{{password}}" in name, and any extra
// passwords, into the NZB as <meta type="password"> entries, so every later
// parse of the stored NZB - restarts, re-claims, backfills - sees them. The
// returned name has the password removed: it names folders and is matched by
// the Arrs, and must not carry a secret into logs.
func MovePasswordIntoNZB(name string, content []byte, extra ...string) (string, []byte) {
	name, fromName := SplitNamePassword(name)
	for _, pw := range append([]string{fromName}, extra...) {
		content = WithPasswordMeta(content, pw)
	}
	return name, content
}

var (
	headOpenPattern  = regexp.MustCompile(`<head\b[^>]*>`)
	headEmptyPattern = regexp.MustCompile(`<head\b[^>]*/>`)
	nzbOpenPattern   = regexp.MustCompile(`<nzb\b[^>]*>`)
)

// WithPasswordMeta returns content with password listed as a
// <meta type="password"> in its <head>, adding the head when there is none.
// Content that already lists the password, or that has no <nzb> element, is
// returned unchanged.
func WithPasswordMeta(content []byte, password string) []byte {
	password = strings.TrimSpace(password)
	if password == "" || len(content) == 0 {
		return content
	}
	for _, have := range headPasswords(content) {
		if have == password {
			return content
		}
	}
	var escaped bytes.Buffer
	_ = xml.EscapeText(&escaped, []byte(password))
	meta := `<meta type="password">` + escaped.String() + `</meta>`

	// Only the document's start is searched: a subject can quote "<head>".
	limit := min(len(content), 64<<10)
	if first := bytes.Index(content[:limit], []byte("<file")); first >= 0 {
		limit = first
	}
	start := content[:limit]
	splice := func(at int, insert string, drop int) []byte {
		out := make([]byte, 0, len(content)+len(insert))
		out = append(out, content[:at]...)
		out = append(out, insert...)
		return append(out, content[at+drop:]...)
	}
	if loc := headEmptyPattern.FindIndex(start); loc != nil {
		return splice(loc[0], "<head>"+meta+"</head>", loc[1]-loc[0])
	}
	// Last in the head: the NZB's own passwords are tried first.
	if at := bytes.Index(start, []byte("</head>")); at >= 0 {
		return splice(at, meta, 0)
	}
	if loc := headOpenPattern.FindIndex(start); loc != nil {
		return splice(loc[1], meta, 0)
	}
	if loc := nzbOpenPattern.FindIndex(start); loc != nil {
		return splice(loc[1], "<head>"+meta+"</head>", 0)
	}
	return content
}

// headPasswords returns the password metas in an NZB's head, stopping at
// the first <file>.
func headPasswords(content []byte) []string {
	decoder := xml.NewDecoder(bytes.NewReader(content))
	decoder.CharsetReader = charset.NewReaderLabel
	decoder.Strict = false
	var passwords []string
	for {
		tok, err := decoder.Token()
		if err != nil { // io.EOF or a malformed document: what was read stands
			return passwords
		}
		start, ok := tok.(xml.StartElement)
		if !ok {
			continue
		}
		switch start.Name.Local {
		case "file":
			return passwords
		case "meta":
			var md nzbMeta
			if decoder.DecodeElement(&md, &start) == nil && md.isPassword() {
				passwords = append(passwords, md.value())
			}
		}
	}
}

// nzbMeta is one <meta> of an NZB head.
type nzbMeta struct {
	Type  string `xml:"type,attr"`
	Value string `xml:",chardata"`
}

func (m nzbMeta) isPassword() bool {
	return strings.EqualFold(strings.TrimSpace(m.Type), "password") && m.value() != ""
}

func (m nzbMeta) value() string { return strings.TrimSpace(m.Value) }

// passwordCandidates joins password lists in order, dropping blanks and
// repeats.
func passwordCandidates(lists ...[]string) []string {
	var out []string
	seen := make(map[string]bool)
	for _, list := range lists {
		for _, pw := range list {
			pw = strings.TrimSpace(pw)
			if pw == "" || seen[pw] {
				continue
			}
			seen[pw] = true
			out = append(out, pw)
		}
	}
	return out
}
