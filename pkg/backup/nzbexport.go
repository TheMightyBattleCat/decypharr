package backup

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"sort"
	"strings"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// BuildNZBXML writes a standard .nzb from a parsed record: one <file> per
// posted file, in record order, with every segment's message ID. Deleted
// files are left out. Subjects are the file names, since the record does
// not keep the original subject lines.
func BuildNZBXML(n *storage.NZB) []byte {
	var b bytes.Buffer
	b.WriteString(xml.Header)
	b.WriteString("<!DOCTYPE nzb PUBLIC \"-//newzBin//DTD NZB 1.1//EN\" \"http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd\">\n")
	b.WriteString("<nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">\n")
	if n.Name != "" || n.Category != "" {
		b.WriteString(" <head>\n")
		if n.Name != "" {
			fmt.Fprintf(&b, "  <meta type=\"name\">%s</meta>\n", xmlEscape(n.Name))
		}
		if n.Category != "" {
			fmt.Fprintf(&b, "  <meta type=\"category\">%s</meta>\n", xmlEscape(n.Category))
		}
		b.WriteString(" </head>\n")
	}
	date := n.DatePosted.Unix()
	if date < 0 {
		date = 0
	}
	for _, f := range n.Files {
		if f.IsDeleted || len(f.Segments) == 0 {
			continue
		}
		groups := f.Groups
		if len(groups) == 0 {
			groups = n.Groups
		}
		fmt.Fprintf(&b, " <file poster=\"\" date=\"%d\" subject=\"%s\">\n  <groups>\n", date, xmlEscape(f.Name))
		for _, g := range groups {
			fmt.Fprintf(&b, "   <group>%s</group>\n", xmlEscape(g))
		}
		b.WriteString("  </groups>\n  <segments>\n")
		segs := append([]storage.NZBSegment(nil), f.Segments...)
		sort.SliceStable(segs, func(i, j int) bool { return segs[i].Number < segs[j].Number })
		for _, s := range segs {
			id := strings.TrimSuffix(strings.TrimPrefix(s.MessageID, "<"), ">")
			fmt.Fprintf(&b, "   <segment bytes=\"%d\" number=\"%d\">%s</segment>\n", s.Bytes, s.Number, xmlEscape(id))
		}
		b.WriteString("  </segments>\n </file>\n")
	}
	b.WriteString("</nzb>\n")
	return b.Bytes()
}

func xmlEscape(s string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
