package version

import "fmt"

type Info struct {
	Version string `json:"version"`
	Channel string `json:"channel"`
	// Commit is the git commit the binary was built from, when the build
	// stamped it (scripts/deploy-remote.sh does).
	Commit string `json:"commit,omitempty"`
}

func (i Info) String() string {
	return fmt.Sprintf("%s-%s", i.Version, i.Channel)
}

var (
	Version = ""
	Channel = ""
	Commit  = ""
)

func GetInfo() Info {
	return Info{
		Version: Version,
		Channel: Channel,
		Commit:  Commit,
	}
}
