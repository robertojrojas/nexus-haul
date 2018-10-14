package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

type Data struct {
	Asset Asset `json:"data"`
}

type Asset struct {
	Type              string  `json:"type"`
	Leaf              bool    `json:"leaf"`
	NodeName          string  `json:"nodeName"`
	Path              string  `json:"path"`
	Children          []Asset `json:"children"`
	RepositoryId      string  `json:"repositoryId"`
	LocallyAvailable  bool    `json:"locallyAvailable"`
	ArtifactTimestamp int     `json:"artifactTimestamp"`
	ArtifactUri       string  `json:"artifactUri"`
	PomUri            string  `json:"pomUri"`
	GroupId           string  `json:"groupId"`
	ArtifactId        string  `json:"artifactId"`
	Version           string  `json:"version"`
	Extension         string  `json:"extension"`
	Packaing          string  `json:"packaging"`
}

type AssetToStream struct {
	sourceURL   string
	targetURL   string
	contentType string
}

type configFile struct {
	SourceURL         string
	TargetURL         string
	SourceDownloadURL string
	Workers           int
}

type authFile struct {
	SourceUser     string
	SourcePassword string
	TargetUser     string
	TargetPassword string
}

type confInfo struct {
	configFile configFile
	authFile   authFile
}

var nexusMigratorConfigFile string
var nexusMigratorAuthFile string

var af authFile
var cf configFile
var ci confInfo

func init() {
	flag.StringVar(&nexusMigratorConfigFile, "migratorConfFile", "./migrator-conf.json", "File (JSON) containing configuration for the Nexus Artifact Migrator.")
	flag.StringVar(&nexusMigratorAuthFile, "migratorAuthFile", "./migrator-auth.json", "File (JSON) containing authentication for the Nexus servers accessed by the Artifact Migrator.")

}

func main() {

	flag.Parse()

	processConfigAndAuthFiles()

	downloadCh := make(chan string, 100)
	unmarshalCh := make(chan []byte, 100)
	processCh := make(chan Asset, 100)
	streamCh := make(chan AssetToStream, 100)
	errCh := make(chan error)

	workers := ci.configFile.Workers

	for {
		if workers < 1 {
			break
		}
		workers--
		go downloader(downloadCh, unmarshalCh, errCh)
		go unmarshaler(unmarshalCh, processCh, errCh)
		go processor(processCh, downloadCh, streamCh, errCh)
		go streamer(streamCh, errCh)
	}

	// Kick it off!
	downloadCh <- ci.configFile.SourceURL

	for {
		select {
		case err := <-errCh:
			fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		}
	}

}

func processConfigAndAuthFiles() error {
	cf = configFile{}
	af = authFile{}

	data, err := readFile(nexusMigratorConfigFile)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, &cf)
	if err != nil {
		return err
	}

	data, err = readFile(nexusMigratorAuthFile)
	if err != nil {
		return err
	}

	err = json.Unmarshal(data, &af)
	if err != nil {
		return err
	}

	ci = confInfo{
		authFile:   af,
		configFile: cf,
	}

	return nil
}

func readFile(filename string) ([]byte, error) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func processor(processCh chan Asset, downloadCh chan string, streamCh chan AssetToStream, errCh chan error) {
	for {
		select {
		case asset := <-processCh:
			//fmt.Println("processor", "asset")
			if hasArtifacts(&asset) {
				//fmt.Println("processor", "hasArtifacts")
				artifacts := getArtifacts(&asset)
				for _, artifact := range artifacts {
					sURL := fmt.Sprintf("%s%s", ci.configFile.SourceDownloadURL, artifact)
					tURL := fmt.Sprintf("%s%s", ci.configFile.TargetURL, artifact)
					contentType := "application/java-archive"
					if strings.HasSuffix(artifact, ".pom") {
						contentType = "application/xml"
					}
					assetToStream := AssetToStream{
						sourceURL:   sURL,
						targetURL:   tURL,
						contentType: contentType,
					}
					//fmt.Println("processor", "hasArtifacts", "assetToStream")
					streamCh <- assetToStream
				}
			} else {
				groups := getGroups(&asset)
				for _, g := range groups {
					rootURL := fmt.Sprintf("%s%s", ci.configFile.SourceURL, g)
					//fmt.Printf("rootURL: %s\n", rootURL)
					downloadCh <- rootURL
				}
			}
		}
	}
}

func streamer(streamCh chan AssetToStream, errCh chan error) {
	for {
		select {
		case assetToStream := <-streamCh:
			//fmt.Printf("streaming from: %s\n", assetToStream.sourceURL)
			// Get the data
			req, err := http.NewRequest("GET", assetToStream.sourceURL, nil)
			if err != nil {
				errCh <- err
				continue
			}

			req.SetBasicAuth(ci.authFile.SourceUser, ci.authFile.SourcePassword)
			req.Header.Add("Accept", `application/json`)

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				errCh <- err
				continue
			}

			defer resp.Body.Close()
			err = httpUpload(assetToStream.targetURL, resp.Body, assetToStream.contentType)
			if err != nil {
				errCh <- err
				continue
			}
		}
	}

}

func unmarshaler(unmarshalCh chan []byte, processCh chan Asset, errCh chan error) {
	for {
		select {
		case data := <-unmarshalCh:
			//fmt.Println("unmarshaler", "data")
			dataObj := Data{}
			err := json.Unmarshal(data, &dataObj)
			if err != nil {
				errCh <- err
			} else {
				processCh <- dataObj.Asset
			}
		}
	}

}

func downloader(downloadCh chan string, unmarshalCh chan []byte, errCh chan error) {
	for {
		select {
		case url := <-downloadCh:
			fmt.Println("downloader", "url", url)
			data, err := httpDownload(url)
			if err != nil {
				errCh <- err
			} else {
				unmarshalCh <- data
			}
		}
	}
}

func httpDownload(url string) ([]byte, error) {
	//fmt.Printf("downloading: %s\n", url)
	// Get the data
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.SetBasicAuth(ci.authFile.SourceUser, ci.authFile.SourcePassword)
	req.Header.Add("Accept", `application/json`)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	//fmt.Printf("res.StatusCode: %d\n", resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		b, _ := ioutil.ReadAll(resp.Body)
		err = fmt.Errorf("URL:[%s], StatusCode:[%d], [%s]", url, resp.StatusCode, string(b))
		return nil, err
	}

	data, err := ioutil.ReadAll(resp.Body)

	return data, nil
}

func httpUpload(url string, body io.Reader, contentType string) error {
	fmt.Printf("streaming TO: %s\n", url)
	// POST the data
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return err
	}
	req.SetBasicAuth(ci.authFile.TargetUser, ci.authFile.TargetPassword)
	req.Header.Set("Content-Type", contentType)

	//fmt.Printf("%s ---- %#v\n", url, req)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	//fmt.Printf("res.StatusCode: %d\n", resp.StatusCode)
	if resp.StatusCode != http.StatusCreated {
		b, _ := ioutil.ReadAll(resp.Body)
		err = fmt.Errorf("URL:[%s], StatusCode:[%d], [%s]", url, resp.StatusCode, string(b))
		return err
	}

	return nil
}

func getArtifacts(asset *Asset) []string {
	artifacts := make([]string, 0)
	for _, a := range asset.Children {
		if a.Leaf {
			artifactURI := a.Path[1:]
			artifacts = append(artifacts, artifactURI)
			//fmt.Println("getArtifacts", "adding", artifactURI)
			if len(a.PomUri) > 0 {
				pomURI := strings.Replace(artifactURI, "jar", "pom", 1)
				artifacts = append(artifacts, pomURI)
				//fmt.Println("getArtifacts", "adding", artifactURI)
			}
		} else {
			//fmt.Printf("Group: %s %s\n", a.Type, a.Path)
			artifacts = append(artifacts, getArtifacts(&a)...)
		}
	}
	//fmt.Printf("getArtifacts: %s %d\n", asset.Path, len(artifacts))
	return artifacts
}

func getGroups(asset *Asset) []string {
	//fmt.Println("getGroups", asset.Path)
	groups := make([]string, 0)
	for _, a := range asset.Children {
		if a.Type == "G" {
			groups = append(groups, a.Path[1:])
		}
	}
	//fmt.Println("getGroups.len", len(groups))
	return groups
}

func hasArtifacts(asset *Asset) bool {
	for _, a := range asset.Children {
		if a.Leaf {
			return true
		} else {
			return hasArtifacts(&a)
		}
	}

	return false
}
