package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"time"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/servers"
	"github.com/gophercloud/gophercloud/openstack/imageservice/v2/imagedata"
	"github.com/gophercloud/gophercloud/openstack/imageservice/v2/images"
	"github.com/gophercloud/gophercloud/pagination"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"gopkg.in/yaml.v2"
)

type osAuthInfos struct {
	IdentityEndpoint string `yaml:"identity_endpoint"`
	Username         string `yaml:"username"`
	Password         string `yaml:"password"`
	DomainName       string `yaml:"domain_name"`
	Region           string `yaml:"region"`
}

type s3Conf struct {
	Endpoint  string `yaml:"endpoint"`
	AccessKey string `yaml:"access_key"`
	SecretKey string `yaml:"secret_key"`
}

type conf struct {
	Mode               string      `yaml:"mode"` // can be projectToS3, projectToProject, s3ToProject
	ServersName        []string    `yaml:"servers_name"`
	ProjectSource      osAuthInfos `yaml:"os_project_source"`
	ProjectDestination osAuthInfos `yaml:"os_project_destination"`
	s3                 s3Conf      `yaml:"s3"`
}

func (c *conf) getConf(path string) error {
	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		return err
	}

	return err
}

func main() {

	// read conf file
	var c conf
	err := c.getConf("./config.yaml")
	if err != nil {
		log.Fatalf("Error when reading configuration file :  \n%s\n", err)
	}

	log.Printf("Running in %s mode", c.Mode)

	//s3
	var s3Client *minio.Client
	if c.Mode == "projectToS3" || c.Mode == "s3ToProject" {
		s3Client, err = minio.New(c.s3.Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(c.s3.AccessKey, c.s3.SecretKey, ""),
			Secure: true,
		})
		if err != nil {
			log.Fatalf("Error during S3 authentication : \n%s\n", err)
		}
	}

	//Openstack src
	srcOsOpts := gophercloud.AuthOptions{
		IdentityEndpoint: c.ProjectSource.IdentityEndpoint,
		Username:         c.ProjectSource.Username,
		Password:         c.ProjectSource.Password,
		DomainName:       c.ProjectSource.DomainName,
	}

	provider, err := openstack.AuthenticatedClient(srcOsOpts)
	if err != nil {
		log.Fatalf("Error during Openstack source authentication : \n%s\n", err)
		return
	}
	srcEndpointOpts := gophercloud.EndpointOpts{
		Region: c.ProjectSource.Region,
	}

	// Create server client
	serverClient, err := openstack.NewComputeV2(provider, srcEndpointOpts)
	if err != nil {
		log.Printf("3. %s\n", err)
		return
	}
	// Create image client
	imageClient, err := openstack.NewImageServiceV2(provider, srcEndpointOpts)
	if err != nil {
		log.Printf("3. %s\n", err)
		return
	}
	//////////

	// Get server list
	serverPages := servers.List(serverClient, servers.ListOpts{})

	err = serverPages.EachPage(func(page pagination.Page) (bool, error) {
		serverList, err := servers.ExtractServers(page)

		for _, wantedServer := range c.ServersName {
			found := false
			for _, osServer := range serverList {
				if osServer.Name == wantedServer {
					found = true
					///////////////////////////////
					fmt.Printf("Create a new Image from instance %s\n", osServer.Name)
					imgName := fmt.Sprintf("%s_%s_migration", osServer.Name, time.Now().Local().Format("2006-01-02_15-04-05"))
					srvImgOpts := servers.CreateImageOpts{
						Name: imgName,
						Metadata: map[string]string{
							"source_instance_id":        osServer.ID,
							"source_instance_name":      osServer.Name,
							"source_instance_tenant_id": osServer.TenantID,
						},
					}
					imageID, err := servers.CreateImage(serverClient, osServer.ID, srvImgOpts).ExtractImageID()
					if err != nil {
						log.Printf("Err during image creation from instance %s. ERR:\n%s\n", osServer.Name, err)
						continue
					}

					// wait image status is ready
					for {
						log.Printf("Checking for image status")

						imgListOpts := images.ListOpts{}
						allPages, err := images.List(imageClient, imgListOpts).AllPages()
						if err != nil {
							log.Printf("Err when fetching image informations from instance %s. ERR:\n%s\n", osServer.Name, err)
							continue
						}

						allImages, err := images.ExtractImages(allPages)
						if err != nil {
							log.Printf("Err when fetching image informations from instance %s. ERR:\n%s\n", osServer.Name, err)
							continue
						}

						ok := false

						for _, image := range allImages {
							if image.ID == imageID {
								fmt.Printf("%s\n", image.Name)
								fmt.Printf("%s\n", image.Status)

								if image.Status == "active" {
									ok = true
								}
							}
						}

						if ok {
							break
						}

						log.Printf("%s image for instance %s is not yet Active", imageID, osServer.Name)
						time.Sleep(5 * time.Second)
					}

					// Download the instance images
					image, err := imagedata.Download(imageClient, imageID).Extract()
					if err != nil {
						log.Printf("Err during image downloading (img. id: %s) for instance %s. ERR:\n%s\n", imageID, osServer.Name, err)
						continue
					}
					defer image.Close()

					// Create the qcow file
					// out, err := os.Create(fmt.Sprintf("%s.qcow2", imgName))
					// if err != nil {
					// 	log.Printf("Err during image downloading (img. id: %s) for instance %s. ERR:\n%s\n", imageID, s.Name, err)
					// 	continue
					// }
					// defer out.Close()
					// io.Copy(out, image)

					//
					// Upload to s3
					//
					n, err := s3Client.PutObject(context.Background(), "vm-bk", fmt.Sprintf("%s.qcow2", imgName), image, -1, minio.PutObjectOptions{})
					if err != nil {
						log.Fatalln(err)
					}
					log.Println("Uploaded", "my-objectname", " of size: ", n, "Successfully.")

					//Get all volume attached to this instance
					for _, vol := range osServer.AttachedVolumes {
						fmt.Println(vol.ID)
					}

					// Uppload volume & instance image to s3
					visi := images.ImageVisibilityPrivate
					createOpts := images.CreateOpts{
						Name:            "kiki-terraform-instance-img-restore",
						DiskFormat:      "qcow2",
						ContainerFormat: "bare",
						Visibility:      &visi,
					}
					newImage, err := images.Create(imageClient, createOpts).Extract()
					if err != nil {
						panic(err)
					}
					reader, err := s3Client.GetObject(context.Background(), "vm-bk", fmt.Sprintf("%s.qcow2", imgName), minio.GetObjectOptions{})
					if err != nil {
						log.Fatalln(err)
					}
					err = imagedata.Upload(imageClient, newImage.ID, reader).ExtractErr()
					if err != nil {
						log.Fatalln(err)
					}
					log.Printf("Image create !\n\n")
					//
					//////////////////////////////
					break
				}
			}
			if !found {
				log.Printf("Server %s not found on Openstack.", wantedServer)
			}
		}
		if err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		log.Printf("4. %s\n", err)
		return
	}

}
