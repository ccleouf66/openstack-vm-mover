package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os"
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
	//	TenantName string `yaml: domain_name`
}

type conf struct {
	Mode               string      `yaml: mode` // can be projectToS3, projectToProject, s3ToProject
	ServersName        []string    `yaml: servers_name`
	ProjectSource      osAuthInfos `yaml:"os_project_source"`
	ProjectDestination osAuthInfos `yaml:"os_project_destination"`
}

func (c *conf) getConf(path string) *conf {

	yamlFile, err := ioutil.ReadFile(path)
	if err != nil {
		log.Printf("yamlFile.Get err   #%v ", err)
	}
	err = yaml.Unmarshal(yamlFile, c)
	if err != nil {
		log.Fatalf("Unmarshal: %v", err)
	}

	return c
}

func main() {

	// read conf file
	var c conf
	c.getConf("./config.yaml")
	//src
	fmt.Printf("src_id_endpoint: %s\n", c.ProjectSource.IdentityEndpoint)
	fmt.Printf("src_username: %s\n", c.ProjectSource.Username)
	fmt.Printf("src_password: %s\n", c.ProjectSource.Password)
	fmt.Printf("src_domain: %s\n", c.ProjectSource.DomainName)
	//dst
	fmt.Printf("dst_id_endpoint: %s\n", c.ProjectDestination.IdentityEndpoint)
	fmt.Printf("dst_username: %s\n", c.ProjectDestination.Username)
	fmt.Printf("dst_password: %s\n", c.ProjectDestination.Password)
	fmt.Printf("dst_domain: %s\n", c.ProjectDestination.DomainName)
	return

	// s3
	endpoint := ""
	accessKeyID := ""
	secretAccessKey := ""

	s3Client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: true,
	})
	if err != nil {
		log.Fatalln(err)
	}

	// Openstack
	opts, err := openstack.AuthOptionsFromEnv()
	if err != nil {
		log.Printf("1. %s\n", err)
		return
	}

	provider, err := openstack.AuthenticatedClient(opts)
	if err != nil {
		log.Printf("2. %s\n", err)
		return
	}

	srcEndpointOpts := gophercloud.EndpointOpts{
		Region: os.Getenv("OS_REGION_NAME"),
	}

	serverClient, err := openstack.NewComputeV2(provider, srcEndpointOpts)
	if err != nil {
		log.Printf("3. %s\n", err)
		return
	}
	imageClient, err := openstack.NewImageServiceV2(provider, srcEndpointOpts)
	if err != nil {
		log.Printf("3. %s\n", err)
		return
	}

	// Get server list
	serverPages := servers.List(serverClient, servers.ListOpts{
		Name: "terraform_instance",
	})

	err = serverPages.EachPage(func(page pagination.Page) (bool, error) {
		serverList, err := servers.ExtractServers(page)
		if err != nil {
			return false, err
		}
		for _, s := range serverList {
			fmt.Println(s.Name)

			// schedule an image creation from this instance
			imgName := fmt.Sprintf("%s_%s_migration", s.Name, time.Now().Local().String())
			srvImgOpts := servers.CreateImageOpts{
				Name: imgName,
				Metadata: map[string]string{
					"source_instance_id":        s.ID,
					"source_instance_name":      s.Name,
					"source_instance_tenant_id": s.TenantID,
				},
			}
			result := servers.CreateImage(serverClient, s.ID, srvImgOpts)
			imageID, err := result.ExtractImageID()
			if err != nil {
				log.Printf("Err during image creation from instance %s. ERR:\n%s\n", s.Name, err)
				continue
			}

			// wait image status is ready
			for {
				log.Printf("Checking for image status")

				imgListOpts := images.ListOpts{}
				allPages, err := images.List(imageClient, imgListOpts).AllPages()
				if err != nil {
					log.Printf("Err when fetching image informations from instance %s. ERR:\n%s\n", s.Name, err)
					continue
				}

				allImages, err := images.ExtractImages(allPages)
				if err != nil {
					log.Printf("Err when fetching image informations from instance %s. ERR:\n%s\n", s.Name, err)
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

				log.Printf("%s image for instance %s is not yet Active", imageID, s.Name)
				time.Sleep(5 * time.Second)
			}

			// Download the instance images
			image, err := imagedata.Download(imageClient, imageID).Extract()
			if err != nil {
				log.Printf("Err during image downloading (img. id: %s) for instance %s. ERR:\n%s\n", imageID, s.Name, err)
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
			for _, vol := range s.AttachedVolumes {
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

		}
		return true, nil
	})
	if err != nil {
		log.Printf("4. %s\n", err)
		return
	}

}
