package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"time"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/v3/snapshots"
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
	S3                 s3Conf      `yaml:"s3"`
	WorkerCount        int         `yaml:"worker_count"`
}

type job struct {
	OsServer     servers.Server
	Conf         conf
	S3Client     *minio.Client
	ServerClient *gophercloud.ServiceClient
	ImageClient  *gophercloud.ServiceClient
	BlockClient  *gophercloud.ServiceClient
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

func worker(id int, jobs <-chan job, results chan<- int) {
	for j := range jobs {
		log.Printf("Worker %d started job for server %s\n", id, j.OsServer.Name)
		err := ProcessOpenstackInstance(j.OsServer, j.Conf, j.S3Client, j.ServerClient, j.ImageClient, j.BlockClient)
		if err != nil {
			log.Println(err)
		}
		log.Printf("Worker %d finished job for server %s\n", id, j.OsServer.Name)
		results <- id
	}
}

func main() {

	// read conf file
	var c conf
	err := c.getConf("./config.yaml")
	if err != nil {
		log.Fatalf("Error when reading configuration file :  \n%s\n", err)
	}

	if c.Mode == "projectToS3" || c.Mode == "projectToProject" || c.Mode == "s3ToProject" {
		log.Printf("Running in %s mode", c.Mode)
	} else {
		log.Fatalf("Mode %s not supported.\nAvailable mode : \n projectToS3\n projectToProject\n s3ToProject", c.Mode)
	}

	//s3
	var s3Client *minio.Client
	if c.Mode == "projectToS3" || c.Mode == "s3ToProject" {
		s3Client, err = minio.New(c.S3.Endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(c.S3.AccessKey, c.S3.SecretKey, ""),
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
		log.Fatalf("Error during Openstack source authentication on compute service : %s\n", err)
	}
	// Create image client
	imageClient, err := openstack.NewImageServiceV2(provider, srcEndpointOpts)
	if err != nil {
		log.Fatalf("Error during Openstack source authentication on image service: %s\n", err)
	}

	blockClient, err := openstack.NewBlockStorageV3(provider, srcEndpointOpts)
	if err != nil {
		log.Fatalf("Error during Openstack source authentication on blockStorage service: %s\n", err)
	}
	//////////

	if c.Mode == "projectToProject" || c.Mode == "projectToS3" {

		if c.WorkerCount < 1 {
			log.Fatalf("Worker count = %d, did you define the 'worker_count' var in the config.yaml file ?\n", c.WorkerCount)
		}
		// Job queue
		jobs := make(chan job, len(c.ServersName))
		results := make(chan int, len(c.ServersName))

		// Define the number of workers working in parallel
		log.Printf("Creating %d workers\n", c.WorkerCount)
		for w := 1; w <= c.WorkerCount; w++ {
			go worker(w, jobs, results)
		}

		// Get server list
		serverPages := servers.List(serverClient, servers.ListOpts{})

		err = serverPages.EachPage(func(page pagination.Page) (bool, error) {
			serverList, err := servers.ExtractServers(page)

			serverCount := 0
			for _, wantedServer := range c.ServersName {
				found := false
				for _, osServer := range serverList {
					if osServer.Name == wantedServer {
						found = true
						///////////////////////////////
						// For each server create a new job with corresponding infos and push it in the queue
						///////////////////////////////
						newJob := job{
							OsServer:     osServer,
							Conf:         c,
							S3Client:     s3Client,
							ServerClient: serverClient,
							ImageClient:  imageClient,
							BlockClient:  blockClient,
						}
						jobs <- newJob
						serverCount++
						//////////////////////////////
						break
					}
				}
				if !found {
					log.Printf("Server %s not found on Openstack.", wantedServer)
				}
			}
			close(jobs)
			for i := 1; i <= serverCount; i++ {
				<-results
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

}

func CreateImageFromInstance(srv servers.Server, serverClient *gophercloud.ServiceClient) (string, string, error) {
	log.Printf("Create a new Image from instance %s\n", srv.Name)
	imgName := fmt.Sprintf("%s_%s_migration", srv.Name, time.Now().Local().Format("2006-01-02_15-04-05"))
	srvImgOpts := servers.CreateImageOpts{
		Name: imgName,
		Metadata: map[string]string{
			"source_instance_id":        srv.ID,
			"source_instance_name":      srv.Name,
			"source_instance_tenant_id": srv.TenantID,
		},
	}
	imageID, err := servers.CreateImage(serverClient, srv.ID, srvImgOpts).ExtractImageID()
	if err != nil {
		return "", "", err
	}
	return imageID, imgName, nil
}

func WaitImageStatusOk(imageID string, srv servers.Server, imageClient *gophercloud.ServiceClient) error {
	log.Printf("Waiting image status for server %s.", srv.Name)
	for {
		image, err := images.Get(imageClient, imageID).Extract()
		if err != nil {
			return err
		}
		if image.Status == "active" {
			log.Printf("Image status is %s for server %s.", image.Status, srv.Name)
			break
		}

		//log.Printf("%s image for instance %s is not yet Active", imageID, srv.Name)
		time.Sleep(10 * time.Second)
	}
	return nil
}

func UploadImageToProject(imageClient *gophercloud.ServiceClient, imageName string, imageReader io.ReadCloser) error {
	log.Printf("Uploading image %s\n", imageName)
	visi := images.ImageVisibilityPrivate
	createOpts := images.CreateOpts{
		Name:            imageName,
		DiskFormat:      "qcow2",
		ContainerFormat: "bare",
		Visibility:      &visi,
	}
	newImage, err := images.Create(imageClient, createOpts).Extract()
	if err != nil {
		return err
	}

	err = imagedata.Upload(imageClient, newImage.ID, imageReader).ExtractErr()
	if err != nil {
		return err
	}
	log.Printf("Image %s uploaded\n", imageName)
	return err
}

func ProcessOpenstackInstance(osServer servers.Server, c conf, s3Client *minio.Client, serverClient *gophercloud.ServiceClient, imageClient *gophercloud.ServiceClient, blockClient *gophercloud.ServiceClient) error {
	// Create new image from server
	imageID, imageName, err := CreateImageFromInstance(osServer, serverClient)
	if err != nil {
		log.Printf("Error during image creation from instance %s\n", osServer.Name)
		return err
	}

	// Wait image status == ready
	err = WaitImageStatusOk(imageID, osServer, imageClient)
	if err != nil {
		log.Printf("Error when fetching image informations from instance %s\n", osServer.Name)
		return err
	}

	// Download the instance images
	imageReader, err := imagedata.Download(imageClient, imageID).Extract()
	if err != nil {
		log.Printf("Err during image downloading (img. id: %s) for instance %s\n", imageID, osServer.Name)
		return err
	}
	defer imageReader.Close()

	// Upload to s3
	if c.Mode == "projectToS3" {
		n, err := s3Client.PutObject(context.Background(), "vm-bk", fmt.Sprintf("%s.qcow2", imageName), imageReader, -1, minio.PutObjectOptions{})
		if err != nil {
			log.Printf("Error during image uploading to s3\n")
			return err
		}
		log.Printf("Uploaded %s of size %d to s3 successfully.", imageName, n.Size)
	}

	// Upload to other project
	if c.Mode == "projectToProject" {
		err = UploadImageToProject(imageClient, imageName, imageReader)
		if err != nil {
			log.Printf("Error during image uploading to Openstack project :\n%s", err)
			return err
		}
	}

	// TODO
	//
	//Get all volume attached to this instance
	for _, vol := range osServer.AttachedVolumes {
		snapshot, err := snapshots.Create(blockClient, snapshots.CreateOpts{
			Name:     fmt.Sprintf("%s_%s", imageName, vol.ID),
			VolumeID: vol.ID,
			Force:    true,
		}).Extract()
		if err != nil {
			log.Printf("Error during volume snapshot (vol_id: %s) creation for instance %s", vol.ID, osServer.Name)
			return err
		}
		log.Printf("%s with %s\n", snapshot.ID, snapshot.Status)
		//////////////////////////:

	}
	return nil
}
