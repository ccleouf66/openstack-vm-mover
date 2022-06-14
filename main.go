package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"time"

	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/extensions/volumetransfers"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/v3/volumes"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/extensions/volumeattach"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/flavors"
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
	OsServer        servers.Server
	Conf            conf
	S3Client        *minio.Client
	SrcServerClient *gophercloud.ServiceClient
	SrcImageClient  *gophercloud.ServiceClient
	SrcBlockClient  *gophercloud.ServiceClient
	DstServerClient *gophercloud.ServiceClient
	DstImageClient  *gophercloud.ServiceClient
	DstBlockClient  *gophercloud.ServiceClient
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
		err := ProcessOpenstackInstance(j)
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
	srcOsAuthOpts := gophercloud.AuthOptions{
		IdentityEndpoint: c.ProjectSource.IdentityEndpoint,
		Username:         c.ProjectSource.Username,
		Password:         c.ProjectSource.Password,
		DomainName:       c.ProjectSource.DomainName,
		AllowReauth:      true,
	}
	srcServerClient, srcImageClient, srcBlockClient, err := AuthOpenstack(srcOsAuthOpts, c.ProjectSource.Region)
	if err != nil {
		log.Printf("Source Openstack authentication failed.\n")
		log.Fatalf("%s", err)
	}
	//Openstack dst
	dstOsAuthOpts := gophercloud.AuthOptions{
		IdentityEndpoint: c.ProjectDestination.IdentityEndpoint,
		Username:         c.ProjectDestination.Username,
		Password:         c.ProjectDestination.Password,
		DomainName:       c.ProjectDestination.DomainName,
		AllowReauth:      true,
	}
	dstServerClient, dstImageClient, dstBlockClient, err := AuthOpenstack(dstOsAuthOpts, c.ProjectDestination.Region)
	if err != nil {
		log.Printf("Destination Openstack authentication failed.\n")
		log.Fatalf("%s", err)
	}

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
		serverPages := servers.List(srcServerClient, servers.ListOpts{})

		err = serverPages.EachPage(func(page pagination.Page) (bool, error) {
			serverList, err := servers.ExtractServers(page)

			for _, wantedServer := range c.ServersName {
				found := false
				for _, osServer := range serverList {
					if osServer.Name == wantedServer {
						found = true
						///////////////////////////////
						// For each server create a new job with corresponding infos and push it in the queue
						///////////////////////////////
						newJob := job{
							OsServer:        osServer,
							Conf:            c,
							S3Client:        s3Client,
							SrcServerClient: srcServerClient,
							SrcImageClient:  srcImageClient,
							SrcBlockClient:  srcBlockClient,

							DstServerClient: dstServerClient,
							DstImageClient:  dstImageClient,
							DstBlockClient:  dstBlockClient,
						}
						jobs <- newJob
						//////////////////////////////
						break
					}
				}
				if !found {
					log.Printf("Server %s not found on Openstack.", wantedServer)
				}
			}
			close(jobs)
			for i := 0; i < len(c.ServersName); i++ {
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

func AuthOpenstack(osAuthOptions gophercloud.AuthOptions, region string) (serverClient *gophercloud.ServiceClient, imageClient *gophercloud.ServiceClient, blockClient *gophercloud.ServiceClient, err error) {

	provider, err := openstack.AuthenticatedClient(osAuthOptions)
	if err != nil {
		log.Printf("Error during Openstack authentication on Client creation.\n")
		return nil, nil, nil, err
	}
	srcEndpointOpts := gophercloud.EndpointOpts{
		Region: region,
	}

	// Create server client
	serverClient, err = openstack.NewComputeV2(provider, srcEndpointOpts)
	if err != nil {
		log.Printf("Error during Openstack authentication on compute service.\n")
		return nil, nil, nil, err
	}

	// Create image client
	imageClient, err = openstack.NewImageServiceV2(provider, srcEndpointOpts)
	if err != nil {
		log.Printf("Error during Openstack authentication on image service.\n")
		return nil, nil, nil, err
	}

	// Create block client
	blockClient, err = openstack.NewBlockStorageV3(provider, srcEndpointOpts)
	if err != nil {
		log.Printf("Error during Openstack authentication on blockStorage service.")
		return nil, nil, nil, err
	}
	blockClient.Microversion = "3.44"
	return serverClient, imageClient, blockClient, err
	//////////
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

func UploadImageToProject(imageClient *gophercloud.ServiceClient, imageName string, imageReader io.ReadCloser) (*images.Image, error) {
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
		return newImage, err
	}

	err = imagedata.Upload(imageClient, newImage.ID, imageReader).ExtractErr()
	if err != nil {
		return newImage, err
	}
	log.Printf("Image %s uploaded\n", imageName)
	return newImage, err
}

func TransferVolumeToProject(server servers.Server, srcBlockClient *gophercloud.ServiceClient, srcServerClient *gophercloud.ServiceClient, dstBlockClient *gophercloud.ServiceClient) {
	//Get all volume attached to this instance
	for _, attachedVol := range server.AttachedVolumes {
		//////////////////////////TODO
		// Snapshot vol only if option is set tot true
		// snapshot, err := snapshots.Create(j.SrcBlockClient, snapshots.CreateOpts{
		// 	Name:     fmt.Sprintf("%s_%s", imageName, vol.ID),
		// 	VolumeID: vol.ID,
		// 	Force:    true,
		// }).Extract()
		// if err != nil {
		// 	log.Printf("Error during volume snapshot (vol_id: %s) creation for instance %s", vol.ID, j.OsServer.Name)
		// 	return err
		// }
		//////////////////////////

		// Detach the volume
		err := volumeattach.Delete(srcServerClient, server.ID, attachedVol.ID).ExtractErr()
		if err != nil {
			log.Printf("Error when delette attachement for volume ID %s : \n%s\n", attachedVol.ID, err)
			continue
		}

		// Wait volume status = available (not attached or in-use)
		err = WaitVolumeAvailable(srcBlockClient, attachedVol.ID)
		if err != nil {
			log.Printf("Error when fetching volume status for volume ID %s : \n%s\n", attachedVol.ID, err)
			continue
		}

		// Create the volumeTransfers on source project
		volumeTransferOpts := volumetransfers.CreateOpts{
			VolumeID: attachedVol.ID,
			Name:     fmt.Sprintf("req-%s", attachedVol.ID),
		}

		reqTransfer, err := volumetransfers.Create(srcBlockClient, volumeTransferOpts).Extract()
		if err != nil {
			log.Printf("Error during volumetransfers creation for volume ID %s : \n%s\n", attachedVol.ID, err)
			continue
		}
		log.Printf("Volume transfer request %s created for volume %s\n", reqTransfer.ID, attachedVol.ID)

		// Accept the volumeTransfers on destination project
		acceptOpts := volumetransfers.AcceptOpts{
			AuthKey: reqTransfer.AuthKey,
		}

		transfer, err := volumetransfers.Accept(dstBlockClient, reqTransfer.ID, acceptOpts).Extract()
		if err != nil {
			log.Printf("Error when accepting volumetransfers %s for volume ID %s : \n%s\n", transfer.ID, attachedVol.ID, err)
		}
		log.Printf("Volume transfer request %s accepted for volume %s\n", reqTransfer.ID, attachedVol.ID)
	}
}

func WaitVolumeAvailable(srcBlockClient *gophercloud.ServiceClient, volID string) error {
	log.Printf("Waiting block volume %s to be available.", volID)
	for {
		vol, err := volumes.Get(srcBlockClient, volID).Extract()
		if err != nil {
			return nil
		}
		if vol.Status == "available" {
			log.Printf("Volume %s -> %s.", volID, vol.Status)
			return nil
		}
		time.Sleep(10 * time.Second)
	}
}

func ProcessOpenstackInstance(j job) error {

	// Create new image from server
	srcImageID, imageName, err := CreateImageFromInstance(j.OsServer, j.SrcServerClient)
	if err != nil {
		log.Printf("Error during image creation from instance %s\n", j.OsServer.Name)
		return err
	}

	// Wait image status == ready
	err = WaitImageStatusOk(srcImageID, j.OsServer, j.SrcImageClient)
	if err != nil {
		log.Printf("Error when fetching image informations from instance %s\n", j.OsServer.Name)
		return err
	}

	// Download the instance images
	imageReader, err := imagedata.Download(j.SrcImageClient, srcImageID).Extract()
	if err != nil {
		log.Printf("Err during image downloading (img. id: %s) for instance %s\n", srcImageID, j.OsServer.Name)
		return err
	}
	defer imageReader.Close()

	// Upload to s3
	// TO-DO
	if j.Conf.Mode == "projectToS3" {
		n, err := j.S3Client.PutObject(context.Background(), "vm-bk", fmt.Sprintf("%s.qcow2", imageName), imageReader, -1, minio.PutObjectOptions{})
		if err != nil {
			log.Printf("Error during image uploading to s3\n")
			return err
		}
		log.Printf("Uploaded %s of size %d to s3 successfully.", imageName, n.Size)
	}

	// Upload to other project
	if j.Conf.Mode == "projectToProject" {
		destImage, err := UploadImageToProject(j.DstImageClient, imageName, imageReader)
		if err != nil {
			log.Printf("Error during image uploading to Openstack project :\n%s", err)
			return err
		}

		// Detach and transfer block volume from source to dest projet
		TransferVolumeToProject(j.OsServer, j.SrcBlockClient, j.SrcServerClient, j.DstBlockClient)

		// Create new server on dest project with exported image
		jsonFlavor, err := json.Marshal(j.OsServer.Flavor)
		if err != nil {
			log.Printf("Error when getting flavor infos for instance %s\n", j.OsServer.Name)
			return err
		}
		srcFlavor := flavors.Flavor{}
		err = srcFlavor.UnmarshalJSON(jsonFlavor)
		if err != nil {
			log.Printf("Error when decoding flavor infos for instance %s\n", j.OsServer.Name)
			return err
		}
		createOpts := servers.CreateOpts{
			Name:      j.OsServer.Name,
			ImageRef:  destImage.ID,
			FlavorRef: srcFlavor.ID,
			Networks: []servers.Network{
				{UUID: "581fad02-158d-4dc6-81f0-c1ec2794bbec"},
			},
		}

		dstServer, err := servers.Create(j.DstServerClient, createOpts).Extract()
		if err != nil {
			log.Printf("Error when creating new instance on destination project for instance %s\n", j.OsServer.Name)
			return err
		}
		log.Printf("New server %s created on destination project\n", dstServer.Name)

	}

	return nil
}
