package vsphere

import (
	"encoding/csv"
	"encoding/json"
	"fmt"

	"io"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/terraform/helper/schema"
	"github.com/terraform-providers/terraform-provider-vsphere/vsphere/internal/helper/folder"
	"github.com/terraform-providers/terraform-provider-vsphere/vsphere/internal/helper/network"
	"github.com/terraform-providers/terraform-provider-vsphere/vsphere/internal/helper/resourcepool"
	"github.com/vmware/govmomi/object"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func dataSourceVSphereCSV() *schema.Resource {
	return &schema.Resource{
		Read: dataSourceVSphereCSVRead,

		Schema: map[string]*schema.Schema{
			"datacenter_id": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},

			"datastore_cluster": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},

			"datastore_cluster_id": &schema.Schema{
				Type:     schema.TypeString,
				Computed: true,
			},

			"csvfile": &schema.Schema{
				Type:     schema.TypeString,
				Required: true,
			},

			"query": &schema.Schema{
				Type:     schema.TypeMap,
				Optional: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},

			"default_disks": &schema.Schema{
				Type:     schema.TypeMap,
				Required: true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},

			"result": &schema.Schema{
				Type:     schema.TypeList,
				Computed: true,
				Elem: &schema.Resource{
					Schema: map[string]*schema.Schema{
						"hostname": &schema.Schema{
							Type:     schema.TypeString,
							Computed: true,
						},
						"address": &schema.Schema{
							Type:     schema.TypeString,
							Computed: true,
						},
						"gateway": &schema.Schema{
							Type:     schema.TypeString,
							Computed: true,
						},
						"subnet": &schema.Schema{
							Type:     schema.TypeInt,
							Computed: true,
						},
						"cpu": &schema.Schema{
							Type:     schema.TypeInt,
							Computed: true,
						},
						"memory": &schema.Schema{
							Type:     schema.TypeInt,
							Computed: true,
						},
						"expires": &schema.Schema{
							Type:     schema.TypeString,
							Computed: true,
						},
						"power": &schema.Schema{
							Type:     schema.TypeString,
							Computed: true,
						},

						"template": &schema.Schema{
							Type:     schema.TypeString,
							Computed: true,
						},

						"network": &schema.Schema{
							Type:     schema.TypeString,
							Computed: true,
						},
						"vapp": &schema.Schema{
							Type:     schema.TypeString,
							Computed: true,
						},
						"folder": &schema.Schema{
							Type:     schema.TypeString,
							Computed: true,
						},
						"disk0": &schema.Schema{
							Type:     schema.TypeInt,
							Computed: true,
						},
						"disk1": &schema.Schema{
							Type:     schema.TypeInt,
							Computed: true,
						},
						"disk2": &schema.Schema{
							Type:     schema.TypeInt,
							Computed: true,
						},
						"disk3": &schema.Schema{
							Type:     schema.TypeInt,
							Computed: true,
						},
					},
				},
			},
		},
	}
}

func dataSourceVSphereCSVRead(d *schema.ResourceData, meta interface{}) error {
	client := meta.(*VSphereClient).vimClient

	var datacenter *object.Datacenter
	if dcID, ok := d.GetOk("datacenter_id"); ok {
		var err error
		datacenter, err = datacenterFromID(client, dcID.(string))
		if err != nil {
			return fmt.Errorf("cannot locate datacenter: %s", err)
		}
	}

	log.Printf("[DEBUG] ==================================================")
	log.Printf("[DEBUG] - Datacenter is %s", datacenter.InventoryPath)
	log.Printf("[DEBUG] ==================================================")

	datastore := d.Get("datastore_cluster")
	pod, err := resourceVSphereDatastoreClusterGetPodFromPath(meta, datastore.(string), datacenter.Reference().Value)
	if err != nil {
		return fmt.Errorf("error loading datastore cluster: %s", err)
	}

	d.Set("datastore_cluster_id", pod.Reference().Value)

	defaultDiskSizes := d.Get("default_disks").(map[string]interface{})
	csvfile := d.Get("csvfile").(string)
	query := d.Get("query").(map[string]interface{})
	data, err := ioutil.ReadFile(csvfile)
	reader := csv.NewReader(strings.NewReader(string(data)))
	columns := []string{
		"hostname",
		"address",
		"gateway",
		"subnet",
		"cpu",
		"memory",
		"vapp",
		"network",
		"template",
		"disk0",
		"disk1",
		"disk2",
		"disk3",
		"expires",
	}

	rows := make([]map[string]interface{}, 0)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("Failed to read CSV file %q: %s", csvfile, err)
		}
		// skip the header row if provided
		var header = true
		for i, v := range record {
			if v != columns[i] {
				header = false
			}
		}
		if header {
			continue
		}
		row := make(map[string]interface{})
		for i, k := range columns {
			row[k], err = strconv.Atoi(record[i])
			if err != nil {
				row[k] = string(record[i])
			}
			log.Println(row)
		}
		rows = append(rows, row)
	}
	resultJson, err := json.MarshalIndent(&rows, "", "    ")
	check(err)

	result := make([]map[string]interface{}, 0)
	err = json.Unmarshal(resultJson, &result)
	if err != nil {
		return fmt.Errorf("command %q produced invalid JSON: %s", csvfile, err)
	}

	// poor mans filter to JSON array
	filtered := make([]map[string]interface{}, 0)
	log.Println("beginning filter search....")
	if query != nil {
		for _, item := range result {
			var add = true
			for q, v := range query {
				log.Printf("item[%v] == %v\n", item[q].(string), v.(string))
				endsWith := strings.HasSuffix(item[q].(string), v.(string))
				if item[q] != v && !endsWith {
					add = false
				}
			}

			if item["vapp"] != "" {
				vappPool := strings.Split(item["vapp"].(string), ":")
				var vapp string
				var pool = ""
				if len(vappPool) == 2 {
					vapp = vappPool[0]
					pool = vappPool[1]
				} else {
					vapp = vappPool[0]
				}

				item["folder"] = "/"
				var vmPath = fmt.Sprintf("%s/vm/%s", datacenter.InventoryPath, item["vapp"])
				f, err := folder.RootPathParticleVM.SplitRelativeFolder(vmPath)
				if err != nil {
					log.Printf("[DEBUG] error parsing virtual machine path %q: %s - Using default / for folder name", vmPath, err)
				} else {
					item["folder"] = folder.NormalizePath(f)
				}

				resourcePool, err := resourcepool.FromPathOrDefault(client, vapp, pool, datacenter)
				check(err)
				log.Printf("[DEBUG] ==================================================")
				log.Printf("[DEBUG] %s", resourcePool.Reference().Value)
				log.Printf("[DEBUG] ==================================================")
				item["vapp"] = resourcePool.Reference().Value
			}

			for i := 0; i < 4; i++ {
				var disk = fmt.Sprintf("disk%d", i)
				if item[disk] == "" || item[disk] == nil {
					item[disk] = 0
					if defaultDiskSizes[disk] != nil {
						item[disk], _ = strconv.Atoi(defaultDiskSizes[disk].(string))
					}
				} else {
					if defaultDiskSizes[disk] == nil {
						continue
					}

					defaultSize, err := strconv.Atoi(defaultDiskSizes[disk].(string))
					if err != nil {
						defaultSize = 0
					}
					var csvsize int = int(item[disk].(float64))
					if csvsize < defaultSize {
						item[disk] = defaultSize
					}
				}
			}

			if item["network"] != "" {
				network, err := network.FromPath(client, item["network"].(string), datacenter)
				check(err)
				item["network"] = network.Reference().Value
			}

			if item["expires"] == "" || item["expires"] == nil {
				// set date to a year from now...
				y, m, d := time.Now().Date()
				item["expires"] = time.Date(y+1, m, d, 0, 0, 0, 0, time.Now().Location()).Format("2006-01-02")
			}

			item["power"] = "true"
			date, err := time.Parse("2006-01-02", item["expires"].(string))
			if err != nil {
				// try formatting the date in dd/mm/YYYY format
				// and yes, this is because of M$ excel f***ing with the format
				date, err = time.Parse("02/01/2006", item["expires"].(string))
				if err != nil {
					return fmt.Errorf("Invalid date format for expires. Format should be 'YYYY-MM-DD'")
				}
			}
			year, month, day := time.Now().Date()
			delta := time.Date(year, month, day, 0, 0, 0, 0, time.Now().Location()).Sub(date).Hours()
			if delta > 0 {
				item["power"] = "false"
			}

			// don't add any machines to the list that are > 7 days past expiry
			// these should already have been moved in the state file by python.
			if delta >= 168 { // 7 * 24 = 7 days
				add = false
			}

			if add {
				filtered = append(filtered, item)
			}
		}
	}

	log.Println("============= FILTERED >>>>>>>>>>>>>>>>>")
	for i := range filtered {
		log.Println(filtered[i])
	}
	log.Println("<<<<<<<<<<<<<=================")

	d.Set("result", &filtered)
	d.SetId("-")
	return nil
}
