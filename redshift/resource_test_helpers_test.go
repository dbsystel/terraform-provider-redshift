package redshift

import (
	"fmt"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
)

func testCheckTypeSetElems(resourceName, attr string, want ...string) resource.TestCheckFunc {
	checks := make([]resource.TestCheckFunc, 0, len(want)+1)

	checks = append(checks,
		resource.TestCheckResourceAttr(resourceName, attr+".#", fmt.Sprintf("%d", len(want))),
	)

	for _, v := range want {
		checks = append(checks,
			resource.TestCheckTypeSetElemAttr(resourceName, attr+".*", v),
		)
	}

	return resource.ComposeTestCheckFunc(checks...)
}
