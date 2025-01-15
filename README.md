# Instructure DAP Helpers

Including a handful of functions for 

1. Querying instructure dap data in a way that can leverage serverless functions in a cost-effective manner. If you are using these Instructure's in a serverful (or less thrifty) environment, it is probably better to use their in-house module's download_table_data or execute_job functions. 
2. Tidying, in an opinionated fashion, the instructure query responses (flattening, removing prefixes, making meta.action field nullable).

Prerequisites: 

1. You still need the [instructure-dap-client](https://pypi.org/project/instructure-dap-client/) downloaded to get credentials pass to the data import functions. 


![test-tidying](https://github.com/blackhat-hemsworth/instructure-dap-helpers/actions/workflows/tests.yml/badge.svg)