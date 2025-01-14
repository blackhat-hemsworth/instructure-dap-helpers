# Instructure DAP Helpers

Including a handful of functions for 

1. Querying instructure dap data in a way that can leverage serverless functions (i.e. not keeping the function running while instructure fetches query results)
2. Tidying, in an opinionated fashion, the instructure query responses

Prerequisites: 

1. You still need the [instructure-dap-client](https://pypi.org/project/instructure-dap-client/) downloaded to get credentials pass to the data import functions. 


![test-tidying](https://github.com/blackhat-hemsworth/instructure-dap-helpers/blob/3295c64ff1726eb59b7a983f20d4bcfacb6952c8/.github/workflows/tests.yml/badge.svg)