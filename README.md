Current Version: 0.1.0
Compatible with DSS version: 5.0.0 and higher

## Plugin information

[Tableau](https://tableau.com) is one of the leading data visualization tools. Tableau Hyper is a file format
that is specific to Tableau.

This plugin offers the ability to interact with the Tableau Hyper format from a DSS instance, 
enabling the following actions:

* Import a Hyper file in Dataiku as a standard dataset
* Export a DSS standard dataset to a Hyper file for immediate open in Tableau Desktop
* Directly upload a dataset to Tableau Server with the Hyper format
* Make credentials to Tableau Server available to all DSS users on instance using a Plugin Preset

## Prerequisites

This plugin requires DSS 5.0.2 or higher. The installation setup for this plugin follows
the standard Dataiku code environment creation procedure. This plugin requires the 
Python modules [Tableau Server Client](https://tableau.github.io/server-client-python/) and 
[Tableau Hyper API](https://help.tableau.com/current/api/hyper_api/en-us/index.html) that 
will be installed automatically through Pip. Note that the Tableau Hyper API is 
compatible with Python 3.6 or Python 3.7 and not with older versions of Python.

## How it works  

Once the plugin is successfully installed, the flow contains two exporters, 
available in the standard "Export" dialog of DSS, a file format component allowing 
creation of Dataiku dataset from Hyper files, and a plugin preset enabling Tableau 
Server credentials to be accessible to all user on the Dataiku instance of your organisation.

Please refer to the [Dataiku Plugin webpage](https://www.dataiku.com/dss/plugins/info/tableau-hyper-extract.html) for detailed usage information

## Components included in this plugin

### The Tableau Hyper Exporter to file

Export the results of your data processing, findings and predictions in DSS
as a Tableau Hyper file that can be opened directly into Tableau. 

### The Tableau Hyper Exporter to Server

Enables the upload of a file to a Tableau Server or to Tableau Online directly from your
DSS flow. Using the preset, the Tableau Server credentials are available for all DSS
users on your instance. Access and interact with your data on Tableau Server or Tableau 
Online.

### Parameter Set

A preset for referencing your Tableau Server or Tableau Online credentials is available in this plugin. Set up your connection 
credentials once for all DSS user on the instance. The parameters available are the following:

* Tableau Server URL
* Tableau Server Site ID
* Tableau Server Username
* Tableau Server Password

Usage of those parameters are detailed on the documentation of the 
[Tableau Server Client in Python](https://tableau.github.io/server-client-python/docs/).

 
```python
import tableauserverclient as TSC

tableau_auth = TSC.TableauAuth('USERNAME', 'PASSWORD', 'SITENAME')
server = TSC.Server('http://SERVER_URL')

with server.auth.sign_in(tableau_auth):
    all_datasources, pagination_item = server.datasources.get()
    print("\nThere are {} datasources on site: ".format(pagination_item.total_available))
    print([datasource.name for datasource in all_datasources])
```

SERVER_URL is the URL of your Tableau server without subpaths. 
For local Tableau servers, an example would be: https://www.MY_SERVER.com. For Tableau Online, 
an example would be: https://10ax.online.tableau.com/.

SITENAME is the subpath of your full site URL (also called contentURL in the REST API). MYSITE would be the site 
name of https://10ax.online.tableau.com/MYSITE. This parameter can be omitted when signing in to the Default site of a 
on premise Tableau server.

### The Tableau Hyper Format Component

Explore Tableau Hyper files directly as a datasource in your DSS flow. Specify the Tableau 
Hyper format on the input file and interact with the data instantly. 

## Specific question relative to DSS

### What is the behavior with respect to partitioning ?

For partitioned datasets, an export to folder will create as many folders as partitions and will create
one Tableau Hyper file in each folder (one per partition). However, uploading a partitioned dataset to Tableau Server is 
not allowed due to the fact that there can be only one connection at a given time for a Hyper file.
