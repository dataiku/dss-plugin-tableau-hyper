Current Version: 0.1.5 <br>
Compatible with DSS version: 6.X and higher<br>

## Plugin information

This plugin aims at enhancing interactions between Tableau Software products and DSS platform. [Tableau Software](https://tableau.com) offers a leading data visualisation engine available on several of their products (Tableau Server, Online, Desktop...).

Visualisations made with Tableau are associated with datasources that can be stored as Tableau Hyper files (`.hyper` extension). 

This plugin offers the ability to interact with the Tableau Hyper format from a DSS instance, enabling the following actions:

* Export a DSS dataset to a Hyper file locally (This file can be opened in Tableau Desktop.)
* Upload a DSS dataset to Tableau Server or Tableau Online
* Create shared credentials for Tableau Server or Tableau Online available to all DSS users on the instance
* Import a Hyper file stored locally as a DSS dataset

## Prerequisites

This plugin requires DSS 6.X or higher. The installation setup for this plugin follows the standard Dataiku code environment creation procedure. This plugin requires the [Tableau Server Client](https://tableau.github.io/server-client-python/) and 
[Tableau Hyper API](https://help.tableau.com/current/api/hyper_api/en-us/index.html) Python modules. They will be installed automatically through Pip. Note that the Tableau Hyper API is compatible with Python 3.6 or Python 3.7 but not with older versions of Python.

## How it works  

Once the plugin is successfully installed, the flow contains two exporters, available in the standard "Export" dialog of DSS, a file format component allowing creation of Dataiku dataset from Hyper files, and a plugin preset enabling Tableau Server or Tableau Online credentials to be accessible to selected users of the DSS instance containing the preset.

Please refer to the [Dataiku Plugin webpage](https://www.dataiku.com/product/plugins/tableau-hyper-export) for detailed usage information.

## Overview of the components included in this plugin

### Tableau Hyper file exporter

Export any DSS dataset as a local `.hyper` file. Once the preprocessing of the data is done, the DSS dataset is exported. Null values are kept and dataset schema is adapted to Tableau Hyper schema. The date and geopoints columns are preserved in Tableau enabling filtering and map visualisations.

### Tableau Hyper server upload

Enables the upload of a DSS dataset to Tableau Server or Tableau Online directly from your DSS flow. After upload, visualisations created from the resulting datasource are easily shared across organisation.

### Parameter Preset

The parameter preset enables referencing of Tableau Server or Tableau Online credentials making those available for selected users of DSS instance. The parameters that need to be set are associated with the Tableau Server Client connection: 

* Tableau Server URL
* Tableau Server Site ID
* Tableau Server Username
* Tableau Server Password

Usage of those parameters are detailed on the documentation of the 
[Tableau Server Client in Python](https://tableau.github.io/server-client-python/docs/):

 
```python
import tableauserverclient as TSC

tableau_auth = TSC.TableauAuth('USERNAME', 'PASSWORD', 'SITENAME')
server = TSC.Server('SERVER_URL')

with server.auth.sign_in(tableau_auth):
    all_datasources, pagination_item = server.datasources.get()
    print("\nThere are {} datasources on site: ".format(pagination_item.total_available))
    print([datasource.name for datasource in all_datasources])
```

```text
SERVER_URL is the URL of your Tableau server without subpaths. 
For local Tableau servers, an example would be: https://www.MY_SERVER.com. For Tableau Online, 
an example would be: https://10ax.online.tableau.com/.

SITENAME is the subpath of your full site URL (also called contentURL in the REST API). MYSITE would be the site 
name of https://10ax.online.tableau.com/MYSITE. This parameter can be omitted when signing in to the Default site of a 
on premise Tableau server.

```

### Tableau Hyper format handler

Tableau Hyper files can be read directly in DSS as datasets in the flow. 

## Technical details with respect to DSS

### What is the behavior during sequential export ?

Each time the DSS dataset is uploaded to Tableau Server or Tableau Online, it overwrites the existing datasource associated to it. Therefore, when launching the export from a DSS flow, the data on Tableau is up-to-date with the latest version of the dataset in DSS at that time.

### What is the behavior with respect to partitioning ?

DSS enables dataset partitioning. When partitioned datasets on DSS are exported to file using the Tableau Hyper plugin, one folder per partition is created. One `.hyper` file per partition and folder will be created.

An upload to server of a partitioned dataset will raise an error and is not supported. Before uploading, the dataset partitions have to be stacked together and merged as one. 

## Run the tests

From the tableau-hyper-export repository, run:
```shell script
PYTHONPATH=$PYTHONPATH:{path to plugin repository}/tableau-hyper-export/python-lib pytest
```
