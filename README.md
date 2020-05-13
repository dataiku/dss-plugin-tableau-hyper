Current Version: 0.1.0
Compatible with DSS version: 7.0.0 and higher

## Plugin information

[Tableau](https://tableau.com) is one of the leading data visualization tools. Tableau Hyper is a file format
that is specific to Tableau.

This plugin offers the ability to interact with the Tableau Hyper format from a DSS instance:

* Export a DSS dataset as a Tableau Hyper file
* Upload a DSS dataset as a Tableau Hyper Extract on Tableau Server or Tableau Online
* Import a Tableau Hyper file as a DSS dataset on a DSS engine

## Prerequisites

The plugin comes prepackaged with a code environement that will install the [Tableau Server Client](https://tableau.github.io/server-client-python/)
and the [Tableau Hyper API](https://help.tableau.com/current/api/hyper_api/en-us/index.html).

## How it works  

Once the plugin is successfully installed you can use it as an ordinary DSS exporter or formatter component.

Please refer to the [Dataiku Plugin webpage](https://www.dataiku.com/dss/plugins/info/tableau-hyper-extract.html) for detailed usage information

## Components included in this plugin

### Parameter Set

A preset for referencing your Tableau Server or Tableau Online credentials is available in this plugin. Set up your connection 
credentials once for all DSS user on the instance. The parameters available are the following:

* Tableau Server URL
* Tableau Server Site ID
* Tableau Server Username
* Tableau Server Password

## The Tableau Hyper Format Component

Explore Hyper files directly as a datasource in your DSS flow. Specify the Tableau 
Hyper format on the input file and interact with the data instantly. 

## The Tableau Hyper Exporter to file

Export the results of your data processing, findings and predictions in DSS
as a Tableau Hyper file that can be opened directly into Tableau.

## The Tableau Hyper Exporter to Server

Enables the upload of a file to a Tableau Server or to Tableau Online directly from your
DSS flow. Using the preset, the Tableau Server credentials are available for all DSS
users on your instance. 