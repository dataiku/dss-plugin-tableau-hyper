<!--<h3><b>Plugin: Tableau Hyper API</b></h3>-->
## <b>Plugin: Tableau Hyper API</b> [[Plugin Page]](https://www.dataiku.com/product/plugins/) <br>

![Teaser Image](https://avatars0.githubusercontent.com/u/828667?s=200&v=4)

### Overview ###

<b>This plugin offers the following capabilities:</b>
- (0) Upload a DSS dataset to Tableau Server 
- (1) Export a DSS dataset as a Hyper File
- (2) Import a hyper file to a DSS project 

### Dependencies ###

This plugins relies on [Tableau Hyper API](https://help.tableau.com/current/api/hyper_api/en-us/index.html) and the 
[Tableau Server Client](https://tableau.github.io/server-client-python/docs/), installed via Pip during plugin setup.
The Tableau Hyper API requires at least Python 3.6 version.

### (1) Usage in DSS

(1) See the demo video on [Google Video](https://drive.google.com/open?id=1YBPjrkygRzAzsC3yNZu6mEfhqHIjx75Z)

### (2) Structure of the plugin ### 

- An exporter to file
- An uploader to server
- A formatter to read a Hyper file

### (3) Testing the plugin component ###

The test are done using the PyTest framework. You can run them using `pytest` in a console.

### Misc ###

Contact thibault.desfontaines@dataiku.com
