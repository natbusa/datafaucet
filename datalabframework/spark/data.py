from pyspark.sql import DataFrame
from IPython.core.display import display, HTML

class Data:
    def __init__(self, df, scols=None, gcols=None):
        self.df = df
        self.gcols = gcols or []
        
        self.scols = scols or df.columns
        self.scols = list(set(self.scols) - set(self.gcols))

    @property
    def columns(self):
        return [x for x in self.df.columns if x in (self.scols + self.gcols)]
    
    def grid(self, limit=1000, render='pandas'):
        # get the data
        data = self.df.select(self.columns).limit(limit).toPandas()
        
        if render=='pandas':
            return data
        elif render=='jsgrid':
            html = """
<link type="text/css" rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jsgrid/1.5.3/jsgrid.min.css" />
<link type="text/css" rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jsgrid/1.5.3/jsgrid-theme.min.css" />
<script type="text/javascript" src="https://cdnjs.cloudflare.com/ajax/libs/jsgrid/1.5.3/jsgrid.min.js"></script>

<div id="jsGrid"></div>

<script>
    var clients = [
        { "Name": "Otto Clay", "Age": 25, "Country": 1, "Address": "Ap #897-1459 Quam Avenue", "Married": false },
        { "Name": "Connor Johnston", "Age": 45, "Country": 2, "Address": "Ap #370-4647 Dis Av.", "Married": true },
        { "Name": "Lacey Hess", "Age": 29, "Country": 3, "Address": "Ap #365-8835 Integer St.", "Married": false },
        { "Name": "Timothy Henson", "Age": 56, "Country": 1, "Address": "911-5143 Luctus Ave", "Married": true },
        { "Name": "Ramona Benton", "Age": 32, "Country": 3, "Address": "Ap #614-689 Vehicula Street", "Married": false }
    ];
    
    db = []
    var i;
    for (i = 0; i < 10; i++) { 
        db = db.concat(clients)
    }
 
    var countries = [
        { Name: "", Id: 0 },
        { Name: "United States", Id: 1 },
        { Name: "Canada", Id: 2 },
        { Name: "United Kingdom", Id: 3 }
    ];
 
    $("#jsGrid").jsGrid({
        width: "100%",
        height: "400px",
 
        heading: true,
        filtering: true,
        inserting: false,
        editing: false,
        selecting: false,
        sorting: true,
        paging: true,
        pageLoading: false,
        autoload: true,

        pageSize: 15,
        pageButtonCount: 3,
        
        controller: {
            loadData: function(filter) {
                //to do: filtering
                return db;
            }
        },

        fields: [
            { name: "Name", type: "text", width: 150, validate: "required" },
            { name: "Age", type: "number", width: 50 },
            { name: "Address", type: "text", width: 200 },
            { name: "Country", type: "select", items: countries, valueField: "Id", textField: "Name" },
            { name: "Married", type: "checkbox", title: "Is Married", sorting: false }
        ]
    });
</script>
"""
        return display(HTML(html)) 

    def facets(self, n=1000):
        try:
            from IPython.display import display
        except:
            display = None
    
        if display:
            jsonstr = self.df.select(self.columns).rows.sample(n).data.collect(n, axis=0).to_json(orient='records')
            HTML_TEMPLATE = """
                <script src="https://cdnjs.cloudflare.com/ajax/libs/webcomponentsjs/1.3.3/webcomponents-lite.js"></script>
                <link rel="import" href="https://raw.githubusercontent.com/PAIR-code/facets/master/facets-dist/facets-jupyter.html">
                <facets-dive id="elem" height="600"></facets-dive>
                <script>
                  var data = {jsonstr};
                  document.querySelector("#elem").data = data;
                </script>"""
            html = HTML_TEMPLATE.format(jsonstr=jsonstr)
            display(HTML(html))

    def one(self, axis=0, as_type='pandas'):
        return self.select(self.columns).collect(1, as_type=as_type)

    def collect(self, n, axis=0, as_type='pandas'):
        return self.df.select(self.columns).limit(n).toPandas()

def _data(self):
    return Data(self)

DataFrame.data = property(_data)
