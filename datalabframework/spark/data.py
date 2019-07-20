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
    
    def grid(self, limit=1000, render='qgrid'):
        try:
            from IPython.display import display
        except:
            display = None

        try:
            import qgrid
        except:
            render = 'default'
            logging.warning('Install qgrid for better visualisation. Using pandas as fallback.')

        # get the data
        data = self.df.select(self.columns).limit(limit).toPandas()

        if render=='qgrid':
            rendered = qgrid.show_grid(data) 
        else:
            rendered = display(data) if display else data
        return rendered

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
