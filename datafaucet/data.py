from pandas import DataFrame

# visualization
from datafaucet.web import grid
from IPython.core.display import display, HTML

class _Data:
    def __init__(self, df, scols=None, gcols=None):
        self.df = df
        self.gcols = gcols or []

        self.scols = scols or df.columns
        self.scols = list(set(self.scols) - set(self.gcols))

    @property
    def columns(self):
        return [x for x in self.df.columns if x in (self.scols + self.gcols)]


    def grid(self, limit=1000, axis=0, render='pandas'):
        # get the data
        data = self.collect(limit, axis)
        if render=='pandas':
            return data
        elif render=='js':
            return display(HTML(grid.render(data)))
        else:
            return data

    def facets(self, n=1000):
        try:
            from IPython.display import display
        except:
            display = None

        if display:
            jsonstr = self.df[self.columns].rows.sample(n).data.collect(n, axis=0).to_json(orient='records')
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

    def one(self, axis=0):
        return self.collect(1, axis)

    def collect(self, n=1000, axis=0):
        res = self.df[self.columns].head(n)
        return res.T if axis else res
