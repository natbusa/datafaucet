import json

template_grid = """
<div id="jsGrid"></div>

<script>
    var records = {records};

    $("#jsGrid").jsGrid({{
        width: "100%",
        height: "400px",

        heading: true,
        filtering: false,
        inserting: false,
        editing: false,
        selecting: false,
        sorting: true,
        paging: true,
        pageLoading: false,
        autoload: true,

        pageSize: 15,
        pageButtonCount: 3,

        controller: {{
            loadData: function(filter) {{
                //to do: filtering
                console.log(filter)
                db = records
                return db;true
            }}
        }},

        fields: {fields}
    }});
</script>
"""

def render(data):
    records = data.to_json(orient='records')
    type_conv = {
        'int64':'number',
        'float64': 'number',
        'bool': 'checkbox'}
    types = [type_conv.get(str(x), 'text') for x in data.dtypes]
    cols = data.columns
    fields = json.dumps([{'name':str(k), 'type':t} for (k,t) in zip(cols, types)])
    return template_grid.format(records=records, fields=fields)
