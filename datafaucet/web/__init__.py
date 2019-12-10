js = """
function require(x) {
    var script = document.createElement('script');
    script.src = x;
    script.crossorigin="anonymous";
    document.head.appendChild(script);
}
require("https://code.jquery.com/jquery-3.4.1.min.js")
require("https://cdnjs.cloudflare.com/ajax/libs/jsgrid/1.5.3/jsgrid.min.js")

"""

css = """
function css(x) {
    var link = document.createElement('link');
    link.href = x;
    link.rel="stylesheet";
    link.type="text/css"
    document.head.appendChild(link);
}
css("https://cdnjs.cloudflare.com/ajax/libs/jsgrid/1.5.3/jsgrid.min.css")
css("https://cdnjs.cloudflare.com/ajax/libs/jsgrid/1.5.3/jsgrid-theme.min.css")
"""

try:
    from IPython.display import Javascript, HTML
    # HTML(css)
    # Javascript(js)
    #print('datafaucet js/css loaded.')
except ImportError:
    pass
